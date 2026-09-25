"""Sign-in through an OpenID Connect provider (Okta): discovery, PKCE,
the code exchange, and ID-token verification against the provider's keys.

The flow is the authorization code flow with PKCE, the one a web app
with a client secret should run:

    start     make a state, a nonce and a PKCE verifier; send the browser
              to the provider's authorization endpoint
    callback  exchange the code (with the verifier) for tokens, verify the
              ID token's signature, issuer, audience, expiry and nonce,
              then read the person out of its claims

Nothing here touches the identity store or the session cookie; the
router does that with what ``verify_id_token`` returns. The HTTP call is
injectable so the whole flow runs in a test against a signed token and
a fake JWKS, with no provider on the network.

Only RS256 is accepted, the algorithm Okta signs ID tokens with by
default. A token signed any other way is refused rather than guessed at.

Configuration (the environment; ``OKTA_*`` names, one provider):

    OKTA_ISSUER            https://<org>.okta.com/oauth2/<authorization-server-id>
    OKTA_CLIENT_ID
    OKTA_CLIENT_SECRET
    OKTA_REDIRECT_URI      the callback registered on the client
    OKTA_SCOPES            default "openid profile"; add the server's custom scope
    OKTA_GROUP_CLAIM       the ID-token claim carrying group names (default groups)
    AUTH_EMAIL_CLAIMS      where the email is read, in order (default email,preferred_username)
    AUTH_GROUP_ROLE_MAP    "<group>=admin,<group>=steward,*=analyst"; the default
                           for a person in no mapped group is AUTH_DEFAULT_ROLE
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DEFAULT_SCOPES = ("openid", "profile")
DEFAULT_EMAIL_CLAIMS = ("email", "preferred_username")
DISCOVERY_TTL_SECONDS = 3600.0
CLOCK_SKEW_SECONDS = 120


class OidcConfigurationError(RuntimeError):
    """The provider is not configured: a required setting is missing."""


class OidcError(RuntimeError):
    """The provider refused, or its answer did not verify."""


# ── helpers ──────────────────────────────────────────────────
def b64url_decode(text: str) -> bytes:
    text = text.strip()
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def pkce_pair() -> tuple[str, str]:
    """→ (verifier, S256 challenge)."""
    verifier = secrets.token_urlsafe(64)
    challenge = b64url_encode(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


def parse_group_role_map(text: str) -> dict[str, str]:
    """``"Sem-Admins=admin, Sem-Stewards=steward, *=analyst"`` → a dict.
    Group names keep their case for display but match case-insensitively."""
    out: dict[str, str] = {}
    for part in (text or "").split(","):
        if "=" not in part:
            continue
        group, role = part.split("=", 1)
        group, role = group.strip(), role.strip().lower()
        if group and role:
            out[group] = role
    return out


def roles_for_groups(groups: Iterable[str], mapping: Mapping[str, str],
                     default_role: str) -> list[str]:
    """The roles a person's groups earn. ``*`` in the map is the role for
    anyone who signed in; the default role applies when the map says
    nothing about them at all. Sorted, deduplicated."""
    lowered = {str(g).strip().lower() for g in groups or () if str(g).strip()}
    roles: set[str] = set()
    for group, role in mapping.items():
        if group == "*" or group.lower() in lowered:
            roles.add(role)
    if not roles and default_role:
        roles.add(default_role.lower())
    return sorted(roles)


def _value(env: Mapping[str, str], *names: str, default: str = "") -> str:
    for name in names:
        raw = env.get(name)
        if raw is not None and str(raw).strip():
            return str(raw).strip().strip("'\"")
    return default


# ── settings ─────────────────────────────────────────────────
@dataclass(frozen=True)
class OidcSettings:
    issuer: str
    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: tuple[str, ...] = DEFAULT_SCOPES
    group_claim: str = "groups"
    email_claims: tuple[str, ...] = DEFAULT_EMAIL_CLAIMS
    group_role_map: dict[str, str] = field(default_factory=dict)
    default_role: str = "analyst"
    provider: str = "okta"

    @property
    def configured(self) -> bool:
        return bool(self.issuer and self.client_id and self.redirect_uri)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "OidcSettings":
        if environ is None:
            try:
                from sahs.util.auth import load_dotenv
                load_dotenv()
            except Exception:  # noqa: BLE001
                pass
            import os
            environ = os.environ
        env = environ
        scopes = tuple(s for s in _value(env, "OKTA_SCOPES").split() if s) or DEFAULT_SCOPES
        if "openid" not in scopes:
            scopes = ("openid", *scopes)
        claims = tuple(c.strip() for c in _value(env, "AUTH_EMAIL_CLAIMS").split(",")
                       if c.strip()) or DEFAULT_EMAIL_CLAIMS
        return cls(
            issuer=_value(env, "OKTA_ISSUER").rstrip("/"),
            client_id=_value(env, "OKTA_CLIENT_ID"),
            client_secret=_value(env, "OKTA_CLIENT_SECRET"),
            redirect_uri=_value(env, "OKTA_REDIRECT_URI"),
            scopes=scopes,
            group_claim=_value(env, "OKTA_GROUP_CLAIM", default="groups"),
            email_claims=claims,
            group_role_map=parse_group_role_map(_value(env, "AUTH_GROUP_ROLE_MAP")),
            default_role=_value(env, "AUTH_DEFAULT_ROLE", default="analyst").lower(),
        )

    def require(self) -> "OidcSettings":
        missing = [name for name, value in (("OKTA_ISSUER", self.issuer),
                                            ("OKTA_CLIENT_ID", self.client_id),
                                            ("OKTA_REDIRECT_URI", self.redirect_uri))
                   if not value]
        if missing:
            raise OidcConfigurationError("Okta sign-in is not configured: set "
                                         + ", ".join(missing))
        return self


# ── the client ───────────────────────────────────────────────
HttpCall = Callable[[str, bytes | None, dict[str, str]], tuple[int, bytes]]


def default_http(url: str, data: bytes | None, headers: dict[str, str]) -> tuple[int, bytes]:
    """(status, body) for one request; HTTP errors come back as a status,
    transport errors raise OidcError."""
    request = Request(url, data=data, headers=headers, method="POST" if data is not None else "GET")
    try:
        with urlopen(request, timeout=15) as response:  # noqa: S310 - the issuer is configuration
            return response.status, response.read()
    except Exception as exc:  # noqa: BLE001
        status = getattr(exc, "code", None)
        if isinstance(status, int):
            try:
                body = exc.read()  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                body = b""
            return status, body
        raise OidcError(f"the identity provider could not be reached: {exc}") from exc


class OidcClient:
    """Discovery, the authorization URL, the code exchange, and token verification."""

    def __init__(self, settings: OidcSettings, *, http: HttpCall | None = None,
                 clock: Callable[[], float] = time.time) -> None:
        self.settings = settings.require()
        self._http = http or default_http
        self._clock = clock
        self._lock = threading.Lock()
        self._discovery: dict[str, Any] | None = None
        self._discovered_at = 0.0
        self._jwks: dict[str, dict[str, Any]] = {}

    # ── discovery and keys ─────────────────────────────────
    def _get_json(self, url: str) -> dict[str, Any]:
        status, body = self._http(url, None, {"Accept": "application/json"})
        if status != 200:
            raise OidcError(f"{url} answered HTTP {status}")
        try:
            payload = json.loads(body)
        except ValueError as exc:
            raise OidcError(f"{url} did not answer JSON") from exc
        if not isinstance(payload, dict):
            raise OidcError(f"{url} did not answer an object")
        return payload

    def discovery(self, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            fresh = self._discovery is not None and (
                self._clock() - self._discovered_at < DISCOVERY_TTL_SECONDS)
            if fresh and not force:
                return self._discovery  # type: ignore[return-value]
        doc = self._get_json(self.settings.issuer + "/.well-known/openid-configuration")
        issuer = str(doc.get("issuer", "")).rstrip("/")
        if issuer != self.settings.issuer:
            raise OidcError(f"discovery names issuer {issuer!r}, configured {self.settings.issuer!r}")
        for key in ("authorization_endpoint", "token_endpoint", "jwks_uri"):
            if not doc.get(key):
                raise OidcError(f"discovery lacks {key}")
        with self._lock:
            self._discovery, self._discovered_at = doc, self._clock()
        return doc

    def _load_jwks(self) -> None:
        doc = self.discovery()
        keys = self._get_json(str(doc["jwks_uri"])).get("keys") or []
        with self._lock:
            self._jwks = {str(k.get("kid")): k for k in keys if isinstance(k, dict) and k.get("kid")}

    def key_for(self, kid: str) -> dict[str, Any]:
        with self._lock:
            key = self._jwks.get(kid)
        if key is None:
            self._load_jwks()          # a rotated key: refresh once, then refuse
            with self._lock:
                key = self._jwks.get(kid)
        if key is None:
            raise OidcError(f"the ID token names an unknown signing key {kid!r}")
        return key

    # ── the flow ───────────────────────────────────────────
    def authorization_url(self, *, state: str, nonce: str, code_challenge: str,
                          extra: Mapping[str, str] | None = None) -> str:
        params = {
            "client_id": self.settings.client_id,
            "redirect_uri": self.settings.redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.settings.scopes),
            "state": state,
            "nonce": nonce,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
            **(extra or {}),
        }
        return f"{self.discovery()['authorization_endpoint']}?{urlencode(params)}"

    def exchange_code(self, code: str, code_verifier: str) -> dict[str, Any]:
        """→ the token response; client_secret_basic first, the body second."""
        endpoint = str(self.discovery()["token_endpoint"])
        form = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.settings.redirect_uri,
            "code_verifier": code_verifier,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded",
                   "Accept": "application/json"}
        if self.settings.client_secret:
            basic = base64.b64encode(
                f"{self.settings.client_id}:{self.settings.client_secret}".encode()).decode()
            status, body = self._http(endpoint, urlencode(form).encode(),
                                      {**headers, "Authorization": f"Basic {basic}"})
            payload = self._token_payload(status, body, tolerate_client_error=True)
            if payload is not None:
                return payload
            form["client_secret"] = self.settings.client_secret
        form["client_id"] = self.settings.client_id
        status, body = self._http(endpoint, urlencode(form).encode(), headers)
        payload = self._token_payload(status, body, tolerate_client_error=False)
        assert payload is not None
        return payload

    @staticmethod
    def _token_payload(status: int, body: bytes, *,
                       tolerate_client_error: bool) -> dict[str, Any] | None:
        try:
            payload = json.loads(body or b"{}")
        except ValueError:
            payload = {}
        if status == 200 and isinstance(payload, dict) and payload.get("id_token"):
            return payload
        error = payload.get("error", "") if isinstance(payload, dict) else ""
        if tolerate_client_error and status in (400, 401) and error == "invalid_client":
            return None
        description = payload.get("error_description", "") if isinstance(payload, dict) else ""
        raise OidcError(f"the code exchange failed: HTTP {status} {error} {description}".strip())

    def verify_id_token(self, token: str, *, nonce: str) -> dict[str, Any]:
        """The claims of a token that verifies: signature (RS256 against
        the provider's keys), issuer, audience, expiry, issue time, nonce."""
        try:
            header_b64, payload_b64, signature_b64 = token.split(".")
            header = json.loads(b64url_decode(header_b64))
            claims = json.loads(b64url_decode(payload_b64))
            signature = b64url_decode(signature_b64)
        except (ValueError, TypeError) as exc:
            raise OidcError("the ID token is malformed") from exc
        if header.get("alg") != "RS256":
            raise OidcError(f"the ID token is signed with {header.get('alg')!r}; only RS256 is accepted")
        key = self.key_for(str(header.get("kid", "")))
        if key.get("kty") != "RSA" or not key.get("n") or not key.get("e"):
            raise OidcError("the signing key is not an RSA key")
        self._verify_rs256(key, f"{header_b64}.{payload_b64}".encode("ascii"), signature)

        now = self._clock()
        if str(claims.get("iss", "")).rstrip("/") != self.settings.issuer:
            raise OidcError("the ID token names another issuer")
        aud = claims.get("aud")
        audiences = aud if isinstance(aud, list) else [aud]
        if self.settings.client_id not in audiences:
            raise OidcError("the ID token was issued to another client")
        if not isinstance(claims.get("exp"), (int, float)) or claims["exp"] <= now - CLOCK_SKEW_SECONDS:
            raise OidcError("the ID token has expired")
        if isinstance(claims.get("iat"), (int, float)) and claims["iat"] > now + CLOCK_SKEW_SECONDS:
            raise OidcError("the ID token is from the future")
        if not nonce or claims.get("nonce") != nonce:
            raise OidcError("the ID token's nonce does not match this sign-in")
        if not claims.get("sub"):
            raise OidcError("the ID token has no subject")
        return claims

    @staticmethod
    def _verify_rs256(key: dict[str, Any], signing_input: bytes, signature: bytes) -> None:
        from cryptography.exceptions import InvalidSignature
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding, rsa
        n = int.from_bytes(b64url_decode(str(key["n"])), "big")
        e = int.from_bytes(b64url_decode(str(key["e"])), "big")
        public_key = rsa.RSAPublicNumbers(e, n).public_key()
        try:
            public_key.verify(signature, signing_input, padding.PKCS1v15(), hashes.SHA256())
        except InvalidSignature as exc:
            raise OidcError("the ID token's signature does not verify") from exc

    # ── reading the person ─────────────────────────────────
    def person_from(self, claims: Mapping[str, Any]) -> dict[str, Any]:
        """email, name, first and last name, groups and the roles they earn."""
        email = ""
        email_claim = ""
        for claim in self.settings.email_claims:
            value = str(claims.get(claim) or "").strip()
            if "@" in value:
                email, email_claim = value, claim
                break
        first = str(claims.get("given_name") or "").strip()
        last = str(claims.get("family_name") or "").strip()
        name = str(claims.get("name") or f"{first} {last}".strip()
                   or email.split("@", 1)[0]).strip()
        raw_groups = claims.get(self.settings.group_claim) or []
        groups = [str(g) for g in raw_groups] if isinstance(raw_groups, list) else [str(raw_groups)]
        return {
            "subject": str(claims.get("sub")),
            "email": email,
            "email_claim": email_claim,
            "name": name,
            "first_name": first,
            "last_name": last,
            "groups": groups,
            "roles": roles_for_groups(groups, self.settings.group_role_map,
                                      self.settings.default_role),
        }


__all__ = ["CLOCK_SKEW_SECONDS", "OidcClient", "OidcConfigurationError", "OidcError",
           "OidcSettings", "b64url_decode", "b64url_encode", "default_http",
           "parse_group_role_map", "pkce_pair", "roles_for_groups"]
