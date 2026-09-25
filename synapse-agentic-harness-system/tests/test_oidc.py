"""The OpenID Connect client against a fake provider: discovery, keys,
the code exchange with its client-auth fallback, and every reason an ID
token is refused."""

from __future__ import annotations

import base64
import json
import time
from urllib.parse import parse_qs, urlsplit

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from sahs.identity.oidc import (OidcClient, OidcError, OidcSettings, b64url_encode,
                                parse_group_role_map, pkce_pair, roles_for_groups)

ISSUER = "https://org.example/oauth2/as1"
CLIENT = "0oa-client"


class FakeOkta:
    """Serves discovery, a JWKS, and a token endpoint; signs ID tokens."""

    def __init__(self) -> None:
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.kid = "k1"
        self.calls: list[tuple[str, dict[str, str], dict[str, str] | None]] = []
        self.token_auth = "basic"        # or "post": which client auth the fake accepts

    def jwk(self) -> dict:
        numbers = self.key.public_key().public_numbers()
        n = numbers.n.to_bytes((numbers.n.bit_length() + 7) // 8, "big")
        e = numbers.e.to_bytes((numbers.e.bit_length() + 7) // 8, "big")
        return {"kty": "RSA", "kid": self.kid, "alg": "RS256", "use": "sig",
                "n": b64url_encode(n), "e": b64url_encode(e)}

    def id_token(self, *, nonce: str, **overrides) -> str:
        now = int(time.time())
        claims = {"iss": ISSUER, "aud": CLIENT, "sub": "00u1", "exp": now + 300, "iat": now,
                  "nonce": nonce, "email": "ana@example.com", "name": "Ana Lyst",
                  "given_name": "Ana", "family_name": "Lyst", "groups": ["Sem-Stewards"]}
        claims.update(overrides)
        header = {"alg": overrides.get("_alg", "RS256"), "kid": overrides.get("_kid", self.kid)}
        claims = {k: v for k, v in claims.items() if not k.startswith("_")}
        head = b64url_encode(json.dumps(header).encode())
        body = b64url_encode(json.dumps(claims).encode())
        signature = self.key.sign(f"{head}.{body}".encode(), padding.PKCS1v15(), hashes.SHA256())
        if overrides.get("_break_signature"):
            signature = bytes([signature[0] ^ 1]) + signature[1:]
        return f"{head}.{body}.{b64url_encode(signature)}"

    def http(self, url: str, data: bytes | None, headers: dict[str, str]) -> tuple[int, bytes]:
        form = {k: v[0] for k, v in parse_qs((data or b"").decode()).items()}
        self.calls.append((url, headers, form or None))
        if url == ISSUER + "/.well-known/openid-configuration":
            return 200, json.dumps({"issuer": ISSUER,
                                    "authorization_endpoint": ISSUER + "/v1/authorize",
                                    "token_endpoint": ISSUER + "/v1/token",
                                    "jwks_uri": ISSUER + "/v1/keys"}).encode()
        if url == ISSUER + "/v1/keys":
            return 200, json.dumps({"keys": [self.jwk()]}).encode()
        if url == ISSUER + "/v1/token":
            if self.token_auth == "basic" and not headers.get("Authorization", "").startswith("Basic "):
                return 401, json.dumps({"error": "invalid_client"}).encode()
            if self.token_auth == "post" and headers.get("Authorization"):
                return 401, json.dumps({"error": "invalid_client"}).encode()
            if form.get("code") != "good-code":
                return 400, json.dumps({"error": "invalid_grant",
                                        "error_description": "no such code"}).encode()
            return 200, json.dumps({"id_token": self.id_token(nonce=form.get("_nonce", "n1")),
                                    "access_token": "at", "token_type": "Bearer"}).encode()
        return 404, b"{}"


def settings(**over) -> OidcSettings:
    base = dict(issuer=ISSUER, client_id=CLIENT, client_secret="sec", redirect_uri="https://app/callback",
                group_role_map=parse_group_role_map("Sem-Admins=admin, Sem-Stewards=steward, *=analyst"))
    return OidcSettings(**{**base, **over})


def test_authorization_url_carries_pkce_state_and_nonce():
    okta = FakeOkta()
    client = OidcClient(settings(), http=okta.http)
    verifier, challenge = pkce_pair()
    url = client.authorization_url(state="okta.s1", nonce="n1", code_challenge=challenge)
    q = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
    assert urlsplit(url).path == "/oauth2/as1/v1/authorize"
    assert q["client_id"] == CLIENT and q["redirect_uri"] == "https://app/callback"
    assert q["response_type"] == "code" and q["scope"] == "openid profile"
    assert q["state"] == "okta.s1" and q["nonce"] == "n1"
    assert q["code_challenge"] == challenge and q["code_challenge_method"] == "S256"
    assert len(verifier) >= 43


def test_discovery_refuses_another_issuer():
    okta = FakeOkta()
    client = OidcClient(settings(issuer="https://other.example/oauth2/x"), http=okta.http)
    with pytest.raises(OidcError):
        client.discovery()


def test_exchange_uses_basic_auth_then_falls_back_to_the_body():
    okta = FakeOkta()
    client = OidcClient(settings(), http=okta.http)
    payload = client.exchange_code("good-code", "verifier")
    assert payload["id_token"]
    token_calls = [c for c in okta.calls if c[0].endswith("/v1/token")]
    assert len(token_calls) == 1 and token_calls[0][1]["Authorization"].startswith("Basic ")
    okta.token_auth = "post"
    okta.calls.clear()
    payload = client.exchange_code("good-code", "verifier")
    token_calls = [c for c in okta.calls if c[0].endswith("/v1/token")]
    assert len(token_calls) == 2 and token_calls[1][2]["client_secret"] == "sec"
    with pytest.raises(OidcError, match="invalid_grant"):
        client.exchange_code("bad-code", "verifier")


def test_id_token_verification_and_every_refusal():
    okta = FakeOkta()
    client = OidcClient(settings(), http=okta.http)
    claims = client.verify_id_token(okta.id_token(nonce="n1"), nonce="n1")
    assert claims["sub"] == "00u1" and claims["email"] == "ana@example.com"
    for bad, reason in (
        (okta.id_token(nonce="n1", _break_signature=True), "signature"),
        (okta.id_token(nonce="other"), "nonce"),
        (okta.id_token(nonce="n1", aud="someone-else"), "another client"),
        (okta.id_token(nonce="n1", iss="https://evil.example"), "another issuer"),
        (okta.id_token(nonce="n1", exp=int(time.time()) - 1000), "expired"),
        (okta.id_token(nonce="n1", _alg="none"), "RS256"),
        (okta.id_token(nonce="n1", _kid="unknown"), "unknown signing key"),
        ("not.a.jwt.at.all", "malformed"),
    ):
        with pytest.raises(OidcError, match=reason):
            client.verify_id_token(bad, nonce="n1")


def test_key_rotation_refreshes_the_jwks_once():
    okta = FakeOkta()
    client = OidcClient(settings(), http=okta.http)
    client.verify_id_token(okta.id_token(nonce="n1"), nonce="n1")
    okta.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    okta.kid = "k2"
    client.verify_id_token(okta.id_token(nonce="n1"), nonce="n1")     # refreshed keys
    assert sum(1 for c in okta.calls if c[0].endswith("/v1/keys")) == 2


def test_person_from_claims_maps_groups_to_roles():
    client = OidcClient(settings(), http=FakeOkta().http)
    person = client.person_from({"sub": "00u1", "email": "Ana@Example.com", "given_name": "Ana",
                                 "family_name": "Lyst", "groups": ["sem-stewards", "Other"]})
    assert person["email"] == "Ana@Example.com" and person["email_claim"] == "email"
    assert person["name"] == "Ana Lyst" and person["roles"] == ["analyst", "steward"]
    no_email = client.person_from({"sub": "1", "preferred_username": "ana@example.com"})
    assert no_email["email"] == "ana@example.com" and no_email["email_claim"] == "preferred_username"
    assert client.person_from({"sub": "1", "preferred_username": "not-an-email"})["email"] == ""


def test_group_role_map_and_defaults():
    mapping = parse_group_role_map(" A=admin ,B=steward, *=analyst,bad,")
    assert mapping == {"A": "admin", "B": "steward", "*": "analyst"}
    assert roles_for_groups(["a"], mapping, "analyst") == ["admin", "analyst"]
    assert roles_for_groups([], {"A": "admin"}, "analyst") == ["analyst"]
    assert roles_for_groups(["A"], {"A": "admin"}, "analyst") == ["admin"]
    assert roles_for_groups([], {}, "") == []


def test_settings_from_env_and_require():
    s = OidcSettings.from_env({"OKTA_ISSUER": ISSUER + "/", "OKTA_CLIENT_ID": CLIENT,
                               "OKTA_REDIRECT_URI": "https://app/callback",
                               "OKTA_SCOPES": "profile custom_scope",
                               "AUTH_GROUP_ROLE_MAP": "G=admin", "AUTH_EMAIL_CLAIMS": "upn, email"})
    assert s.issuer == ISSUER and s.scopes == ("openid", "profile", "custom_scope")
    assert s.email_claims == ("upn", "email") and s.group_role_map == {"G": "admin"}
    assert s.configured and s.require() is s
    with pytest.raises(Exception, match="OKTA_ISSUER"):
        OidcSettings.from_env({}).require()
    pem = serialization.Encoding.PEM  # the cryptography import is exercised above; keep ruff honest
    assert pem is not None
