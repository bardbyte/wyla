"""Test doubles: a scripted model, a fake catalog, a fake gateway, and a
Vertex stream. Nothing here touches a network; every double records what
it was asked so a test can pin the exact request."""

from __future__ import annotations

import base64
import copy
import json
from typing import Any, Callable, Iterator

from kcx.transport import TransportError


def jwt(claims: dict[str, Any]) -> str:
    """An unsigned JWT whose payload carries the given claims."""
    def seg(obj: Any) -> str:
        return base64.urlsafe_b64encode(
            json.dumps(obj).encode()).decode().rstrip("=")
    return f"{seg({'alg': 'HS256'})}.{seg(claims)}.sig"


def google_error(status: int, reason: str, message: str) -> bytes:
    """A refusal in Google's JSON shape, with the ErrorInfo detail when a
    reason is given."""
    words = {400: "INVALID_ARGUMENT", 401: "UNAUTHENTICATED",
             403: "PERMISSION_DENIED", 404: "NOT_FOUND",
             429: "RESOURCE_EXHAUSTED", 500: "INTERNAL", 503: "UNAVAILABLE"}
    err: dict[str, Any] = {"code": status, "message": message,
                           "status": words.get(status, "UNKNOWN")}
    if reason:
        err["details"] = [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
                           "reason": reason, "domain": "googleapis.com"}]
    return json.dumps({"error": err}).encode()


def entry(name: str, *, system: str = "BIGQUERY", display: str | None = None,
          description: str = "", entry_type: str = "bigquery-table"
          ) -> dict[str, Any]:
    """One raw search result in the API's shape. The display name is the
    name unless given; an empty string leaves it out, as the API does for
    some entries."""
    source: dict[str, Any] = {
        "resource": f"//bigquery.googleapis.com/projects/demo/datasets/dw/"
                    f"tables/{name}",
        "system": system}
    if display is None:
        display = name
    if display:
        source["displayName"] = display
    if description:
        source["description"] = description
    return {"dataplexEntry": {
        "name": f"projects/demo/locations/us/entryGroups/@bigquery/entries/"
                f"{name}",
        "entryType": f"projects/1/locations/global/entryTypes/{entry_type}",
        "fullyQualifiedName": f"bigquery:demo.dw.{name}",
        "entrySource": source}}


class ScriptedModel:
    """Scripted PARTS per model call: each step is a list of ``{"text"}``,
    ``{"thought"}``, ``{"call": {"name", "args"}}`` items (plus an
    optional ``{"finish": …}``), or a callable of the contents. Emits the
    same events the real planes do. Call parts get ids (``call_N``)
    unless ``ids=False``, and a thought signature on the FIRST call of a
    step unless ``signed=False`` (the 2.5 shape). ``fail_first`` raises a
    transport failure before anything is yielded on that many calls."""

    def __init__(self, steps: list[Any] = (), *, ids: bool = True,
                 signed: bool = True, fail_first: int = 0,
                 usage: dict[str, int] | None = None) -> None:
        self.steps = list(steps)
        self.calls: list[dict[str, Any]] = []
        self.ids = ids
        self.signed = signed
        self.fail_first = fail_first
        self.usage_each = usage or {"prompt_tokens": 100, "output_tokens": 20,
                                    "thought_tokens": 5, "cached_tokens": 0}
        self.usage = {"calls": 0, "prompt_tokens": 0, "output_tokens": 0,
                      "thought_tokens": 0}
        self.plane = "scripted"
        self.model = "scripted-model"
        self._n = 0

    def describe(self) -> str:
        return "scripted model (no network)"

    def converse(self, contents: list[dict[str, Any]], *, system: str = "",
                 tools: list[dict[str, Any]] | None = None,
                 thinking_level: str = "", include_thoughts: bool = True,
                 max_output_tokens: int = 8192, timeout: float | None = None
                 ) -> Iterator[dict[str, Any]]:
        self.calls.append({"contents": copy.deepcopy(contents),
                           "system": system,
                           "tools": [t["name"] for t in (tools or [])],
                           "thinking_level": thinking_level,
                           "max_output_tokens": max_output_tokens})
        self.usage["calls"] += 1
        if self.fail_first > 0:
            self.fail_first -= 1
            raise TransportError("scripted transport failure")
        step = self.steps.pop(0) if self.steps else []
        if callable(step):
            step = step(contents)
        parts: list[dict[str, Any]] = []
        finish = "STOP"
        first_call = True
        for item in step or []:
            if "finish" in item:
                finish = item["finish"]
            elif "text" in item:
                yield {"kind": "text", "delta": item["text"]}
                parts.append({"text": item["text"]})
            elif "thought" in item:
                yield {"kind": "thought", "delta": item["thought"]}
                parts.append({"thought": True, "text": item["thought"]})
            elif "call" in item:
                self._n += 1
                call: dict[str, Any] = {"name": item["call"]["name"],
                                        "args": item["call"].get("args") or {}}
                if self.ids:
                    call["id"] = f"call_{self._n}"
                part: dict[str, Any] = {"functionCall": call}
                if self.signed and first_call:
                    part["thoughtSignature"] = "scripted-signature"
                first_call = False
                parts.append(part)
                yield {"kind": "call", "name": call["name"],
                       "args": call["args"], "id": call.get("id", "")}
        yield {"kind": "done", "parts": parts, "finish": finish,
               "usage": dict(self.usage_each)}


class FakeCatalog:
    """searchEntries as the API answers it: rows chosen by a substring of
    the query (a callable of the body for anything fancier), statuses or
    exceptions queued ahead of the scripted 200, every request kept."""

    def __init__(self, answers: dict[str, Any] | None = None, *,
                 default: Any = ()) -> None:
        self.answers = dict(answers or {})
        # rows, or a whole payload in the API's shape
        self.default = default if isinstance(default, dict) else list(default)
        self.fail_next: list[Any] = []
        self.requests: list[dict[str, Any]] = []
        self.tokens = 0

    def token(self) -> str:
        self.tokens += 1
        return "catalog-token"

    def http(self, method: str, url: str, headers: dict[str, str],
             body: bytes | None, *, timeout: float | None = None
             ) -> tuple[int, dict[str, str], bytes]:
        payload = json.loads(body) if body else {}
        self.requests.append({"method": method, "url": url,
                              "headers": dict(headers), "body": payload,
                              "timeout": timeout})
        if self.fail_next:
            item = self.fail_next.pop(0)
            if isinstance(item, BaseException):
                raise item
            if isinstance(item, tuple):
                return item[0], {}, item[1]
            return item, {}, google_error(item, "", f"scripted {item}")
        query = str(payload.get("query", ""))
        rows: Any = None
        for key, value in self.answers.items():
            if key in query:
                rows = value(payload) if callable(value) else value
                break
        if rows is None:
            rows = self.default
        if isinstance(rows, dict):
            answer = rows                      # a whole payload, as given
        else:
            answer = {"results": list(rows), "totalSize": len(rows),
                      "nextPageToken": "", "unreachable": []}
        return 200, {"content-type": "application/json"}, \
            json.dumps(answer).encode()


def sse(chunks: list[Any]) -> Callable[[dict[str, Any]], Iterator[dict[str, Any]]]:
    """A Vertex stream double: yields the chunk dicts in order and raises
    where the script places an exception. The bodies sent are kept on
    the function."""
    bodies: list[dict[str, Any]] = []

    def stream(body: dict[str, Any]) -> Iterator[dict[str, Any]]:
        bodies.append(body)
        for chunk in chunks:
            if isinstance(chunk, BaseException):
                raise chunk
            yield chunk
    stream.bodies = bodies                      # type: ignore[attr-defined]
    return stream


class FakeGateway:
    """The identity service and the gateway together: a token that lives
    599 s, answers scripted per model call (a parts list, or a callable
    of the request body), statuses queued ahead, every request kept."""

    def __init__(self, answers: list[Any] = ()) -> None:
        self.answers = list(answers)
        self.now = 1_700_000_000.0
        self.requests: list[dict[str, Any]] = []
        self.minted = 0
        self.dead: set[str] = set()
        self.fail_next: list[Any] = []

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def http(self, method: str, url: str, headers: dict[str, str],
             body: bytes | None, *, timeout: float | None = None
             ) -> tuple[int, dict[str, str], bytes]:
        if url.endswith("/token"):
            self.minted += 1
            token = jwt({"exp": int(self.now) + 599, "n": self.minted})
            self.requests.append({"kind": "token", "url": url,
                                  "headers": dict(headers),
                                  "body": json.loads(body or b"{}")})
            return 200, {}, json.dumps({"authorization_token": token}).encode()
        token = headers["Authorization"].split(" ", 1)[1]
        payload = json.loads(body or b"{}")
        self.requests.append({"kind": "model", "url": url, "token": token,
                              "body": payload, "timeout": timeout})
        claims = json.loads(base64.urlsafe_b64decode(
            token.split(".")[1] + "=="))
        if token in self.dead or claims["exp"] <= self.now:
            return 401, {"WWW-Authenticate": "Bearer"}, b""
        if self.fail_next:
            item = self.fail_next.pop(0)
            if isinstance(item, BaseException):
                raise item
            if isinstance(item, tuple):
                return item[0], {}, item[1]
            return item, {}, b'{"error":{"message":"boom"}}'
        answer = self.answers.pop(0) if self.answers else {
            "parts": [{"text": "nothing scripted"}]}
        if callable(answer):
            answer = answer(payload)
        return 200, {}, json.dumps({
            "candidates": [{"content": {"parts": answer["parts"]},
                            "finishReason": answer.get("finish", "STOP")}],
            "usageMetadata": answer.get("usage", {
                "promptTokenCount": 100, "candidatesTokenCount": 20,
                "thoughtsTokenCount": 30})}).encode()

    @property
    def model_requests(self) -> list[dict[str, Any]]:
        return [r for r in self.requests if r["kind"] == "model"]
