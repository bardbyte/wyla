"""The sign-in on both surfaces, as the browser sees it: the shell draws
the account row from /api/auth/me instead of shipping a name; every api
call goes through the session module (the CSRF header, the sign-in page
on a 401); the sign-in, account and people pages exist and are served;
and the admin routes behind the People page work end to end on the
sqlite store, with the CSRF header the page sends."""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_signin_flow import _sign_in, client, fake  # noqa: E402,F401

REPO = Path(__file__).resolve().parents[3]
# the example SYNAPSE_USER_NAME from .env.example: the shell must not ship it
EXAMPLE_NAME = "John Doe"
ADMIN = REPO / "apps" / "synapse_admin" / "frontend"
SYNAPSE = REPO / "apps" / "synapse" / "frontend"


def read(root: Path, rel: str) -> str:
    return (root / rel).read_text(encoding="utf-8")


def test_both_shells_draw_the_account_row_and_gate_the_nav():
    for root in (ADMIN, SYNAPSE):
        index = read(root, "index.html")
        assert 'id="account-name"' in index and 'id="account-role"' in index
        assert 'id="account-avatar"' in index and 'href="#/account"' in index
        assert EXAMPLE_NAME not in index                        # no shipped identity
        assert index.index('class="account"') > index.index('class="navlist"')
    admin = read(ADMIN, "index.html")
    assert 'href="#/users" data-tab="users" data-needs="users.manage" hidden' in admin
    assert "data-needs" not in read(SYNAPSE, "index.html")    # nothing to gate there yet


def test_every_api_call_goes_through_the_session_module():
    for root in (ADMIN, SYNAPSE):
        api = read(root, "js/api.js")
        assert 'import { apiFetch } from "./session.js";' in api
        assert not re.search(r"(?<![A-Za-z])fetch\(", api), root  # no bare fetch left
        session = read(root, "js/session.js")
        assert 'const CSRF_COOKIE = "synapse_csrf";' in session
        assert 'const CSRF_HEADER = "X-CSRF-Token";' in session
        assert "r.status === 401" in session and "toSignin()" in session
        assert 'path.startsWith("/api/auth/")' in session       # a wrong password stays on the page
        assert 'fetch("/api/auth/me"' in session
        assert 'credentials: "same-origin"' in session
    assert 'export const HOME = "#/home";' in read(ADMIN, "js/session.js")
    assert 'export const HOME = "#/chat";' in read(SYNAPSE, "js/session.js")


def test_the_routers_boot_as_the_signed_in_person():
    for root, extra in ((ADMIN, ("users",)), (SYNAPSE, ())):
        main = read(root, "js/main.js")
        assert 'import { renderSignin } from "./pages/signin.js";' in main
        assert 'import { renderAccount } from "./pages/account.js";' in main
        assert "signin: () => renderSignin(outlet, query)" in main
        assert "account: () => renderAccount(outlet)" in main
        assert "async function boot()" in main and "await whoami()" in main
        assert "signedOut = !who.user && who.status === 401" in main
        assert 'document.body.classList.toggle("signed-out", signedOut)' in main
        assert "drawAccount(who.user)" in main and "gateNav(who.user)" in main
        assert "const [path, query = \"\"] = raw.split(\"?\");" in main
        for page in extra:
            assert f"{page}: () => render{page.title()}(outlet)" in main
    assert 'location.replace("/synapse/")' in read(ADMIN, "js/main.js")   # analysts have Synapse


def test_the_sign_in_page_offers_okta_first_and_the_local_form_only_when_open():
    for root in (ADMIN, SYNAPSE):
        page = read(root, "js/pages/signin.js")
        assert 'apiFetch("/api/auth/okta")' in page
        assert "Continue with Okta" in page and 'id="okta-start"' in page
        assert "status.local_login ? LOCAL_FORM" in page
        assert '"/api/auth/signup" : "/api/auth/login"' in page
        assert "encodeURIComponent(location.pathname + next)" in page   # next is a path on this site
        assert "safeHash(params.get(\"next\"))" in page
        assert "Sign-in is not configured on this server" in page


def test_the_account_page_connects_google_in_a_popup_and_signs_out():
    for root in (ADMIN, SYNAPSE):
        page = read(root, "js/pages/account.js")
        assert 'apiFetch("/api/auth/google/connection")' in page
        assert '"/api/auth/google/start?popup=1"' in page
        assert 'e.data?.type !== "google-connected"' in page
        assert 'method: "DELETE"' in page
        assert "signOut({ everywhere: true })" in page
        assert "requires_user_oauth" in page
    css = read(ADMIN, "styles/app.css")
    for cls in (".signed-out .sidenav", ".signin-card", ".signin-error", ".users-page"):
        assert cls in css
    assert ".signin-card" in read(SYNAPSE, "styles/app.css")


def test_the_new_modules_are_served_on_both_surfaces(client):
    for path in ("/js/session.js", "/js/pages/signin.js", "/js/pages/account.js",
                 "/js/pages/users.js", "/synapse/js/session.js",
                 "/synapse/js/pages/signin.js", "/synapse/js/pages/account.js"):
        response = client.get(path)
        assert response.status_code == 200, path
        assert "javascript" in response.headers["content-type"], path


def test_the_people_page_routes_work_for_an_admin_with_the_csrf_header(client, fake):
    fake.groups = ["Sem-Admins"]
    _sign_in(client, fake)
    csrf = {"x-csrf-token": client.cookies.get("synapse_csrf")}

    listed = client.get("/api/admin/users").json()
    assert listed["available"] and [u["email"] for u in listed["users"]] == ["ana@example.com"]
    me = listed["users"][0]
    assert me["roles"] == ["admin", "analyst"] and me["last_login_at"]

    # a second person, an analyst, to manage
    fake.groups, fake.email, fake.subject = ["Nobody-Special"], "bo@example.com", "00u2"
    bo_client = client.__class__(client.app)
    _sign_in(bo_client, fake)
    bo = next(u for u in client.get("/api/admin/users").json()["users"] if u["email"] == "bo@example.com")
    assert bo["roles"] == ["analyst"]

    refused = client.post(f"/api/admin/users/{bo['user_id']}/roles", json={"role": "steward"})
    assert refused.status_code == 403 and "CSRF" in refused.text
    granted = client.post(f"/api/admin/users/{bo['user_id']}/roles", json={"role": "steward"}, headers=csrf)
    assert granted.status_code == 200, granted.text
    assert granted.json()["user"]["roles"] == ["analyst", "steward"]
    revoked = client.delete(f"/api/admin/users/{bo['user_id']}/roles/analyst", headers=csrf)
    assert revoked.status_code == 200 and revoked.json()["user"]["roles"] == ["steward"]

    disabled = client.patch(f"/api/admin/users/{bo['user_id']}", json={"status": "disabled"}, headers=csrf)
    assert disabled.status_code == 200 and disabled.json()["user"]["status"] == "disabled"
    assert bo_client.get("/api/auth/me").status_code == 401         # their session ended
    own = client.patch(f"/api/admin/users/{me['user_id']}", json={"status": "disabled"}, headers=csrf)
    assert own.status_code == 400

    access = client.get("/api/admin/access").json()
    assert access["available"]
    ana = next(u for u in access["users"] if u["email"] == "ana@example.com")
    assert ana["successful_login_count"] == 1 and ana["contexts"][0]["login_count"] == 1
    assert ana["last_login_at"]

    gone = client.delete(f"/api/admin/users/{bo['user_id']}", headers=csrf)
    assert gone.status_code == 200
    assert [u["email"] for u in client.get("/api/admin/users").json()["users"]] == ["ana@example.com"]


def test_the_people_page_routes_refuse_an_analyst(client, fake):
    fake.groups = ["Nobody-Special"]
    _sign_in(client, fake)
    assert client.get("/api/admin/users").status_code == 403
    assert client.get("/api/admin/access").status_code == 403
