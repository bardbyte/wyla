"""The identity store on the sqlite stand-in: the same SQL the Spanner
path runs, against a file, so every verb is exercised without a Spanner."""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

from sahs.identity.store import IdentityStore, hash_token, normalize_email
from sahs.spanner import AuthSettings, GoogleOAuthSettings, SpannerSettings


def store(**auth) -> IdentityStore:
    settings = SpannerSettings(project_id="l", instance_id="l", database_id="l",
                               sqlite_path=":memory:", mode="sqlite")
    defaults = dict(pepper="pepper", bootstrap_admin_email="root@example.com",
                    lock_after=2, lock_minutes=1)
    return IdentityStore(settings, AuthSettings(**{**defaults, **auth}))


def test_signup_bootstraps_the_admin_and_issues_a_session():
    st = store()
    user, token = st.signup("Root@Example.com", "Root", "Admin", "passw0rd!x")
    assert user["roles"] == ["admin"] and user["surfaces"] == ["admin", "synapse"]
    assert "users.manage" in user["permissions"] and user["username"] == "root"
    assert st.session_user(token)["email"] == "Root@Example.com"
    assert st.session_user("not-a-token") is None
    analyst, _ = st.signup("ana@example.com", "Ana Lyst", "passw0rd!y")
    assert analyst["roles"] == ["analyst"] and analyst["surfaces"] == ["synapse"]
    assert analyst["username"] == "ana" and analyst["first_name"] == ""


def test_duplicate_and_domain_and_invitation_rules():
    st = store(allowed_email_domains=("example.com",), open_signup=False)
    st.signup("root@example.com", "Root", "passw0rd!x")            # the bootstrap admin may
    with pytest.raises(ValueError, match="invitation"):
        st.signup("someone@example.com", "Some One", "passw0rd!x")
    open_store = store(allowed_email_domains=("example.com",))
    with pytest.raises(ValueError, match="domains"):
        open_store.signup("x@other.org", "X", "passw0rd!x")
    open_store.signup("a@example.com", "A", "passw0rd!x")
    with pytest.raises(ValueError, match="already exists"):
        open_store.signup("A@Example.com", "A again", "passw0rd!x")


def test_login_lockout_and_reset():
    st = store()
    user, _ = st.signup("ana@example.com", "Ana", "passw0rd!y")
    for _ in range(2):
        with pytest.raises(ValueError, match="invalid email or password"):
            st.login("ana@example.com", "wrong")
    with pytest.raises(ValueError, match="locked"):
        st.login("ana@example.com", "passw0rd!y")
    assert st.direct_reset_password("ana@example.com", "newpassw0rd!") == user["user_id"]
    again, token = st.login("ana@example.com", "newpassw0rd!")
    assert again["status"] == "active" and st.session_user(token)["user_id"] == user["user_id"]
    with pytest.raises(ValueError):
        st.login("ana@example.com", "passw0rd!y")               # the old password is retired
    attempts = st._query("SELECT Reason, Succeeded FROM LoginAttempts ORDER BY OccurredAt")
    assert [a["Reason"] for a in attempts] == ["bad_password", "bad_password", "locked", "ok", "bad_password"]
    assert [bool(a["Succeeded"]) for a in attempts] == [False, False, False, True, False]


def test_roles_grant_revoke_and_sync_from_groups():
    st = store()
    user, _ = st.signup("ana@example.com", "Ana", "passw0rd!y")
    uid = user["user_id"]
    assert st.grant_role(uid, "steward")["roles"] == ["analyst", "steward"]
    assert st.revoke_role(uid, "analyst")["roles"] == ["steward"]
    assert st.revoke_role(uid, "analyst") is None                  # nothing active to revoke
    with pytest.raises(ValueError, match="unknown role"):
        st.grant_role(uid, "wizard")
    assert st.set_roles(uid, ["admin", "ghost"])["roles"] == ["admin"]
    assert st.grant_role("no-such-user", "admin") is None


def test_status_changes_end_sessions_and_delete_is_soft():
    st = store()
    user, token = st.signup("ana@example.com", "Ana", "passw0rd!y")
    assert st.update_user(user["user_id"], name="Ana L.", status="disabled")["status"] == "disabled"
    assert st.session_user(token) is None
    with pytest.raises(ValueError):
        st.update_user(user["user_id"], status="weird")
    assert st.delete_user(user["user_id"]) is True
    assert st.user(user["user_id"]) is None and st.delete_user(user["user_id"]) is False
    assert [u["email"] for u in st.list_users()] == []


def test_audit_rows_and_login_attempt_shapes():
    st = store()
    user, _ = st.signup("root@example.com", "Root", "passw0rd!x")
    st.record_audit("role.granted", "success", actor_user_id=user["user_id"],
                    subject_user_id=user["user_id"], details={"role": "admin"},
                    ip="127.0.0.1", user_agent="ua", request_id="r1")
    rows = st._query("SELECT Action, Outcome, Ip, RequestId, Details FROM AuditEvents "
                     "WHERE Action = @a", {"a": "role.granted"})
    assert rows[0]["Outcome"] == "success" and rows[0]["Ip"] == "127.0.0.1"
    assert '"role": "admin"' in str(rows[0]["Details"])


def test_external_identity_links_and_relinks():
    st = store()
    root, _ = st.signup("root@example.com", "Root", "passw0rd!x")
    issuer = "https://org.example/oauth2/as1"
    person = st.find_or_create_external_user(
        provider="okta", issuer=issuer, subject="00u1", email="okta.person@example.com",
        name="Okta Person", first_name="Okta", last_name="Person",
        claims={"groups": ["g1"]}, default_roles=["analyst"])
    assert person["roles"] == ["analyst"] and person["name"] == "Okta Person"
    again = st.find_or_create_external_user(provider="okta", issuer=issuer, subject="00u1",
                                            email="okta.person@example.com")
    assert again["user_id"] == person["user_id"]
    linked = st.find_or_create_external_user(provider="okta", issuer=issuer, subject="00u9",
                                             email="Root@example.com", default_roles=["analyst"])
    assert linked["user_id"] == root["user_id"] and linked["roles"] == ["admin"]
    with pytest.raises(ValueError, match="email"):
        st.find_or_create_external_user(provider="okta", issuer=issuer, subject="00u2", email="")


def test_one_time_states():
    st = store()
    st.put_state("s1", "okta_signin", {"verifier": "v", "nonce": "n"}, ttl_seconds=60)
    assert st.pop_state("s1", "okta_signin") == {"verifier": "v", "nonce": "n"}
    assert st.pop_state("s1", "okta_signin") is None
    st.put_state("s2", "google_connect", {}, user_id="u1")
    assert st.pop_state("s2", "okta_signin") is None                # another kind, consumed
    st.put_state("s3", "okta_signin", {}, ttl_seconds=-1)
    assert st.pop_state("s3", "okta_signin") is None                # expired


def test_google_connection_round_trip_is_encrypted_at_rest():
    st = store()
    user, _ = st.signup("root@example.com", "Root", "passw0rd!x")
    google = GoogleOAuthSettings(client_id="c", client_secret="s", redirect_uri="https://x/cb",
                                 token_encryption_key=Fernet.generate_key().decode())
    st.save_google_connection(user["user_id"], subject="g1", email="root@example.com",
                              refresh_token="rt-secret", scopes=["a", "b"], settings=google)
    row = st.google_connection(user["user_id"])
    assert row["Scopes"] == ["a", "b"] and b"rt-secret" not in bytes(row["RefreshTokenCiphertext"])
    from sahs.util.google_auth.oauth import decrypt_refresh_token
    assert decrypt_refresh_token(row["RefreshTokenCiphertext"], google) == "rt-secret"
    st.revoke_google_connection(user["user_id"])
    assert st.google_connection(user["user_id"]) is None


def test_helpers():
    assert normalize_email("  A@B.Co ") == "a@b.co"
    assert len(hash_token("x")) == 32
