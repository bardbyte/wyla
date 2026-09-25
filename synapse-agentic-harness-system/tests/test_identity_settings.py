"""The three settings objects and the store switch, from environments:
their readers (sahs/util/spanner/settings.py) behind our facade, plus
the sqlite stand-in, the lockout dials and the local-login flag."""

from __future__ import annotations

import pytest

from sahs.spanner import (AuthSettings, GoogleOAuthConfigurationError, GoogleOAuthSettings,
                          SpannerConfigurationError, SpannerSettings, grpc_endpoint,
                          spanner_is_enabled, store_mode)


def test_store_switch():
    assert store_mode({}) == "local" and not spanner_is_enabled({})
    assert spanner_is_enabled({"SAHS_STORE": "sqlite"}) and spanner_is_enabled({"SAHS_STORE": "spanner"})
    with pytest.raises(SpannerConfigurationError):
        store_mode({"SAHS_STORE": "postgres"})


def test_spanner_settings_refuse_incomplete_and_accept_complete():
    with pytest.raises(SpannerConfigurationError, match="SAHS_STORE=local"):
        SpannerSettings.from_env({})
    with pytest.raises(SpannerConfigurationError, match="SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID"):
        SpannerSettings.from_env({"SAHS_STORE": "spanner", "SPANNER_PROJECT_ID": "p"})
    s = SpannerSettings.from_env({"SAHS_STORE": "spanner", "SPANNER_PROJECT_ID": "p",
                                  "SPANNER_INSTANCE_ID": "i", "SPANNER_DATABASE_ID": "d",
                                  "SPANNER_EMULATOR_HOST": "localhost:9010"})
    assert s.database_path == "projects/p/instances/i/databases/d"
    assert s.emulator_host == "localhost:9010" and s.mode == "spanner"
    plain = SpannerSettings.from_env({"SAHS_STORE": "spanner", "SPANNER_PROJECT_ID": "p",
                                      "SPANNER_INSTANCE_ID": "i", "SPANNER_DATABASE_ID": "d",
                                      "SPANNER_URL": "https://spanner.example/"})
    assert plain.endpoint == "https://spanner.example" and plain.emulator_host is None
    lite = SpannerSettings.from_env({"SAHS_STORE": "sqlite", "SAHS_IDENTITY_SQLITE": "/tmp/x.db"})
    assert lite.mode == "sqlite" and lite.sqlite_path == "/tmp/x.db"


def test_inline_service_account_json_is_understood():
    doc = '{"type": "service_account", "client_email": "x@y"}'
    s = SpannerSettings.from_env({"SAHS_STORE": "spanner", "SPANNER_PROJECT_ID": "p",
                                  "SPANNER_INSTANCE_ID": "i", "SPANNER_DATABASE_ID": "d",
                                  "SYNAPSE_SPANNER_SA_KEY": doc})
    assert s.credentials_data["client_email"] == "x@y" and s.credentials_path is None
    with pytest.raises(SpannerConfigurationError, match="not found"):
        SpannerSettings.from_env({"SAHS_STORE": "spanner", "SPANNER_PROJECT_ID": "p",
                                  "SPANNER_INSTANCE_ID": "i", "SPANNER_DATABASE_ID": "d",
                                  "SYNAPSE_SPANNER_SA_KEY": "/no/such/key.json"})


def test_grpc_endpoint_forms():
    assert grpc_endpoint("https://spanner.googleapis.com") == "spanner.googleapis.com:443"
    assert grpc_endpoint("https://private.example:8443/path") == "private.example:8443"
    assert grpc_endpoint("localhost:9010") == "localhost:9010"
    assert grpc_endpoint("") == "spanner.googleapis.com:443"


def test_auth_settings_defaults_and_flags():
    a = AuthSettings.from_env({})                         # no store: lenient, nothing demanded
    assert a.session_hours == 12 and a.cookie_secure == "auto" and not a.local_login_enabled
    b = AuthSettings.from_env({"AUTH_LOCAL_LOGIN": "1", "AUTH_ALLOWED_EMAIL_DOMAINS": " Example.com, @corp.org",
                               "AUTH_COOKIE_SECURE": "false", "AUTH_LOCK_AFTER": "3"})
    assert b.local_login_enabled and b.allowed_email_domains == ("example.com", "corp.org")
    assert b.cookie_secure == "false" and b.lock_after == 3
    with pytest.raises(SpannerConfigurationError):
        AuthSettings.from_env({"AUTH_COOKIE_SECURE": "maybe"})
    with pytest.raises(SpannerConfigurationError):
        AuthSettings.from_env({"AUTH_LOCK_AFTER": "many"})


def test_auth_settings_with_a_store_hold_their_rules():
    with pytest.raises(SpannerConfigurationError, match="AUTH_PEPPER"):
        AuthSettings.from_env({"SAHS_STORE": "sqlite", "AUTH_PEPPER": "short"})
    with pytest.raises(SpannerConfigurationError, match="positive"):
        AuthSettings.from_env({"SAHS_STORE": "spanner", "AUTH_PEPPER": "long-enough",
                               "AUTH_SESSION_HOURS": "0"})
    with pytest.raises(SpannerConfigurationError, match="forbidden"):
        AuthSettings.from_env({"SAHS_STORE": "spanner", "AUTH_PEPPER": "long-enough",
                               "AUTH_ALLOW_INSECURE_DIRECT_RESET": "1", "EPAAS_ENV": "e3"})
    a = AuthSettings.from_env({"SAHS_STORE": "sqlite", "AUTH_PEPPER": "long-enough",
                               "AUTH_DEFAULT_ROLE": "Steward", "AUTH_LOCK_MINUTES": "2",
                               "AUTH_LOCAL_LOGIN": "1"})
    assert a.default_role == "steward" and a.lock_after == 5 and a.lock_minutes == 2
    assert a.local_login_enabled and a.pepper == "long-enough"


def test_google_oauth_settings():
    with pytest.raises(GoogleOAuthConfigurationError, match="GOOGLE_OAUTH_CLIENT_ID"):
        GoogleOAuthSettings.from_env({})
    g = GoogleOAuthSettings.from_env({"GOOGLE_OAUTH_CLIENT_ID": "c", "GOOGLE_OAUTH_CLIENT_SECRET": "s",
                                      "GOOGLE_OAUTH_REDIRECT_URI": "https://x/cb",
                                      "GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY": "k"})
    assert g.scopes == ("openid", "email", "https://www.googleapis.com/auth/userinfo.email",
                        "https://www.googleapis.com/auth/bigquery")        # fixed: the consent flow needs these
    assert g.post_connect_uri == "/synapse-admin/#/account?google=connected"
    with pytest.raises(GoogleOAuthConfigurationError, match="deployed callback host"):
        GoogleOAuthSettings.from_env({"GOOGLE_OAUTH_CLIENT_ID": "c", "GOOGLE_OAUTH_CLIENT_SECRET": "s",
                                      "GOOGLE_OAUTH_REDIRECT_URI": "http://localhost:8400/cb",
                                      "GOOGLE_OAUTH_TOKEN_ENCRYPTION_KEY": "k", "EPAAS_ENV": "e1"})
