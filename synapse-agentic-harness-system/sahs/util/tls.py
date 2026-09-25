"""Corporate TLS trust helpers for outbound Google and gateway calls.

The certificate package and the secret mount are named by the
environment, and neither is consulted when unset:

    SAHS_CA_PACKAGE   an importable package exposing ``certificate_path()``
    SAHS_SECRETS_DIR  a directory holding ``ca-bundle.crt``,
                      ``ca_bundle.crt`` or ``cacert.pem``

Unset means neither is consulted, and only the ``*_CA_BUNDLE`` /
``SSL_CERT_FILE`` variables can name a bundle.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path

_CA_ENV_NAMES = ("GATEWAY_CA_BUNDLE", "GEMINI_CA_BUNDLE", "REQUESTS_CA_BUNDLE",
                 "SSL_CERT_FILE")
_COMMON_CA_NAMES = ("ca-bundle.crt", "ca_bundle.crt", "cacert.pem")


def _packaged_certificate_path() -> str:
    package = os.environ.get("SAHS_CA_PACKAGE", "").strip()
    if not package:
        return ""
    try:
        module = importlib.import_module(package)
        return str(module.certificate_path() or "")
    except (ImportError, AttributeError, TypeError, ValueError):
        return ""


def _common_ca_paths() -> tuple[Path, ...]:
    directory = os.environ.get("SAHS_SECRETS_DIR", "").strip()
    if not directory:
        return ()
    return tuple(Path(directory) / name for name in _COMMON_CA_NAMES)


def corporate_ca_bundle() -> str:
    """Return a configured or packaged corporate CA bundle path, if present."""
    for name in _CA_ENV_NAMES:
        value = os.environ.get(name, "").strip()
        if value and Path(value).is_file():
            return value
    path = _packaged_certificate_path()
    if path and Path(path).is_file():
        return path
    for candidate in _common_ca_paths():
        if candidate.is_file():
            return str(candidate)
    return ""


def apply_tls_environment() -> str:
    """Populate common TLS env vars when a corporate CA bundle is available."""
    bundle = corporate_ca_bundle()
    if not bundle:
        return ""
    for name in ("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", "SSL_CERT_FILE",
                 "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"):
        os.environ.setdefault(name, bundle)
    return bundle


__all__ = ["apply_tls_environment", "corporate_ca_bundle"]
