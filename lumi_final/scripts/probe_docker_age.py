"""Discover running Docker containers that look like Postgres/AGE and
emit ready-to-paste `export` commands for LUMI's env vars.

Usage:
    python scripts/probe_docker_age.py            # discover + print exports
    eval "$(python scripts/probe_docker_age.py --exports-only)"   # apply

Detection rules:
    A container is a candidate if ANY of these hold:
      - its image name contains "age" or "postgres"
      - it exposes container-port 5432
      - it has a POSTGRES_USER env var

For each candidate the script prints:
    - container id, name, image, status
    - host:port mapping for 5432/tcp
    - POSTGRES_USER / PASSWORD / DB (from container env)
    - whether `LOAD 'age';` succeeds (if psycopg is installed)
    - the exact `export` block to paste, or `--exports-only` for eval.

Exits 0 if at least one usable AGE container is found, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field


# ─── pretty print ─────────────────────────────────────────────

def _hdr(msg: str) -> None:
    print(f"\n\033[1;36m── {msg} ──\033[0m")

def _pass(msg: str) -> None:
    print(f"  \033[1;32m✓\033[0m {msg}")

def _fail(msg: str) -> None:
    print(f"  \033[1;31m✗\033[0m {msg}")

def _info(msg: str) -> None:
    print(f"    \033[2m{msg}\033[0m")


# ─── docker introspection ─────────────────────────────────────

@dataclass
class Candidate:
    cid: str
    name: str
    image: str
    status: str
    host_port: str | None = None              # host port mapped to 5432
    pg_user: str = "postgres"
    pg_password: str = "postgres"
    pg_db: str = "postgres"
    env: dict[str, str] = field(default_factory=dict)
    age_loadable: bool | None = None          # tri-state: None = not checked
    age_error: str = ""


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        _fail("`docker` not on PATH.")
        return False
    try:
        subprocess.run(
            ["docker", "info"], capture_output=True, check=True, timeout=5,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        _fail(f"`docker info` failed: {e}")
        _info("Is Docker Desktop running?")
        return False
    return True


def _list_containers() -> list[dict]:
    """Return parsed `docker ps --format json` rows (all running)."""
    out = subprocess.run(
        ["docker", "ps", "--format", "{{json .}}"],
        capture_output=True, text=True, check=True,
    ).stdout
    rows = []
    for line in out.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _inspect(cid: str) -> dict:
    """Full `docker inspect` for one container."""
    out = subprocess.run(
        ["docker", "inspect", cid],
        capture_output=True, text=True, check=True,
    ).stdout
    return json.loads(out)[0]


def _is_candidate(row: dict, inspected: dict) -> bool:
    image = (row.get("Image") or "").lower()
    if "age" in image or "postgres" in image:
        return True
    # exposed port 5432
    ports = inspected.get("NetworkSettings", {}).get("Ports") or {}
    if "5432/tcp" in ports:
        return True
    env = inspected.get("Config", {}).get("Env") or []
    if any(e.startswith("POSTGRES_USER=") for e in env):
        return True
    return False


def _extract_candidate(row: dict, inspected: dict) -> Candidate:
    env_list = inspected.get("Config", {}).get("Env") or []
    env = {}
    for e in env_list:
        if "=" in e:
            k, v = e.split("=", 1)
            env[k] = v

    host_port = None
    ports = inspected.get("NetworkSettings", {}).get("Ports") or {}
    bindings = ports.get("5432/tcp") or []
    if bindings:
        host_port = bindings[0].get("HostPort")

    return Candidate(
        cid=row.get("ID", "")[:12],
        name=row.get("Names", ""),
        image=row.get("Image", ""),
        status=row.get("Status", ""),
        host_port=host_port,
        pg_user=env.get("POSTGRES_USER", "postgres"),
        pg_password=env.get("POSTGRES_PASSWORD", "postgres"),
        pg_db=env.get("POSTGRES_DB", env.get("POSTGRES_USER", "postgres")),
        env=env,
    )


# ─── AGE connectivity check ───────────────────────────────────

def _check_age_loadable(c: Candidate) -> None:
    """Try to connect and `LOAD 'age';`. Fills c.age_loadable + c.age_error."""
    try:
        import psycopg
    except ImportError:
        c.age_loadable = None
        c.age_error = "psycopg not installed (pip install 'psycopg[binary]')"
        return

    if not c.host_port:
        c.age_loadable = False
        c.age_error = "no host port mapped to 5432/tcp"
        return

    try:
        with psycopg.connect(
            host="localhost",
            port=int(c.host_port),
            dbname=c.pg_db,
            user=c.pg_user,
            password=c.pg_password,
            connect_timeout=5,
        ) as conn, conn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute("SET search_path = ag_catalog, \"$user\", public;")
            cur.execute("SELECT count(*) FROM ag_catalog.ag_graph;")
            n = cur.fetchone()[0]
            c.age_loadable = True
            c.age_error = f"AGE loaded; {n} graph(s) present"
    except Exception as e:
        c.age_loadable = False
        c.age_error = f"{type(e).__name__}: {e}"


# ─── exports emit ─────────────────────────────────────────────

def _emit_exports(c: Candidate, *, quiet: bool = False) -> str:
    graph = os.environ.get("LUMI_PG_GRAPH", "lumi_semantic")
    block = (
        f"export LUMI_AGE_ENABLED=1\n"
        f"export LUMI_PG_HOST=localhost\n"
        f"export LUMI_PG_PORT={c.host_port}\n"
        f"export LUMI_PG_DATABASE={c.pg_db}\n"
        f"export LUMI_PG_USER={c.pg_user}\n"
        f"export LUMI_PG_PASSWORD={c.pg_password}\n"
        f"export LUMI_PG_GRAPH={graph}\n"
    )
    if quiet:
        sys.stdout.write(block)
    return block


# ─── main ─────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--exports-only", action="store_true",
        help="Emit only the export block for the best candidate (eval-friendly).",
    )
    parser.add_argument(
        "--skip-age-check", action="store_true",
        help="Don't try to connect / LOAD 'age'; just inspect Docker.",
    )
    args = parser.parse_args()

    if not args.exports_only:
        _hdr("Docker availability")
    if not _docker_available():
        return 1
    if not args.exports_only:
        _pass("docker daemon reachable")

    if not args.exports_only:
        _hdr("Scanning running containers")
    try:
        rows = _list_containers()
    except subprocess.CalledProcessError as e:
        _fail(f"docker ps failed: {e}")
        return 1

    candidates: list[Candidate] = []
    for row in rows:
        try:
            ins = _inspect(row["ID"])
        except subprocess.CalledProcessError:
            continue
        if _is_candidate(row, ins):
            candidates.append(_extract_candidate(row, ins))

    if not candidates:
        if not args.exports_only:
            _fail("No Postgres/AGE-looking containers running.")
            _info("Start one with:")
            _info("  docker run -d --name lumi-age -p 5432:5432 \\")
            _info("    -e POSTGRES_PASSWORD=postgres apache/age:latest")
        return 1

    if not args.exports_only:
        _pass(f"Found {len(candidates)} candidate container(s).")

    # AGE check for each
    if not args.skip_age_check:
        for c in candidates:
            _check_age_loadable(c)

    # rank: AGE-loadable first, then port-mapped, then by name
    candidates.sort(
        key=lambda c: (
            0 if c.age_loadable is True else (1 if c.age_loadable is None else 2),
            0 if c.host_port else 1,
            c.name,
        ),
    )

    if not args.exports_only:
        _hdr("Candidates")
        for i, c in enumerate(candidates):
            tag = "BEST" if i == 0 else f"#{i+1}"
            print(f"\n  [{tag}] {c.name}  ({c.cid})")
            _info(f"image:    {c.image}")
            _info(f"status:   {c.status}")
            _info(f"host port → 5432: {c.host_port or '(not mapped)'}")
            _info(f"POSTGRES_USER:     {c.pg_user}")
            _info(f"POSTGRES_PASSWORD: {c.pg_password}")
            _info(f"POSTGRES_DB:       {c.pg_db}")
            if c.age_loadable is True:
                _pass(f"AGE: {c.age_error}")
            elif c.age_loadable is False:
                _fail(f"AGE: {c.age_error}")
            else:
                _info(f"AGE: skipped — {c.age_error}")

    best = candidates[0]
    if best.age_loadable is False:
        if not args.exports_only:
            _hdr("Result")
            _fail("Best candidate cannot LOAD 'age'. Exports NOT emitted.")
            _info(f"Reason: {best.age_error}")
            _info("If this container isn't AGE-enabled, stop it and run apache/age:latest.")
        return 1

    if args.exports_only:
        _emit_exports(best, quiet=True)
        return 0

    _hdr("Ready-to-paste exports")
    print()
    print(_emit_exports(best))
    print("Or apply in one shot:")
    print("  eval \"$(python scripts/probe_docker_age.py --exports-only)\"")
    return 0


if __name__ == "__main__":
    sys.exit(main())
