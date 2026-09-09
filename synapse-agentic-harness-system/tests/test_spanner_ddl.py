"""The Spanner DDL under db/spanner/, held to the database's rules
and to the Python registries without a Spanner: the lint finds
nothing, and the shape the design promises is there."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

SILO = Path(__file__).resolve().parents[1]
DDL = SILO / "db" / "spanner"

_spec = importlib.util.spec_from_file_location(
    "spanner_ddl_check", SILO / "scripts" / "spanner_ddl_check.py")
lint = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lint)


def _text(name: str) -> str:
    return (DDL / name).read_text(encoding="utf-8")


def test_the_lint_finds_nothing():
    assert lint.check() == []


def test_identity_holds_nothing_recoverable_and_roles_are_rows():
    ddl = _text("001_identity.sql")
    assert "CREATE UNIQUE INDEX UsersByEmail ON Users (EmailNormalized)" in ddl
    assert "AS (LOWER(TRIM(Email))) STORED" in ddl
    assert "PasswordHash    STRING(512) NOT NULL" in ddl
    assert "PepperVersion" in ddl and "argon2id" in ddl
    assert "TokenHash         BYTES(32)   NOT NULL" in ddl
    assert "CREATE UNIQUE INDEX AuthSessionsByToken ON AuthSessions (TokenHash)" in ddl
    assert "ReuseDetectedAt" in ddl                         # refresh families
    assert "SecretCiphertext BYTES(1024)" in ddl and "KmsKeyVersion" in ddl
    assert "CREATE CHANGE STREAM AuditStream FOR AuditEvents" in ddl
    for policy in ("OLDER_THAN(AbsoluteExpiresAt, INTERVAL 7 DAY)",
                   "OLDER_THAN(OccurredAt, INTERVAL 30 DAY)",
                   "OLDER_THAN(OccurredAt, INTERVAL 400 DAY)",
                   "OLDER_THAN(ExpiresAt, INTERVAL 1 DAY)"):
        assert policy in ddl, policy
    assert "Surfaces      ARRAY<STRING(16)>" in ddl
    seed = ddl.split("INSERT INTO Roles", 1)[1]
    assert "('admin'" in seed and "['lumi', 'synapse']" in seed
    assert "('analyst'" in seed and "('steward'" in seed
    assert "'users.manage'" in seed and "'metrics.certify'" in seed
    assert "INSERT INTO RolePermissions" in seed
    # the current password is read by key range, never by an index
    # that would keep only the retired rows
    assert "ActiveCredentialByUser" not in ddl
    # the five phase-2 tables are marked so
    assert ddl.count("phase 2: applied with the file, empty in the first "
                     "rollout") == 4
    # nothing keyed by a sequence or a time
    assert "DEFAULT (GENERATE_UUID())" in ddl
    assert not re.search(r"PRIMARY KEY \((CreatedAt|OccurredAt)", ddl)


def test_the_chat_store_is_the_sqlite_store_with_a_person():
    ddl = _text("002_chat.sql")
    for table in ("ChatProjects", "ChatSessions", "ChatMessages",
                  "ChatArtifacts", "ChatPlanVersions", "ChatFeedback",
                  "ChatFiles", "ChatEvents", "ChatMemories", "UserSkills",
                  "KnowledgeFiles"):
        assert f"CREATE TABLE {table} (" in ddl, table
    assert ddl.count("INTERLEAVE IN PARENT ChatSessions ON DELETE CASCADE") == 6
    assert "INTERLEAVE IN PARENT Users ON DELETE CASCADE" in ddl   # memory, skills
    assert "OwnerUserId   STRING(36)  NOT NULL" in ddl
    assert "CHECK (Model IN ('', 'vertex', 'eag'))" in ddl
    assert "Text_Tokens TOKENLIST   AS (TOKENIZE_FULLTEXT(Text)) HIDDEN" in ddl
    # a search spans a person's chats: the index partitions by the owner
    assert "PARTITION BY OwnerUserId" in ddl
    assert "OwnerUserId STRING(36)  NOT NULL" in ddl.split(
        "CREATE TABLE ChatMessages", 1)[1].split(") PRIMARY KEY", 1)[0]
    assert "CREATE INDEX UserSkillsShared ON UserSkills (Shared, Name)" in ddl
    assert "CREATE CHANGE STREAM ChatEventsStream FOR ChatEvents" in ddl
    assert "ObjectPath" in ddl and "SentTurn" in ddl
    assert "IngestedRun" in ddl


def test_the_graph_is_assertions_plus_a_fold_and_a_property_graph():
    from sahs.graph.ids import ID_PATTERNS
    from sahs.graph.quads import RELATIONS, WITNESSES
    ddl = _text("003_graph.sql")
    for table in ("GraphRuns", "GraphNodeAssertions", "GraphNodes",
                  "GraphEdgeAssertions", "GraphEdges", "GraphCrosswalk",
                  "GraphStatusTransitions", "Builds"):
        assert f"CREATE TABLE {table} (" in ddl, table
    assert "PRIMARY KEY (SubjectId, Relation, ObjectId, Witness, Seq)" in ddl
    assert "PRIMARY KEY (SubjectId, Relation, ObjectId, Witness);" in ddl
    assert "CREATE PROPERTY GRAPH SynapseGraph" in ddl
    assert "DYNAMIC LABEL (Kind)" in ddl and "DYNAMIC LABEL (Relation)" in ddl
    assert "DYNAMIC PROPERTIES (Props)" in ddl
    assert "SOURCE KEY (SubjectId) REFERENCES GraphNodes (NodeId)" in ddl
    for relation in RELATIONS:
        assert f"'{relation}'" in ddl, relation
    for witness in WITNESSES:
        assert f"'{witness}'" in ddl, witness
    for kind in ID_PATTERNS:
        assert f"'{kind}'" in ddl, kind
    assert "CHECK (Status IN ('active', 'superseded', 'retracted'))" in ddl


def test_the_readme_and_the_design_say_how():
    readme = (DDL / "README.md").read_text(encoding="utf-8")
    assert not (SILO / "docs" / "specs" / "spanner_schema.md").exists()
    design = (SILO / "docs" / "spanner_schema.md").read_text(
        encoding="utf-8")
    assert "gcloud spanner databases ddl update" in readme
    assert "DYNAMIC LABEL" in readme and "static labels" in readme
    assert "docs/spanner_schema.md" in readme
    for piece in ("Argon2id", "Secret Manager", "SameSite=Lax",
                  "Refresh tokens rotate in families", "NIST 800-63B",
                  "| `admin` | `lumi`, `synapse` |",
                  "| `analyst` | `synapse` |",
                  "GRAPH SynapseGraph", "one quad per witness family",
                  "POST /api/auth/login", "POST /api/auth/signup",
                  "the steward's permission set",
                  "PARTITION BY OwnerUserId",
                  # the first rollout, decided
                  "No second factor, no email verification, no invitation",
                  "The graph stays on the filesystem",
                  "No SQLite anywhere in the deployment",
                  "`SAHS_STORE=local`", "`SAHS_STORE=spanner`",
                  "`AUTH_PEPPER`", "`AUTH_BOOTSTRAP_ADMIN_EMAIL`"):
        assert piece in design, piece
    # every table in the DDL is explained by name
    for name in ("001_identity.sql", "002_chat.sql", "003_graph.sql"):
        for table in re.findall(r"CREATE TABLE (\w+)", _text(name)):
            assert f"`{table}`" in design, table


def test_the_env_example_carries_the_spanner_block():
    env = (SILO / ".env.example").read_text(encoding="utf-8")
    for var in ("SAHS_STORE=", "SPANNER_PROJECT_ID", "SPANNER_INSTANCE_ID",
                "SPANNER_DATABASE_ID", "LUMI_SPANNER_SA_KEY",
                "SPANNER_EMULATOR_HOST", "AUTH_PEPPER", "AUTH_SESSION_HOURS",
                "AUTH_IDLE_MINUTES", "AUTH_BOOTSTRAP_ADMIN_EMAIL",
                "AUTH_OPEN_SIGNUP", "AUTH_ALLOWED_EMAIL_DOMAINS",
                "AUTH_DEFAULT_ROLE", "AUTH_LOCK_AFTER", "SAHS_FILES_DIR"):
        assert var in env, var
    # every variable the design names is in the example, and the other way
    design = (SILO / "docs" / "spanner_schema.md").read_text(
        encoding="utf-8")
    named = set(re.findall(r"`((?:SAHS_STORE|SAHS_FILES_DIR|SPANNER_[A-Z_]+"
                           r"|AUTH_[A-Z_]+|LUMI_SPANNER_SA_KEY))`", design))
    for var in named:
        assert var in env, var
