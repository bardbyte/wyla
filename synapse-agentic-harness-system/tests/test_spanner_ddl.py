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


def test_the_readme_and_the_spec_say_how():
    readme = (DDL / "README.md").read_text(encoding="utf-8")
    spec = (SILO / "docs" / "specs" / "spanner_schema.md").read_text(
        encoding="utf-8")
    assert "gcloud spanner databases ddl update" in readme
    assert "DYNAMIC LABEL" in readme and "static labels" in readme
    for piece in ("Argon2id", "Secret Manager", "SameSite=Lax",
                  "Refresh tokens rotate in families", "NIST 800-63B",
                  "admin sees Lumi, analyst sees\nSynapse",
                  "GRAPH SynapseGraph", "one quad per witness family",
                  "POST /api/auth/login", "steward's permission set"):
        assert piece in spec, piece
