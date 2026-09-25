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
    # the column's type and nullability, not its alignment: the file is
    # hand-aligned, and a second copy may pad columns differently
    assert re.search(r"PasswordHash\s+STRING\(512\)\s+NOT NULL", ddl)
    assert "PepperVersion" in ddl and "argon2id" in ddl
    assert re.search(r"TokenHash\s+BYTES\(32\)\s+NOT NULL", ddl)
    assert "CREATE UNIQUE INDEX AuthSessionsByToken ON AuthSessions (TokenHash)" in ddl
    assert "ReuseDetectedAt" in ddl                         # refresh families
    assert "SecretCiphertext BYTES(1024)" in ddl and "KmsKeyVersion" in ddl
    assert "CREATE CHANGE STREAM AuditStream FOR AuditEvents" in ddl
    for policy in ("OLDER_THAN(AbsoluteExpiresAt, INTERVAL 7 DAY)",
                   "OLDER_THAN(OccurredAt, INTERVAL 30 DAY)",
                   "OLDER_THAN(OccurredAt, INTERVAL 400 DAY)",
                   "OLDER_THAN(ExpiresAt, INTERVAL 1 DAY)"):
        assert policy in ddl, policy
    assert re.search(r"Surfaces\s+ARRAY<STRING\(16\)>", ddl)
    seed = ddl.split("INSERT INTO Roles", 1)[1]
    assert "('admin'" in seed and "['admin', 'synapse']" in seed
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
    assert "CHECK (Model IN ('', 'vertex', 'gateway'))" in ddl
    assert "Text_Tokens TOKENLIST   AS (TOKENIZE_FULLTEXT(Text)) HIDDEN" in ddl
    # a search spans a person's chats: the index partitions by the owner
    assert "PARTITION BY OwnerUserId" in ddl
    assert "OwnerUserId STRING(36)  NOT NULL" in ddl.split(
        "CREATE TABLE ChatMessages", 1)[1].split(") PRIMARY KEY", 1)[0]
    assert "CREATE INDEX UserSkillsShared ON UserSkills (Shared, Name)" in ddl
    assert "CREATE CHANGE STREAM ChatEventsStream FOR ChatEvents" in ddl
    assert "ObjectPath" in ddl and "SentTurn" in ddl
    assert "IngestedRun" in ddl


def test_008_widens_the_model_choice_and_lists_every_artifact_type(tmp_path):
    """002 then 008 applied to the lint's model: ChatSessions.Model is
    STRING(64) with no plane CHECK left (the catalog owns the choices),
    and ChatArtifacts' type list is exactly sahs.assistant.artifacts.TYPES
    (kpi included) — so a fresh database and the live one with 008 on
    top end up the same. The lint reads the three ALTER forms, applies
    them in file order, and refuses what it cannot read."""
    from sahs.assistant.artifacts import TYPES
    from sahs.assistant.spanner_store import MODEL_CHOICE_CHARS
    ddl = _text("008_chat_model.sql")
    assert "ALTER TABLE ChatSessions DROP CONSTRAINT ck_sessions_model;" in ddl
    assert ("ALTER TABLE ChatSessions ALTER COLUMN Model STRING(64) "
            "NOT NULL DEFAULT ('');") in ddl
    assert "ALTER TABLE ChatArtifacts DROP CONSTRAINT ck_artifacts_type;" in ddl
    assert "ALTER TABLE ChatArtifacts ADD CONSTRAINT ck_artifacts_type CHECK" in ddl
    for kind in TYPES:
        assert f"'{kind}'" in ddl, kind
    assert MODEL_CHOICE_CHARS == 64
    # before 008: the first rollout's narrow column and lists
    before, findings = lint.load([DDL / "001_identity.sql", DDL / "002_chat.sql"])
    assert findings == []
    assert before["ChatSessions"].columns["Model"] == "STRING(16)"
    assert "ck_sessions_model" in before["ChatSessions"].constraints
    assert lint.check_list_in(before["ChatArtifacts"], "ck_artifacts_type") == {
        "chart", "table", "document", "dashboard", "diagram", "query"}
    # after 008: 64 wide, no plane CHECK, the registry's types
    after, findings = lint.load([DDL / "001_identity.sql", DDL / "002_chat.sql",
                                 DDL / "008_chat_model.sql"])
    assert findings == []
    assert after["ChatSessions"].columns["Model"] == "STRING(64)"
    assert "ck_sessions_model" not in after["ChatSessions"].constraints
    assert not any("Model IN" in text
                   for text in after["ChatSessions"].constraints.values())
    assert lint.check_list_in(after["ChatArtifacts"], "ck_artifacts_type") == set(TYPES)
    assert "kpi" in set(TYPES)
    # the lint sees every table, the widened column included, and 9 files
    tables, _ = lint.load(lint.ddl_files())
    assert len(tables) == 44 and len(lint.ddl_files()) == 9
    assert tables["ChatSessions"].columns["Model"] == "STRING(64)"
    # what it refuses: a constraint that is not there, a column that is
    # not there, a form it does not read, a name added twice
    bad = tmp_path / "099_bad.sql"
    bad.write_text(
        "ALTER TABLE ChatSessions DROP CONSTRAINT ck_nothing;\n"
        "ALTER TABLE ChatSessions ALTER COLUMN Nope STRING(64);\n"
        "ALTER TABLE ChatArtifacts ADD CONSTRAINT ck_artifacts_type CHECK (Type IN ('x'));\n"
        "ALTER TABLE ChatSessions RENAME TO Chats;\n"
        "ALTER TABLE Ghost DROP CONSTRAINT ck_x;\n", encoding="utf-8")
    _t, findings = lint.load([DDL / "001_identity.sql", DDL / "002_chat.sql", bad])
    assert [f.split(": ", 2)[-1][:40] for f in findings] == [
        "DROP CONSTRAINT ck_nothing: ChatSessions",
        "ALTER COLUMN Nope: ChatSessions has no s",
        "ADD CONSTRAINT ck_artifacts_type: ChatAr",
        "an ALTER form this check does not read (",
        "ALTER on unknown table Ghost"[:40]]


def test_009_adds_the_usage_columns_the_store_adds_to(tmp_path):
    """009: five ADD COLUMN statements on ChatSessions — the usage totals
    the runtime adds to when a turn ends — INT64, NOT NULL with a
    DEFAULT so a table with rows takes them; the lint applies the form,
    holds the schema to the store's own column list, and refuses a
    column added twice or a NOT NULL one with no default."""
    from sahs.assistant.spanner_store import USAGE_COLUMNS
    ddl = _text("009_usage.sql")
    for column in USAGE_COLUMNS:
        assert (f"ALTER TABLE ChatSessions ADD COLUMN {column} INT64 NOT NULL "
                "DEFAULT (0);") in ddl, column
    base = [DDL / "001_identity.sql", DDL / "002_chat.sql", DDL / "008_chat_model.sql"]
    before, _ = lint.load(base)
    assert not any(c in before["ChatSessions"].columns for c in USAGE_COLUMNS)
    after, findings = lint.load(base + [DDL / "009_usage.sql"])
    assert findings == []
    for column in USAGE_COLUMNS:
        assert after["ChatSessions"].columns[column] == "INT64", column
    assert after["ChatSessions"].columns["MessageCount"] == "INT64"
    assert lint.check() == []                      # the registry check passes
    bad = tmp_path / "099_bad.sql"
    bad.write_text(
        "ALTER TABLE ChatSessions ADD COLUMN TokensIn INT64 NOT NULL DEFAULT (0);\n"
        "ALTER TABLE ChatSessions ADD COLUMN Spent INT64 NOT NULL;\n"
        "ALTER TABLE ChatSessions ADD COLUMN Select INT64;\n", encoding="utf-8")
    _t, findings = lint.load(base + [DDL / "009_usage.sql", bad])
    assert [f.split(": ", 2)[-1][:34] for f in findings] == [
        "ADD COLUMN TokensIn: ChatSessions ",
        "ADD COLUMN Spent: NOT NULL with no",
        "ADD COLUMN Select: a reserved word"]


def test_the_content_tables_hold_the_bytes_and_the_board():
    """007: a file's bytes in chunks under its ChatFiles row, and the
    review ledger as a head row, its versions, its events and each
    person's seen-mark — keyed, interleaved, CHECKed."""
    ddl = _text("007_content.sql")
    for table in ("ChatFileChunks", "ReviewSubmissions", "ReviewVersions",
                  "ReviewEvents", "ReviewSeen"):
        assert f"CREATE TABLE {table} (" in ddl, table
    assert "PRIMARY KEY (SessionId, FileId, Seq)" in ddl
    assert "INTERLEAVE IN PARENT ChatFiles ON DELETE CASCADE" in ddl
    assert re.search(r"Chunk\s+BYTES\(MAX\)\s+NOT NULL", ddl)
    assert ddl.count("INTERLEAVE IN PARENT ReviewSubmissions ON DELETE CASCADE") == 2
    assert "INTERLEAVE IN PARENT Users ON DELETE CASCADE" in ddl      # ReviewSeen
    assert "CHECK (Kind IN ('skill', 'knowledge'))" in ddl
    assert "CHECK (Status IN ('pending', 'published', 'rejected', 'withdrawn'))" in ddl
    assert "CHECK (Ext IN ('md', 'txt', 'csv', 'json', 'yaml', 'yml', 'sql'))" in ddl
    assert "'submitted', 'resubmitted', 'ai_review', 'approved', 'rejected', 'withdrawn'" in ddl
    assert "FOREIGN KEY (SubmitterUserId) REFERENCES Users (UserId)" in ddl
    assert "PRIMARY KEY (SubmissionId, Seq)" in ddl and "Payload       JSON" in ddl
    # the ledger's by/at are reserved words in GoogleSQL: Actor, OccurredAt
    assert not re.search(r"^\s+(By|At)\s+", ddl, re.M)
    # the lists the store writes are the DDL's
    from sahs.assistant import reviews as rv
    for status in rv.STATUSES:
        assert f"'{status}'" in ddl, status
    for event in rv.EVENTS:
        assert f"'{event}'" in ddl, event
    for ext in rv.EXTS:
        assert f"'{ext}'" in ddl, ext
    # the chunk ceiling stays under Spanner's 10 MiB cell
    from sahs.assistant.content_store import CHUNK_BYTES
    assert CHUNK_BYTES <= 8 * 1024 * 1024


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


def test_the_bundle_tables_hang_under_builds_and_every_file_is_in_the_readme():
    """005: what sahs/builds/spanner_store.py writes, keyed as it reads,
    interleaved so a re-publish's DELETE FROM Builds takes the old
    bundle and its chunks with it."""
    ddl = _text("005_build_bundles.sql")
    assert "CREATE TABLE BuildBundles (" in ddl and "CREATE TABLE BuildBundleChunks (" in ddl
    assert ddl.count("INTERLEAVE IN PARENT Builds ON DELETE CASCADE") == 1
    assert ddl.count("INTERLEAVE IN PARENT BuildBundles ON DELETE CASCADE") == 1
    assert re.search(r"Chunk\s+BYTES\(MAX\)\s+NOT NULL", ddl)
    assert re.search(r"Complete\s+BOOL\s+NOT NULL DEFAULT \(false\)", ddl)
    assert "PRIMARY KEY (BuildId, Seq)" in ddl
    assert "PublishedAt  TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true)" in ddl
    readme = (DDL / "README.md").read_text(encoding="utf-8")
    for path in sorted(DDL.glob("*.sql")):
        assert f"`{path.name}`" in readme, path.name


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
                  "| `admin` | `admin`, `synapse` |",
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
                "SPANNER_DATABASE_ID", "SYNAPSE_SPANNER_SA_KEY",
                "SPANNER_EMULATOR_HOST", "AUTH_PEPPER", "AUTH_SESSION_HOURS",
                "AUTH_IDLE_MINUTES", "AUTH_BOOTSTRAP_ADMIN_EMAIL",
                "AUTH_OPEN_SIGNUP", "AUTH_ALLOWED_EMAIL_DOMAINS",
                "AUTH_DEFAULT_ROLE", "AUTH_LOCK_AFTER", "SAHS_FILES_DIR"):
        assert var in env, var
    # every variable the design names is in the example, and the other way
    design = (SILO / "docs" / "spanner_schema.md").read_text(
        encoding="utf-8")
    named = set(re.findall(r"`((?:SAHS_STORE|SAHS_FILES_DIR|SPANNER_[A-Z_]+"
                           r"|AUTH_[A-Z_]+|SYNAPSE_SPANNER_SA_KEY))`", design))
    for var in named:
        assert var in env, var
