-- ============================================================
-- Synapse on Spanner · 003 · the graph, as Spanner Graph
--
-- The laptop's graph is append-only JSONL (sahs/graph/quads.py):
--   nodes/<kind>.jsonl   {"id", "props", "prov"}
--   edges/<relation>.jsonl {"s", "r", "o", "props"?, "prov"}
-- with the current state a fold where the LAST record wins per
-- identity — (id) for a node, (s, r, o, witness) for an edge — and
-- provenance on every record (source, run, witness, status, support,
-- evidence, actor). Nothing edits history.
--
-- On Spanner the same discipline is two tables per family:
--   *Assertions  — the JSONL, one row per record, append-only;
--   GraphNodes / GraphEdges — the fold, maintained by the single
--                  writer in the same transaction as the append.
-- The property graph is defined over the fold, so GQL sees the
-- current state and SQL over *Assertions sees the history.
--
-- Applied with the other two files; EMPTY in the first rollout, where
-- the graph stays on the filesystem (docs/spanner_schema.md §5).
-- ============================================================

-- A run: one build-graph invocation, the provenance anchor.
CREATE TABLE GraphRuns (
  RunId       STRING(64)  NOT NULL,
  StartedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  FinishedAt  TIMESTAMP,
  Actor       STRING(36),
  Manifest    JSON,
) PRIMARY KEY (RunId);

-- ── nodes ────────────────────────────────────────────────────
-- NodeId is the graph's own id ("table:dw.gms_transaction",
-- "metric:0141ad8167c0"), Kind its prefix (sahs/graph/ids.py). A
-- random Shard in front of the id keeps a bulk load from writing one
-- hot range; readers always know the id, so the shard is derivable.
CREATE TABLE GraphNodeAssertions (
  NodeId      STRING(300) NOT NULL,
  Seq         INT64       NOT NULL,          -- per node, ascending
  Kind        STRING(16)  NOT NULL,
  Props       JSON        NOT NULL,
  -- provenance, the Prov model flattened
  Source      STRING(48)  NOT NULL,
  RunId       STRING(64)  NOT NULL,
  Witness     STRING(24)  NOT NULL,
  Status      STRING(16)  NOT NULL DEFAULT ('active'),
  Support     INT64,
  Evidence    STRING(1000),
  Actor       STRING(36),
  Retrieved   STRING(64),
  ValidFor    ARRAY<STRING(64)> NOT NULL DEFAULT (ARRAY<STRING>[]),
  AssertedAt  TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_nodeassert_status CHECK (Status IN ('active', 'superseded', 'retracted')),
  CONSTRAINT fk_nodeassert_run FOREIGN KEY (RunId) REFERENCES GraphRuns (RunId),
) PRIMARY KEY (NodeId, Seq);

CREATE INDEX GraphNodeAssertionsByRun ON GraphNodeAssertions (RunId, Kind);

-- The fold: the last active assertion per node.
CREATE TABLE GraphNodes (
  NodeId      STRING(300) NOT NULL,
  Kind        STRING(16)  NOT NULL,
  Props       JSON        NOT NULL,
  Label       STRING(300),                   -- props.label / name, for search
  Label_Tokens TOKENLIST  AS (TOKENIZE_FULLTEXT(Label)) HIDDEN,
  Source      STRING(48)  NOT NULL,
  RunId       STRING(64)  NOT NULL,
  Witness     STRING(24)  NOT NULL,
  Status      STRING(16)  NOT NULL DEFAULT ('active'),
  Seq         INT64       NOT NULL,          -- the assertion folded
  UpdatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_nodes_kind CHECK (Kind IN (
    'table', 'col', 'pred', 'tmpl', 'metric', 'mgroup', 'concept', 'term',
    'acr', 'skill', 'schema', 'domain', 'doc', 'lob', 'mdom', 'status',
    'run', 'policy', 'owner', 'review')),
) PRIMARY KEY (NodeId);

CREATE INDEX GraphNodesByKind ON GraphNodes (Kind, NodeId);
CREATE SEARCH INDEX GraphNodesLabel ON GraphNodes (Label_Tokens);

-- ── edges ────────────────────────────────────────────────────
-- Identity is (s, r, o, witness): one quad per witness family, so
-- independent testimony never collapses at the store (E12/A1).
CREATE TABLE GraphEdgeAssertions (
  SubjectId   STRING(300) NOT NULL,
  Relation    STRING(32)  NOT NULL,
  ObjectId    STRING(300) NOT NULL,
  Witness     STRING(24)  NOT NULL,
  Seq         INT64       NOT NULL,          -- per (s, r, o, witness)
  Props       JSON,
  Source      STRING(48)  NOT NULL,
  RunId       STRING(64)  NOT NULL,
  Status      STRING(16)  NOT NULL DEFAULT ('active'),
  Support     INT64,
  Evidence    STRING(1000),
  Actor       STRING(36),
  Retrieved   STRING(64),
  ValidFor    ARRAY<STRING(64)> NOT NULL DEFAULT (ARRAY<STRING>[]),
  AssertedAt  TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_edgeassert_status CHECK (Status IN ('active', 'superseded', 'retracted')),
  CONSTRAINT fk_edgeassert_run FOREIGN KEY (RunId) REFERENCES GraphRuns (RunId),
) PRIMARY KEY (SubjectId, Relation, ObjectId, Witness, Seq);

CREATE INDEX GraphEdgeAssertionsByRun ON GraphEdgeAssertions (RunId, Relation);

-- The fold. EdgeId is the concatenation the property graph keys on.
CREATE TABLE GraphEdges (
  EdgeId      STRING(1000) NOT NULL AS (
    CONCAT(SubjectId, '|', Relation, '|', ObjectId, '|', Witness)) STORED,
  SubjectId   STRING(300) NOT NULL,
  Relation    STRING(32)  NOT NULL,
  ObjectId    STRING(300) NOT NULL,
  Witness     STRING(24)  NOT NULL,
  Props       JSON,
  Source      STRING(48)  NOT NULL,
  RunId       STRING(64)  NOT NULL,
  Status      STRING(16)  NOT NULL DEFAULT ('active'),
  Support     INT64,
  Seq         INT64       NOT NULL,
  UpdatedAt   TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_edges_relation CHECK (Relation IN (
    'has_column', 'has_schema', 'bound_to', 'defines_metric', 'measured_on',
    'variant_of', 'mapped_term', 'alias_of', 'joins_via', 'co_queried_with',
    'derived_from', 'upstream_of', 'owned_by', 'certified_as', 'has_policy',
    'has_domain', 'evidenced_by', 'valid_in', 'member_of', 'described_by',
    'concerns', 'in_lob', 'in_domain', 'used_by', 'fk_references', 'kc_pushed')),
  CONSTRAINT ck_edges_witness CHECK (Witness IN (
    'catalog_mined', 'jobs_30d', 'audit_30d', 'dmp', 'gmns', 'skill_contract',
    'snippet', 'atlas', 'lumi', 'bq', 'steward', 'user_variant',
    'llm_enriched', 'gold_attested', 'studio', 'kc')),
  CONSTRAINT fk_edges_subject FOREIGN KEY (SubjectId) REFERENCES GraphNodes (NodeId),
  CONSTRAINT fk_edges_object FOREIGN KEY (ObjectId) REFERENCES GraphNodes (NodeId),
) PRIMARY KEY (SubjectId, Relation, ObjectId, Witness);

CREATE UNIQUE INDEX GraphEdgesById ON GraphEdges (EdgeId);
CREATE INDEX GraphEdgesByObject ON GraphEdges (ObjectId, Relation, SubjectId);
CREATE INDEX GraphEdgesByRelation ON GraphEdges (Relation, SubjectId);

-- ── the identity layer and the clerk's decisions ─────────────
-- The crosswalk (graph/identity/crosswalk.jsonl): a source's name
-- for a thing → the graph's id, with how sure and from where.
CREATE TABLE GraphCrosswalk (
  Source      STRING(48)  NOT NULL,
  SourceRef   STRING(500) NOT NULL,
  NodeId      STRING(300) NOT NULL,
  Confidence  FLOAT64,
  RunId       STRING(64)  NOT NULL,
  AssertedAt  TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY (Source, SourceRef);

CREATE INDEX GraphCrosswalkByNode ON GraphCrosswalk (NodeId);

-- Status transitions (sahs/graph/clerk.py): every move a metric or a
-- predicate makes through mined → team_candidate → pending →
-- certified → deprecated, with the actor and the reason. Append-only;
-- the current status is the certified_as edge in the fold.
CREATE TABLE GraphStatusTransitions (
  NodeId      STRING(300) NOT NULL,
  Seq         INT64       NOT NULL,
  FromStatus  STRING(16)  NOT NULL,
  ToStatus    STRING(16)  NOT NULL,
  ActorUserId STRING(36)  NOT NULL,
  Reason      STRING(1000),
  RunId       STRING(64),
  OccurredAt  TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  CONSTRAINT ck_transitions_to CHECK (ToStatus IN (
    'mined', 'team_candidate', 'pending', 'certified', 'rejected',
    'deprecated', 'retracted')),
) PRIMARY KEY (NodeId, Seq);

-- ── the property graph over the fold ─────────────────────────
-- One node table, one edge table: the label is the node's Kind or the
-- edge's Relation (dynamic labels), the properties are the JSON the
-- record carries (dynamic properties) plus every typed column. Where
-- the target Spanner lacks dynamic labels, the static form is the
-- same two tables with LABEL Node / LABEL Edge and the kind or the
-- relation read as a property (see the README).
-- GQL then reads the way the compiler thinks:
--   GRAPH SynapseGraph
--   MATCH (t:table {NodeId: 'table:dw.gms_transaction'})-[:has_column]->(c:col)
--   RETURN c.NodeId, c.Props.data_type
--   GRAPH SynapseGraph
--   MATCH (m:metric)-[e:measured_on]->(t:table)
--   WHERE e.Status = 'active' AND e.Witness IN ('jobs_30d', 'studio')
--   RETURN m.Label, t.NodeId, e.Support
CREATE PROPERTY GRAPH SynapseGraph
  NODE TABLES (
    GraphNodes
      KEY (NodeId)
      DYNAMIC LABEL (Kind)
      DYNAMIC PROPERTIES (Props)
  )
  EDGE TABLES (
    GraphEdges
      KEY (SubjectId, Relation, ObjectId, Witness)
      SOURCE KEY (SubjectId) REFERENCES GraphNodes (NodeId)
      DESTINATION KEY (ObjectId) REFERENCES GraphNodes (NodeId)
      DYNAMIC LABEL (Relation)
      DYNAMIC PROPERTIES (Props)
  );

-- ── the compiled build's serving indexes, kept beside the graph ──
-- A build is a snapshot of the fold rendered for serving (cards,
-- indexes). Its manifest lands here so "which build served this
-- answer" resolves; the cards stay files on the serving host.
CREATE TABLE Builds (
  BuildId     STRING(64)  NOT NULL,
  RunId       STRING(64),
  Manifest    JSON        NOT NULL,
  Promoted    BOOL        NOT NULL DEFAULT (false),
  BuiltAt     TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true),
  PromotedAt  TIMESTAMP,
  PromotedBy  STRING(36),
) PRIMARY KEY (BuildId);

CREATE NULL_FILTERED INDEX BuildsPromoted ON Builds (Promoted, PromotedAt DESC);
