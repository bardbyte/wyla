"""Realistic table-schema definitions for synthetic data generation.

Each SyntheticTable carries enough to drive every downstream artifact
(MDM digest, BQ profile, baseline LookML, plausible SQL queries):
    - name, FQN, domain tags
    - columns with type / nullability / PII / description
    - primary key + foreign key declarations
    - common business synonyms
    - sample distinct values for low-cardinality columns
    - expected metric formulas computed against this table
    - typical filter patterns observed in production

The 20+ tables here represent the real corpus surfaces: cardmember
fact, product dim, transaction events, risk profiles, loyalty ledger,
acquisitions, merchant hierarchy. Domain spread chosen to exercise
every entity type we expect the graph to resolve.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


PIITaxonomy = Literal[
    "Public",
    "Internal",
    "Internal>BusinessKey",
    "Sensitive",
    "Sensitive>Identifier",
    "Sensitive>Identifier>MemberID",
    "Sensitive>Identifier>AccountNumber",
    "Sensitive>Identifier>SSN",
    "Sensitive>Identifier>Email",
    "Sensitive>FinancialAmount",
    "Sensitive>Geographic",
    "Restricted",
    "Restricted>FullPAN",
]


@dataclass
class SyntheticColumn:
    name: str
    data_type: Literal["STRING", "INT64", "NUMERIC", "FLOAT64", "DATE", "TIMESTAMP", "BOOL"]
    nullable: bool = True
    description: str = ""
    business_name: str = ""
    is_primary: bool = False
    is_dedupe_key: bool = False
    is_partitioning: bool = False
    cluster_position: int | None = None
    pii_taxonomy: PIITaxonomy = "Internal"
    sample_distinct: list[str] = field(default_factory=list)
    is_coded: bool = False  # True if values are opaque codes (cm11, 005, etc.)


@dataclass
class SyntheticFK:
    from_column: str
    to_table: str
    to_column: str
    cardinality: Literal["many_to_one", "one_to_one", "many_to_many"] = "many_to_one"


@dataclass
class SyntheticMetric:
    technical_name: str
    business_name: str
    formula: str            # SQL fragment computed against this table
    grain: Literal["row", "account", "aggregated", "partition"] = "aggregated"
    domain: str = "Finance"
    synonyms: list[str] = field(default_factory=list)


@dataclass
class SyntheticTable:
    name: str
    is_in_dmp: bool
    company_domain: str
    data_domain: str
    business_name: str
    description: str
    table_type: Literal["BASE TABLE", "VIEW"] = "BASE TABLE"
    columns: list[SyntheticColumn] = field(default_factory=list)
    foreign_keys: list[SyntheticFK] = field(default_factory=list)
    metrics: list[SyntheticMetric] = field(default_factory=list)
    typical_filters: list[tuple[str, str]] = field(default_factory=list)  # (column, value)
    row_count: int = 1_000_000
    owner_team: str = "data-platform@example.com"
    # Dataplex Catalog-style fields
    asset_kind: Literal[
        "Table", "View", "MaterializedView", "ExternalTable", "BIDashboard"
    ] = "Table"
    tags: list[str] = field(default_factory=list)
    lineage_upstream: list[str] = field(default_factory=list)  # tables this one is derived from


# ─── Domain entity columns reused across tables ─────────────


def _cm11() -> SyntheticColumn:
    return SyntheticColumn(
        name="cm11", data_type="STRING", nullable=False,
        description="Cardmember 11-digit identifier (canonical ID for a card member).",
        business_name="Cardmember ID",
        pii_taxonomy="Sensitive>Identifier>MemberID",
        is_coded=True,
    )


def _acct_id() -> SyntheticColumn:
    return SyntheticColumn(
        name="acct_id", data_type="STRING", nullable=False,
        description="Account identifier; multiple accounts can roll up to one cardmember.",
        business_name="Account ID",
        pii_taxonomy="Sensitive>Identifier>AccountNumber",
        is_coded=True,
    )


def _rpt_dt(partition: bool = True) -> SyntheticColumn:
    return SyntheticColumn(
        name="rpt_dt", data_type="DATE", nullable=False,
        description="Reporting snapshot date — the grain at which the row was captured.",
        business_name="Report Date",
        is_partitioning=partition,
        sample_distinct=["2026-05-30", "2026-05-29", "2026-05-28"],
    )


def _bus_seg(cluster: int | None = 1) -> SyntheticColumn:
    return SyntheticColumn(
        name="bus_seg", data_type="STRING", nullable=False,
        description="Business segment classification (Consumer, Small Business, Corporate, etc.).",
        business_name="Business Segment",
        cluster_position=cluster,
        sample_distinct=["Consumer", "Small Business", "Corporate", "Centurion", "Platinum"],
        is_coded=False,
    )


def _data_source(cluster: int | None = 2) -> SyntheticColumn:
    return SyntheticColumn(
        name="data_source", data_type="STRING", nullable=False,
        description="Upstream data source; structural filter in 90%+ of queries.",
        business_name="Data Source",
        cluster_position=cluster,
        sample_distinct=["cornerstone", "legacy", "acquired"],
        is_coded=False,
    )


# ─── The synthetic table catalog ─────────────────────────────


def _build_synthetic_tables() -> list[SyntheticTable]:
    tables: list[SyntheticTable] = []

    # ── Cardmember domain ──
    tables.append(SyntheticTable(
        name="custins_customer_insights_cardmember",
        is_in_dmp=True,
        company_domain="Finance",
        data_domain="CP&A",
        business_name="Customer Insights — Cardmember Daily",
        description=(
            "Cardmember-level daily snapshot fact carrying billed business, "
            "active flags, balances, and segment classification. The most "
            "queried table in the corpus — root of nearly every Finance "
            "aggregation."
        ),
        row_count=845_230_197,
        owner_team="cardmember-insights@example.com",
        columns=[
            _cm11(),
            _rpt_dt(),
            _bus_seg(),
            _data_source(),
            SyntheticColumn(
                name="card_product_id", data_type="STRING", nullable=False,
                description="Card product code (005=Platinum, 006=Gold, 010=Centurion, ...).",
                business_name="Card Product ID",
                sample_distinct=["005", "006", "010", "015", "020"],
                is_coded=True,
            ),
            SyntheticColumn(
                name="billed_business", data_type="NUMERIC", nullable=True,
                description="Dollar amount billed to this cardmember on this date.",
                business_name="Billed Business",
                pii_taxonomy="Sensitive>FinancialAmount",
            ),
            SyntheticColumn(
                name="accounts_in_force", data_type="INT64", nullable=True,
                description="Count of active accounts the cardmember holds as of rpt_dt.",
                business_name="Accounts In Force",
            ),
            SyntheticColumn(
                name="fico", data_type="INT64", nullable=True,
                description="FICO credit score (300-850).",
                business_name="FICO Score",
                pii_taxonomy="Sensitive>FinancialAmount",
            ),
            SyntheticColumn(
                name="generation", data_type="STRING", nullable=True,
                description="Generational cohort (Gen Z, Millennial, Gen X, Boomer, Silent).",
                business_name="Generation",
                sample_distinct=["Gen Z", "Millennial", "Gen X", "Boomer", "Silent"],
            ),
        ],
        foreign_keys=[
            SyntheticFK("card_product_id", "drm_product_hier", "card_product_id"),
            SyntheticFK("cm11", "fin_consumer_business_card_member_status", "cm11", "one_to_one"),
        ],
        metrics=[
            SyntheticMetric(
                technical_name="total_billed_business",
                business_name="Total Billed Business",
                formula="SUM(billed_business)",
                grain="aggregated",
                domain="Finance",
                synonyms=["TBB", "Total BB", "Billed Business"],
            ),
            SyntheticMetric(
                technical_name="active_cardmembers",
                business_name="Active Cardmembers",
                formula="COUNT(DISTINCT cm11) FILTER (WHERE accounts_in_force > 0)",
                grain="aggregated",
                domain="Cardmember",
                synonyms=["Active CMs", "Engaged Cardmembers"],
            ),
            SyntheticMetric(
                technical_name="fico_band",
                business_name="Credit Score Band",
                formula=(
                    "CASE WHEN fico >= 740 THEN 'Prime' "
                    "WHEN fico >= 670 THEN 'Near-Prime' "
                    "ELSE 'Sub' END"
                ),
                grain="row",
                domain="Risk",
                synonyms=["FICO Tier", "Credit Tier"],
            ),
        ],
        typical_filters=[
            ("data_source", "cornerstone"),
            ("bus_seg", "Consumer"),
        ],
    ))

    tables.append(SyntheticTable(
        name="fin_consumer_business_card_member_status",
        is_in_dmp=True,
        company_domain="Finance",
        data_domain="FODL",
        business_name="Cardmember Status — Consumer + Business",
        description="Per-cardmember status flags (active, suspended, churned).",
        row_count=98_000_000,
        owner_team="cardmember-insights@example.com",
        columns=[
            _cm11(),
            _rpt_dt(),
            SyntheticColumn(
                name="cm_status", data_type="STRING", nullable=False,
                description="Cardmember-level status code (A=Active, S=Suspended, C=Churned, P=Prospect).",
                business_name="Cardmember Status",
                sample_distinct=["A", "S", "C", "P"],
                is_coded=True,
            ),
            SyntheticColumn(
                name="basic_supp_in", data_type="STRING", nullable=True,
                description="Basic vs supplementary card indicator (B=Basic, S=Supp).",
                business_name="Basic/Supp Indicator",
                sample_distinct=["B", "S"],
                is_coded=True,
            ),
        ],
        foreign_keys=[
            SyntheticFK("cm11", "custins_customer_insights_cardmember", "cm11", "one_to_one"),
        ],
        metrics=[
            SyntheticMetric(
                technical_name="active_cardmember_count",
                business_name="Active Cardmember Count",
                formula="COUNT(DISTINCT cm11) FILTER (WHERE cm_status = 'A')",
                domain="Cardmember",
                synonyms=["Active CMs"],
            ),
        ],
        typical_filters=[("cm_status", "A")],
    ))

    # ── Product domain ──
    tables.append(SyntheticTable(
        name="drm_product_hier",
        is_in_dmp=True,
        company_domain="Cardmember",
        data_domain="Product Reference",
        business_name="Product Hierarchy Dimension",
        description="Code → name lookup for card products. Tiny but critical for code resolution.",
        row_count=47,
        owner_team="product-mgmt@example.com",
        columns=[
            SyntheticColumn(
                name="card_product_id", data_type="STRING", nullable=False,
                description="Card product code (PK).",
                business_name="Card Product ID",
                is_primary=True,
                sample_distinct=["005", "006", "010", "015", "020", "025", "030"],
                is_coded=True,
            ),
            SyntheticColumn(
                name="card_product_name", data_type="STRING", nullable=False,
                description="Human-readable product name.",
                business_name="Card Product Name",
                sample_distinct=["Platinum", "Gold", "Centurion", "Green", "Business Platinum"],
            ),
            SyntheticColumn(
                name="product_tier", data_type="STRING", nullable=False,
                description="Marketing tier (Premium, Standard, Entry).",
                business_name="Product Tier",
                sample_distinct=["Premium", "Standard", "Entry"],
            ),
            SyntheticColumn(
                name="launch_date", data_type="DATE", nullable=False,
                description="Product launch date.",
                business_name="Launch Date",
            ),
        ],
    ))

    tables.append(SyntheticTable(
        name="custins_customer_insights_product",
        is_in_dmp=True,
        company_domain="Finance",
        data_domain="CP&A",
        business_name="Customer Insights — Product Rollup",
        description="Aggregated product-level metrics; rolls up cardmember table to product grain.",
        row_count=11_500_000,
        owner_team="cardmember-insights@example.com",
        columns=[
            SyntheticColumn(
                name="card_product_id", data_type="STRING", nullable=False,
                description="Card product code.",
                business_name="Card Product ID",
                is_coded=True,
            ),
            _rpt_dt(),
            _bus_seg(),
            _data_source(),
            SyntheticColumn(
                name="total_billed_business", data_type="NUMERIC", nullable=True,
                description="Rolled-up billed business for this product on rpt_dt.",
                business_name="Total Billed Business (product rollup)",
                pii_taxonomy="Sensitive>FinancialAmount",
            ),
            SyntheticColumn(
                name="active_cm_count", data_type="INT64", nullable=True,
                description="Count of active cardmembers holding this product.",
                business_name="Active Cardmember Count",
            ),
        ],
        foreign_keys=[
            SyntheticFK("card_product_id", "drm_product_hier", "card_product_id"),
        ],
        metrics=[
            SyntheticMetric(
                technical_name="product_billed_business",
                business_name="Total Billed Business by Product",
                formula="SUM(total_billed_business)",
                domain="Finance",
                synonyms=["Product TBB"],
            ),
        ],
    ))

    # ── Transaction domain ──
    tables.append(SyntheticTable(
        name="pmdl_fin_business_volume_transaction_detail",
        is_in_dmp=True,
        company_domain="Finance",
        data_domain="FODL",
        business_name="Transaction Detail — Business Volume",
        description="Per-transaction event-level table; ~100B rows. Heavily partitioned + clustered.",
        row_count=99_000_000_000,
        owner_team="transaction-platform@example.com",
        columns=[
            SyntheticColumn(
                name="txn_id", data_type="STRING", nullable=False,
                description="Globally-unique transaction identifier (PK).",
                business_name="Transaction ID",
                is_primary=True, is_dedupe_key=True,
                pii_taxonomy="Internal>BusinessKey",
                is_coded=True,
            ),
            _cm11(),
            _acct_id(),
            SyntheticColumn(
                name="merchant_id", data_type="STRING", nullable=False,
                description="Merchant identifier — joins to merchant hierarchy.",
                business_name="Merchant ID",
                pii_taxonomy="Internal>BusinessKey",
                is_coded=True,
            ),
            _rpt_dt(),
            SyntheticColumn(
                name="trans_dt", data_type="DATE", nullable=False,
                description="Transaction date (may lag rpt_dt by 1-2 days).",
                business_name="Transaction Date",
                cluster_position=1,
            ),
            SyntheticColumn(
                name="trans_amount", data_type="NUMERIC", nullable=False,
                description="Transaction dollar amount.",
                business_name="Transaction Amount",
                pii_taxonomy="Sensitive>FinancialAmount",
            ),
            SyntheticColumn(
                name="mcc_code", data_type="STRING", nullable=True,
                description="Merchant Category Code (4-digit ISO 18245).",
                business_name="MCC Code",
                sample_distinct=["5411", "5812", "5912", "4111", "5734"],
                is_coded=True,
            ),
            SyntheticColumn(
                name="card_product_id", data_type="STRING", nullable=False,
                description="Card product used.",
                business_name="Card Product ID",
                is_coded=True,
            ),
        ],
        foreign_keys=[
            SyntheticFK("cm11", "custins_customer_insights_cardmember", "cm11"),
            SyntheticFK("merchant_id", "gms_merchant_full_hier", "merchant_id"),
            SyntheticFK("card_product_id", "drm_product_hier", "card_product_id"),
        ],
        metrics=[
            SyntheticMetric(
                technical_name="total_transaction_volume",
                business_name="Total Transaction Volume",
                formula="SUM(trans_amount)",
                domain="Finance",
                synonyms=["Txn Volume", "TXN VOL"],
            ),
            SyntheticMetric(
                technical_name="transaction_count",
                business_name="Transaction Count",
                formula="COUNT(*)",
                domain="Finance",
                synonyms=["Txn Count", "TC"],
            ),
        ],
        typical_filters=[("trans_dt", "last 90 days")],
    ))

    tables.append(SyntheticTable(
        name="pmdl_fin_match",
        is_in_dmp=True,
        company_domain="Finance",
        data_domain="FODL",
        business_name="Financial Match — Reconciliation",
        description="Reconciliation table matching billed vs settled transactions.",
        row_count=14_000_000_000,
        columns=[
            SyntheticColumn(
                name="txn_id", data_type="STRING", nullable=False,
                description="Transaction ID being matched.",
                business_name="Transaction ID", is_dedupe_key=True,
                is_coded=True,
            ),
            _rpt_dt(),
            _cm11(),
            SyntheticColumn(
                name="match_status", data_type="STRING", nullable=False,
                description="Reconciliation status (M=Matched, U=Unmatched, D=Disputed).",
                business_name="Match Status",
                sample_distinct=["M", "U", "D"],
                is_coded=True,
            ),
        ],
        foreign_keys=[
            SyntheticFK("txn_id", "pmdl_fin_business_volume_transaction_detail", "txn_id", "one_to_one"),
        ],
    ))

    # ── Merchant domain ──
    tables.append(SyntheticTable(
        name="gms_merchant_full_hier",
        is_in_dmp=True,
        company_domain="Merchant",
        data_domain="MERCHANT",
        business_name="Merchant Full Hierarchy",
        description="Merchant master with industry hierarchy (chain → location → terminal).",
        row_count=6_500_000,
        owner_team="merchant-platform@example.com",
        columns=[
            SyntheticColumn(
                name="merchant_id", data_type="STRING", nullable=False,
                description="Merchant identifier (PK).",
                business_name="Merchant ID",
                is_primary=True,
                is_coded=True,
            ),
            SyntheticColumn(
                name="merchant_name", data_type="STRING", nullable=False,
                description="Doing-business-as name.",
                business_name="Merchant Name",
            ),
            SyntheticColumn(
                name="industry_code", data_type="STRING", nullable=False,
                description="High-level industry segment (RETAIL, RESTAURANT, TRAVEL, ...).",
                business_name="Industry",
                sample_distinct=["RETAIL", "RESTAURANT", "TRAVEL", "GAS", "ENTERTAINMENT"],
            ),
            SyntheticColumn(
                name="mcc_code", data_type="STRING", nullable=True,
                description="Merchant category code (4-digit ISO 18245).",
                business_name="MCC Code",
                is_coded=True,
            ),
            SyntheticColumn(
                name="chain_id", data_type="STRING", nullable=True,
                description="Parent chain identifier (e.g., all Starbucks share a chain_id).",
                business_name="Chain ID",
                is_coded=True,
            ),
        ],
    ))

    # ── Risk domain ──
    tables.append(SyntheticTable(
        name="risk_indv_cust_hist",
        is_in_dmp=True,
        company_domain="Risk",
        data_domain="Scoring, Decisioning & Eligibility",
        business_name="Risk — Individual Customer History",
        description="Per-cardmember risk history with credit scores + delinquency.",
        row_count=420_000_000,
        owner_team="risk-modeling@example.com",
        columns=[
            _cm11(),
            SyntheticColumn(
                name="as_of_dt", data_type="DATE", nullable=False,
                description="Snapshot date (synonym of rpt_dt in this table).",
                business_name="As Of Date",
                is_partitioning=True,
            ),
            SyntheticColumn(
                name="fico", data_type="INT64", nullable=True,
                description="FICO score on as_of_dt.",
                business_name="FICO Score",
                pii_taxonomy="Sensitive>FinancialAmount",
            ),
            SyntheticColumn(
                name="bal_age_cd", data_type="STRING", nullable=True,
                description="Balance aging bucket code (0=current, 1=30d, 2=60d, 3=90d+).",
                business_name="Balance Age Bucket",
                sample_distinct=["0", "1", "2", "3"],
                is_coded=True,
            ),
            SyntheticColumn(
                name="cust_xref_id", data_type="STRING", nullable=False,
                description="Internal cross-reference identifier.",
                business_name="Customer Cross-Ref ID",
                pii_taxonomy="Sensitive>Identifier>MemberID",
                is_coded=True,
            ),
        ],
        foreign_keys=[
            SyntheticFK("cm11", "custins_customer_insights_cardmember", "cm11"),
        ],
    ))

    tables.append(SyntheticTable(
        name="risk_pers_acct_history",
        is_in_dmp=True,
        company_domain="Risk",
        data_domain="Scoring, Decisioning & Eligibility",
        business_name="Risk — Personal Account History",
        description="Account-level monthly history with balances + spend + writeoffs.",
        row_count=11_000_000_000,
        owner_team="risk-modeling@example.com",
        columns=[
            _acct_id(),
            _cm11(),
            SyntheticColumn(
                name="acct_as_of_dt", data_type="DATE", nullable=False,
                description="Account snapshot date.",
                business_name="Account As-Of Date",
                is_partitioning=True,
            ),
            SyntheticColumn(
                name="acct_bill_bal_mth01_amt", data_type="NUMERIC", nullable=True,
                description="Account billed balance at month-end.",
                business_name="Account Bill Balance (M0)",
                pii_taxonomy="Sensitive>FinancialAmount",
            ),
            SyntheticColumn(
                name="acct_spend_mth01_amt", data_type="NUMERIC", nullable=True,
                description="Account spend in month 0.",
                business_name="Account Spend (M0)",
                pii_taxonomy="Sensitive>FinancialAmount",
            ),
            SyntheticColumn(
                name="acct_wrt_off_am", data_type="NUMERIC", nullable=True,
                description="Cumulative write-off amount on this account.",
                business_name="Write-Off Amount",
                pii_taxonomy="Sensitive>FinancialAmount",
            ),
        ],
        foreign_keys=[
            SyntheticFK("cm11", "custins_customer_insights_cardmember", "cm11"),
        ],
    ))

    # ── Acquisitions ──
    tables.append(SyntheticTable(
        name="acqdw_acquisition_us",
        is_in_dmp=True,
        company_domain="Acquisitions Tracking",
        data_domain="Acquisitions Tracking",
        business_name="Acquisitions — US",
        description="New-account acquisition events with decision + channel.",
        row_count=180_000_000,
        owner_team="acquisitions@example.com",
        columns=[
            _cm11(),
            SyntheticColumn(
                name="dcsn_dt", data_type="DATE", nullable=False,
                description="Decision date (approved/declined).",
                business_name="Decision Date",
                is_partitioning=True,
            ),
            SyntheticColumn(
                name="dcsn_cd", data_type="STRING", nullable=False,
                description="Decision code (A=Approved, D=Declined, P=Pending).",
                business_name="Decision Code",
                sample_distinct=["A", "D", "P"],
                is_coded=True,
            ),
            SyntheticColumn(
                name="card_type", data_type="STRING", nullable=False,
                description="Card type code (S=Standard, B=Business).",
                business_name="Card Type",
                sample_distinct=["S", "B"],
                is_coded=True,
            ),
            SyntheticColumn(
                name="acq_type", data_type="STRING", nullable=False,
                description="Acquisition channel (DM, EM, BRN, CC, ONL — Direct Mail, Email, Branch, Call Center, Online).",
                business_name="Acquisition Type",
                sample_distinct=["DM", "EM", "BRN", "CC", "ONL"],
                is_coded=True,
            ),
        ],
        metrics=[
            SyntheticMetric(
                technical_name="new_accounts_acquired",
                business_name="New Accounts Acquired",
                formula="COUNT(*) FILTER (WHERE dcsn_cd = 'A')",
                domain="Acquisitions",
                synonyms=["NAA", "New Acct Acq", "Approvals"],
            ),
        ],
    ))

    # ── Loyalty ──
    tables.append(SyntheticTable(
        name="loyalty_rc_redemption",
        is_in_dmp=True,
        company_domain="Loyalty",
        data_domain="LOYALTY AND BENEFITS",
        business_name="Loyalty — Reward Redemption",
        description="Per-redemption event for membership-rewards points.",
        row_count=620_000_000,
        owner_team="loyalty@example.com",
        columns=[
            _cm11(),
            _rpt_dt(),
            SyntheticColumn(
                name="redemption_id", data_type="STRING", nullable=False,
                description="Unique redemption event ID.",
                business_name="Redemption ID",
                is_primary=True,
                is_coded=True,
            ),
            SyntheticColumn(
                name="points_redeemed", data_type="INT64", nullable=False,
                description="Number of MR points redeemed in this event.",
                business_name="Points Redeemed",
            ),
            SyntheticColumn(
                name="redemption_type", data_type="STRING", nullable=False,
                description="Reward category (TRV, GFT, STA, CSH — Travel, Gift Card, Statement Credit, Cash).",
                business_name="Redemption Type",
                sample_distinct=["TRV", "GFT", "STA", "CSH"],
                is_coded=True,
            ),
        ],
        foreign_keys=[
            SyntheticFK("cm11", "custins_customer_insights_cardmember", "cm11"),
        ],
    ))

    tables.append(SyntheticTable(
        name="r42_loyalty_ledger_event",
        is_in_dmp=True,
        company_domain="Loyalty",
        data_domain="Loyalty",
        business_name="Loyalty — Points Ledger",
        description="Append-only ledger of all points-earning/redemption events.",
        row_count=4_800_000_000,
        owner_team="loyalty@example.com",
        columns=[
            _cm11(),
            SyntheticColumn(
                name="event_ts", data_type="TIMESTAMP", nullable=False,
                description="Event timestamp (UTC).",
                business_name="Event Timestamp",
                is_partitioning=True,
            ),
            SyntheticColumn(
                name="event_type", data_type="STRING", nullable=False,
                description="EARN, REDEEM, EXPIRE, ADJUST, BONUS.",
                business_name="Event Type",
                sample_distinct=["EARN", "REDEEM", "EXPIRE", "ADJUST", "BONUS"],
                is_coded=False,
            ),
            SyntheticColumn(
                name="point_amount", data_type="INT64", nullable=False,
                description="Signed point delta (positive=earn, negative=redeem).",
                business_name="Point Amount",
            ),
        ],
    ))

    # ── Travel ──
    tables.append(SyntheticTable(
        name="tlsarpt_travel_sales",
        is_in_dmp=True,
        company_domain="Travel",
        data_domain="TRAVEL",
        business_name="Travel Sales Report",
        description="Travel booking events made through Amex Travel.",
        row_count=92_000_000,
        owner_team="travel-platform@example.com",
        columns=[
            _cm11(),
            _rpt_dt(),
            SyntheticColumn(
                name="trip_id", data_type="STRING", nullable=False,
                description="Unique trip identifier.",
                business_name="Trip ID",
                is_primary=True, is_dedupe_key=True,
                is_coded=True,
            ),
            SyntheticColumn(
                name="trip_type", data_type="STRING", nullable=False,
                description="Travel category (AIR, HOTEL, CAR, CRUISE, PKG).",
                business_name="Trip Type",
                sample_distinct=["AIR", "HOTEL", "CAR", "CRUISE", "PKG"],
                is_coded=True,
            ),
            SyntheticColumn(
                name="trans_usd_am", data_type="NUMERIC", nullable=False,
                description="Total trip cost in USD.",
                business_name="Transaction USD Amount",
                pii_taxonomy="Sensitive>FinancialAmount",
            ),
            SyntheticColumn(
                name="vend_nm", data_type="STRING", nullable=True,
                description="Travel vendor name (airline, hotel chain, etc.).",
                business_name="Vendor Name",
            ),
        ],
    ))

    return tables


# Domain-derived lineage hints (Dataplex Catalog-style upstream graph).
# Maps a table to the set of upstream sources it conceptually derives from.
# Picked from realistic AmEx warehouse layering: fact tables roll up from
# raw transaction + dimension tables.
_LINEAGE_HINTS: dict[str, list[str]] = {
    "custins_customer_insights_cardmember": [
        "pmdl_fin_business_volume_transaction_detail",
        "fin_consumer_business_card_member_status",
        "risk_indv_cust_hist",
    ],
    "custins_customer_insights_product": [
        "drm_product_hier",
        "pmdl_fin_business_volume_transaction_detail",
    ],
    "fin_consumer_business_card_member_status": [
        "risk_pers_acct_history",
    ],
    "pmdl_fin_match": [
        "pmdl_fin_business_volume_transaction_detail",
    ],
    "loyalty_rc_redemption": [
        "r42_loyalty_ledger_event",
    ],
    "risk_indv_cust_hist": [
        "risk_pers_acct_history",
    ],
}

_ASSET_KIND_HINTS: dict[str, str] = {
    # most are base tables; a couple are derived views in real practice
    "custins_customer_insights_cardmember": "MaterializedView",
    "custins_customer_insights_product": "MaterializedView",
    "pmdl_fin_match": "View",
}

_TAG_HINTS: dict[str, list[str]] = {
    "custins_customer_insights_cardmember": ["cornerstone", "pii", "daily-refresh", "tier-1"],
    "custins_customer_insights_product": ["cornerstone", "daily-refresh"],
    "drm_product_hier": ["dim", "stable", "lookup"],
    "gms_merchant_full_hier": ["dim", "stable", "lookup"],
    "pmdl_fin_business_volume_transaction_detail": ["fact", "high-volume", "pii", "tier-1"],
    "pmdl_fin_match": ["derived", "intermediate"],
    "risk_indv_cust_hist": ["risk", "scd-type-2", "pii"],
    "risk_pers_acct_history": ["risk", "scd-type-2", "pii"],
    "acqdw_acquisition_us": ["acquisitions", "us-only"],
    "loyalty_rc_redemption": ["loyalty", "event-stream"],
    "r42_loyalty_ledger_event": ["loyalty", "event-stream", "tier-2"],
    "tlsarpt_travel_sales": ["travel", "pii"],
    "fin_consumer_business_card_member_status": ["status", "scd-type-2"],
}


def _apply_catalog_enrichments(tables: list[SyntheticTable]) -> list[SyntheticTable]:
    for t in tables:
        t.lineage_upstream = _LINEAGE_HINTS.get(t.name, [])
        t.asset_kind = _ASSET_KIND_HINTS.get(t.name, "Table")  # type: ignore[assignment]
        t.tags = _TAG_HINTS.get(t.name, [])
    return tables


# Module-level export
SYNTHETIC_TABLES: list[SyntheticTable] = _apply_catalog_enrichments(
    _build_synthetic_tables(),
)
