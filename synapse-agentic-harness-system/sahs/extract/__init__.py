"""BigQuery warehouse extraction — the one-time, resumable pull that
produces the ``real_extractions_production/`` archive the truth graph
loads (``sahs.loaders.archives.bq_extraction``).

Modules:

    config     tables.yaml → RunConfig / TableSpec (three identities per
               table: billing project, logical dataset, physical dataset)
    bq_rest    REST client on the proven BQConnection contract — jobs,
               dry runs, tables.get, row policies; retries; the byte ledger
    bq_sql     every SQL statement, parameterized (never string-spliced)
    history    the local indexer that routes 30 days of job/audit history
               into each table's 17_queries_30d/ digests
    warehouse  the orchestrator: phases, checkpoint state, writers, reports
"""
