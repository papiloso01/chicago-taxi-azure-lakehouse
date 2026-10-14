# Implementation roadmap

1. Foundation merged and live ADF definitions imported.
2. Taxi extraction and ADF orchestration implemented; review this PR, create the ingestion job and perform live acceptance. Weather/community ingestion remains pending.
3. Implement bronze source-key MERGE and audit/watermark behaviour.
4. Implement silver schemas, quarantine, facts/dimensions and contract checks.
5. Implement gold marts and count/revenue reconciliation.
6. Add functional, integration, ETL validation and E2E suites as separate CI jobs.
7. Implement local MySQL publication and failure recovery; validate cloud provisioning separately.
8. Add Key Vault, monitored schedules, optional advisory AI PR review and reviewed infra automation.
9. Build Power BI/Tableau outputs; perform real cloud acceptance and backfill benchmarks.

Use PRs for review before merge. Do not claim future pipeline stages or MySQL/BI deployment as
complete from the connection smoke tests.

## Current acceptance

- One-day notebook and ADF landing runs succeeded (9,285 rows in the notebook run).
- Bronze loader implemented; live Delta merge/replay acceptance is pending.
- Next: bronze acceptance, saved job and ADF handoff, then silver cleaning/facts/dimensions.

- Bronze one-day load and same-run replay were reported successful (9,285 rows); saved bronze job also passed through ADF.
- Combined ADF landing-to-bronze pipeline implemented; live acceptance remains pending.

- Combined ADF landing→bronze was reported successful, saved and published.
- Silver snapshot warehouse implemented; test in Databricks before connecting its saved job to ADF.
- Remaining: community/weather enrichment, gold marts/BI, late-arrival policy and daily scheduling.
