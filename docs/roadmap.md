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
