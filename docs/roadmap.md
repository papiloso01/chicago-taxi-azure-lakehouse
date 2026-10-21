# Implementation roadmap

## Reported live acceptance

- ADF Git integration, ADLS/Unity Catalog and serverless saved jobs are configured.
- Full API → landing → bronze → silver → gold pipeline succeeded and was published.
- January–May 2025 backfill completed; layer counts, revenue and audit lineage matched.
- Bronze replay and silver/gold rebuild checks completed; package/runtime compatibility resolved with Standard v6 for gold.
- Tableau Cloud connects to five gold marts; dashboard and cross-source date parameters were published.
- Functional, HTTP integration, Spark ETL validation and offline end-to-end CI suites are implemented.

## Current review: community-area enrichment

- Official community API schema and all 77 public features validated read-only.
- Ingestion, geometry validation, immutable raw storage, bronze snapshots and a versioned silver reference are implemented.
- Silver rebuilds reuse the published reference for names; reference versions are bound to silver load IDs.
- Gold adds a live map dataset and verifies joins preserve known-area trip/revenue totals.
- GeoJSON boundary export and Tableau activation instructions are included.
- Azure reference ingestion, package 0.7.0, rebuild recovery and live Tableau map acceptance remain pending PR review/merge.

Follow [community-area activation](community_area_enrichment.md). Reuse the current bronze backfill; no five-month API reload is needed.

## Remaining

1. Activate and validate named area demand and Tableau map.
2. Weather ingestion/enrichment with source delay and missing-data coverage.
3. Late-arrival overlap/reconciliation policy, daily schedules, alerts and retry/recovery automation.
4. Larger backfill/runtime benchmarking and incremental silver/gold optimization.
5. Optional advisory AI PR review alongside deterministic tests.
6. MySQL serving/publication and recovery if provisioning becomes available or local serving is required.

Keep PR review before merge and cloud acceptance after merge. Snapshot table groups do not publish as one transaction;
block competing writers/readers during rebuild/failure. No daily schedule or full-year benchmark is established by these tests.
