# Implementation roadmap

## Completed and reported live acceptance

- GitHub foundation, ADF Git integration, ADLS and Unity Catalog setup.
- One-day API extraction: 9,285 rows in the initial notebook test.
- Bronze Delta loading, same-run replay and saved-job execution through ADF.
- Combined landing → bronze pipeline succeeded and was published.
- Silver typed fact/dimensions/quarantine ran successfully; counts reconciled, uniqueness checks
  passed and replay counts stayed unchanged.
- Saved silver job 654018398720089 passed its ADF test; user saved and published.
- Functional, HTTP integration, Spark ETL validation and offline e2e CI suites implemented.

## Current review and next acceptance

The three-stage ADF pipeline succeeded; the user confirmed matching counts, completed audits and publication.
Gold reporting marts are implemented. Review/merge the gold PR, then follow
[gold acceptance](gold_reporting.md) before creating its saved job or connecting BI.

## Remaining

1. Community-area names/geometry and weather ingestion/enrichment.
2. Gold live acceptance, saved-job/ADF handoff, BI exports and dashboard connection.
3. Year backfill benchmarking, late-arrival overlap/reconciliation and incremental silver optimization.
4. Key Vault, monitored schedules, failure alerts and reviewed infrastructure automation.
5. Optional advisory AI PR review; keep deterministic CI checks authoritative.
6. MySQL serving/publication and recovery tests; Azure provisioning remains blocked for this subscription.
7. Tableau/Power BI dashboard connection and reporting acceptance.

Use PRs for review before merge. No daily schedule, full-year benchmark, cloud MySQL or BI deployment
is established by the one-day tests. Silver is a full snapshot and its multiple tables do not publish
as one transaction; block downstream readers while rebuilding or recovering.
