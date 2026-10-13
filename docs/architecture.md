# Architecture decisions

ADF owns schedules, date bounds, extraction orchestration and Databricks Job invocation.
Databricks serverless owns PySpark transformations. ADLS retains raw files and managed Delta
bronze/silver/gold tables. Unity Catalog owns table/volume privileges and storage identity.
MySQL is the planned reporting/control store, not the raw-data lake.

First ingest a completed year in date batches, then Chicago-local T−1 with deliberate overlap
for late source updates. Weather uses its independent delayed overlap. Advance watermarks only
after quality checks and successful publication. Source keys and MERGE logic must make replay safe.

Bronze preserves payloads. Silver cleans and quarantines, builds one-trip facts and unique dimensions.
Gold produces dashboard grains. Unknown enrichment must not lose trips or inflate money. MySQL
publication stages by run and validates before promoting; design transaction/recovery semantics
with the actual publisher. No direct Spark overwrite of live dashboard tables.

Use offline fixtures in CI. Cloud acceptance tests are explicit manual/gated jobs with managed
identities. The current functional suite covers date windows and metadata only. Rerun correctness,
end-to-end ETL and annual-volume performance are pending implementation.
