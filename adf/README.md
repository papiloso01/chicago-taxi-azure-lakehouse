# Azure Data Factory definitions

Existing factory: `adf-chicago-taxi-dev-ul16`, UK South.

Validated connections:
- `ls_adls_chicago_taxi_dev`: ADLS Gen2, system-assigned managed identity.
- `ls_databricks_serverless_dev`: existing Databricks workspace, managed identity, Serverless selection.
- `pl_test_databricks_serverless`: Databricks Job activity invoking the saved serverless smoke-test job.

Live definitions are now imported under ADF-standard `pipeline/`, `linkedService/` and `factory/`
directories. The serverless test pipeline points to saved Job ID `1079634501917575`.
`pl_ingest_taxi_landing` is a new definition whose ingestion Job ID must be provided after
creating a separate serverless job; never reuse the smoke-test Job ID. Link ADF Git to this repository after PR review;
select a collaboration branch and `/adf` root deliberately. `adf_publish` is generated deployment
output; do not hand-edit it. No daily/backfill trigger is enabled yet.
