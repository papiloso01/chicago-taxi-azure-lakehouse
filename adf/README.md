# Azure Data Factory definitions

Existing factory: `adf-chicago-taxi-dev-ul16`, UK South.

Validated connections:
- `ls_adls_chicago_taxi_dev`: ADLS Gen2, system-assigned managed identity.
- `ls_databricks_serverless_dev`: existing Databricks workspace, managed identity, Serverless selection.
- `pl_test_databricks_serverless`: Databricks Job activity invoking the saved serverless smoke-test job.

The exact live JSON is not available in this session. Do not manufacture live Job IDs, authentication
properties or claim these definitions were exported. Export reviewed definitions from ADF Studio
into the directories below, excluding credentials. Link ADF Git to this repository after PR review;
select a collaboration branch and `/adf` root deliberately. `adf_publish` is generated deployment
output; do not hand-edit it. No daily/backfill trigger is enabled yet.
