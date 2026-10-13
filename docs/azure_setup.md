# User-confirmed Azure setup

## Existing resources

- Resource group: `rg-chicago-taxi-dev`, UK South.
- Databricks workspace: `dbw-chicago-taxi-dev` (Hybrid/Premium, secure cluster connectivity,
  managed network rather than own-VNet injection).
- Host: `https://adb-7405617327453018.18.azuredatabricks.net`.
- ADLS Gen2: `stchicagotaxidevul16`, containers landing/bronze/silver/gold.
- Access Connector: `ac-chicago-taxi-dev`, system-assigned identity; Storage Blob Data Contributor
  assigned on this project storage account.
- ADF: `adf-chicago-taxi-dev-ul16`, system-assigned identity; storage data access and Contributor
  assigned on the Databricks workspace for the validated managed-identity connection.

These identifiers are configuration, not credentials. IDs and settings must still be checked
against actual Azure exports before infrastructure deployment.

## Unity Catalog

Credential: `cred_chicago_taxi_dev`.
External locations: `ext_chicago_landing_dev`, `ext_chicago_bronze_dev`,
`ext_chicago_silver_dev`, `ext_chicago_gold_dev` at their respective container roots.
Storage access validation passed; optional file events were skipped/disabled.

Catalog: `chicago_taxi_dev`. Schemas: landing, bronze, silver, gold.
Catalog fallback storage: `abfss://silver@stchicagotaxidevul16.dfs.core.windows.net/catalog_default/`.
Layer managed locations: `/managed/` inside each bronze/silver/gold container.
Landing external volume: `chicago_taxi_dev.landing.raw_files`, backed by
`abfss://landing@stchicagotaxidevul16.dfs.core.windows.net/raw/`.

## Validated execution

The owner confirmed serverless SQL catalog access and managed Delta create/read/drop tests.
A serverless Python job then wrote/read/deleted a unique landing-volume test folder, and ADF
successfully invoked that saved job using a Databricks Job activity. Connection tests alone do
not validate production ingestion, source completeness, scale or downstream publication.

Serverless Starter Warehouse is SQL-only. Python notebooks must use notebook/job serverless
compute. Serverless environment Python/Spark versions are managed independently from classic
Runtime 16.4 LTS. Use Spark Connect-compatible code; the tested client rejected errorifexists,
so the test uses overwrite only inside its fresh UUID directory.

## Blockers and cost controls

Classic clusters failed for UK South VM sizes with SkuNotAvailable. SKU checks showed subscription
restrictions. Alternative VM checks in West Europe do not establish MySQL eligibility.
Azure MySQL UK South failed with ProvisionNotSupportedForRegion; West/North Europe MySQL capability
checks failed. Do not describe the cloud database as deployed. The owner does not want to upgrade
the subscription; eligible usage may consume remaining trial credit and stops under its terms.

Cancel classic retries, keep old test pipelines unscheduled, stop idle SQL warehouses and avoid
unnecessary deployments. The managed-network NAT gateway can incur charges independently of
cluster execution. Review Cost Management rather than assuming serverless or stopped clusters
mean zero resource costs. Do not delete managed Databricks networking resources individually.

## Next setup

Export ADF definitions; configure repo integration; retain the successful job ID locally.
Create Key Vault/least-privilege secret access before database credentials are used. Add reviewed
MySQL/cloud publishing configuration when provisioning is available. AI review automation, alerts,
infra deployment and daily triggers are future phases, not enabled in this foundation.
