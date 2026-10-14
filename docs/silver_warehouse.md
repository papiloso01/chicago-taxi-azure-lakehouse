# Bronze → silver warehouse

The combined ADF landing→bronze test succeeded. Silver is implemented here but has not been run in Azure.
It creates managed Delta tables in `chicago_taxi_dev.silver`. Gold reporting marts remain a later step.

## Tables and meaning

| Table | Grain / key | Purpose |
|---|---|---|
| fact_trip | One accepted current trip / trip_id | Typed measures, timestamps, dimension keys and bronze provenance |
| dim_taxi | One observed taxi identifier / taxi_key | Vehicle identity, not driver identity |
| dim_company | One normalized company name / company_key | Company analysis; uppercase, trimmed, collapsed whitespace |
| dim_payment_type | One normalized payment type / payment_type_key | Payment breakdown |
| dim_date | One observed start date / date_key | Date attributes; this is not yet a complete calendar spine |
| dim_community_area | One observed area ID / area_key | Pickup/dropoff roles; official names/geometry not loaded yet |
| quarantine_trip | One rejected bronze trip | Raw payload, quality reasons, run/hash provenance |
| warehouse_loads | One completed rebuild / silver_load_id | Pinned bronze version and reconciliation counts |

Keys for string dimensions are deterministic SHA256 hashes. Unknown identity has its own namespace;
missing values do not collide with a literal source value `UNKNOWN`. Community-area key 0 means unavailable,
while IDs 1–77 are valid. Only observed members are materialized. Date day_of_week uses Sunday=1 through Saturday=7. Fact foreign keys are checked for orphans.
Schemas are defined in `src/chicago_taxi/transformations/silver/contracts.py` and documented in metadata.

## Cleaning and reporting rules

- Source local timestamps are parsed explicitly as TIMESTAMP_NTZ, preserving Chicago wall-clock times.
  No UTC instant is inferred for ambiguous DST hours. Source timestamps are rounded; use reported
  trip_seconds for duration rather than subtracting rounded start/end times.
- Trip distance is DECIMAL(18,6) in miles and kilometres; km = miles × 1.609344, rounded to six decimals.
  Reported trip distance does not identify an actual road route.
- Fare, tips, tolls, extras and trip total are DECIMAL(18,2) USD, rounded to cents. Trip total is reported
  gross revenue, not earnings/profit. Do not synthesize totals from components.
- Taxi identity is not a driver identity. Passenger gender is absent and is not inferred.
- Required: nonblank trip ID and valid source start timestamp. Optional absent duration/distance/money,
  taxi/company/payment/area values remain unavailable, with coverage flags on key measures.
- Nonblank malformed or out-of-range numeric values, negative duration/distance/money, unsupported
  timestamp formats, invalid dates, end before start, and area IDs outside 1–77 are quarantined.
  Negative financial values may represent corrections; retain them in quarantine for investigation.
- Zero miles is valid. Gold's shortest-positive-trip metric must exclude zero; trip counts may include it.
- No arbitrary maximum-fare/distance cutoffs, refund interpretation, revenue-component balancing rule,
  exact route reconstruction, or gender/driver enrichment is invented.
- Every source trip must reconcile into fact or quarantine. Null measures must not be converted to zero
  or silently treated as complete coverage in reporting.

## Snapshot publication

The loader pins a bronze Delta version and rebuilds the entire current silver snapshot, so a corrected
trip can move between quarantine and fact without stale rows remaining. Dimensions are current snapshots,
not historical SCD2 entities. This is a correctness-first development implementation; full annual
rebuild performance has not been benchmarked and incremental processing is future work.

Each table overwrite is atomic, but the group of tables and audit append is **not one transaction**.
Stop competing silver writers and downstream reporting during rebuild. A completed job and its new
warehouse_loads record are the publication boundary. If a write fails, rerun the full rebuild and keep
reporting blocked until it completes. This change does not add a gold job, view, trigger or ADF activity.

## First Azure acceptance after merge (Windows CMD)

```cmd
git switch main
git pull origin main
python -m build --wheel
```

1. Upload `dist/chicago_taxi_azure_lakehouse-0.5.0-py3-none-any.whl` into the volume's `libraries` folder.
2. Import `databricks/notebooks/build_taxi_silver.py`.
3. Choose serverless and set the Environment dependency to:
   `/Volumes/chicago_taxi_dev/landing/raw_files/libraries/chicago_taxi_azure_lakehouse-0.5.0-py3-none-any.whl`.
   Install only that package version in the silver environment. Existing ingestion/bronze job dependencies
   do not need changing for this acceptance test.
4. Run imports and widgets. Keep catalog `chicago_taxi_dev`, then run the load cell.
5. Expected result: `status=success`, pinned `bronze_version`, `input_rows`, `fact_rows`, `quarantined_rows`.
   **Do not assume all 9,285 trips pass**: reconcile actual rejection reasons and counts.
6. Check the database:

```sql
SELECT COUNT(*) AS fact_rows, COUNT(DISTINCT trip_id) AS unique_trips
FROM chicago_taxi_dev.silver.fact_trip;
SELECT COUNT(*) AS rejected_rows FROM chicago_taxi_dev.silver.quarantine_trip;
SELECT * FROM chicago_taxi_dev.silver.warehouse_loads ORDER BY completed_at_utc DESC;
SELECT explode(quality_issues) AS issue, COUNT(*) AS rejected_records
FROM chicago_taxi_dev.silver.quarantine_trip
GROUP BY issue ORDER BY rejected_records DESC;
```

Count each rejected record once for reconciliation; a record can have multiple issues in the last query.
The latest audit must satisfy input_rows = fact_rows + quarantined_rows. Fact unique_trips must equal fact_rows.

7. Verify joins preserve trip count:

```sql
SELECT COUNT(*) AS joined_trips
FROM chicago_taxi_dev.silver.fact_trip f
JOIN chicago_taxi_dev.silver.dim_taxi t ON f.taxi_key=t.taxi_key
JOIN chicago_taxi_dev.silver.dim_company c ON f.company_key=c.company_key
JOIN chicago_taxi_dev.silver.dim_payment_type p ON f.payment_type_key=p.payment_type_key
JOIN chicago_taxi_dev.silver.dim_date d ON f.start_date_key=d.date_key
JOIN chicago_taxi_dev.silver.dim_community_area pickup ON f.pickup_area_key=pickup.area_key
JOIN chicago_taxi_dev.silver.dim_community_area dropoff ON f.dropoff_area_key=dropoff.area_key;
```

8. Repeat the notebook load without a bronze change. Fact/quarantine counts and dimension keys must
   remain stable. A new audit record is expected for every rebuild.
9. After acceptance, save `job_build_taxi_silver`, serverless, max concurrency 1, retries 0, no schedule.
   ADF integration follows only after that saved job test passes.

## CI coverage

Functional tests, HTTP ingestion integration, Spark ETL validation and offline end-to-end transformations
have distinct suites in GitHub Actions and CircleCI. The offline e2e uses fixture API rows through actual
landing files, validated staging, bronze Spark preparation and silver facts/dimensions/quarantine.
It does not test Databricks, ADF expressions, Unity Catalog, Delta storage publication or MySQL.
Live type compatibility, source-specific quarantine volumes, overwrite/replay, permissions and annual
performance remain cloud acceptance checks.

Official reference: [Databricks TIMESTAMP_NTZ](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/data-types/timestamp-ntz-type).
New Delta tables with this type automatically enable its table feature; consumers must support it.
