# Full landing → bronze → silver → gold acceptance

The user confirmed that gold totals matched silver, a gold audit appeared and the saved gold job
passed its standalone ADF debug test with stable counts. The new combined four-stage pipeline still
requires live acceptance after merge.

## Pipeline and saved jobs

Open `pl_taxi_landing_bronze_silver_gold` after merge. It preserves the tested upstream activities
and runs gold only when silver succeeds. The saved jobs are:

| Pipeline parameter | Default Job ID |
|---|---|
| ingestion_job_id | 553434583210414 |
| bronze_job_id | 341771216415253 |
| silver_job_id | 654018398720089 |
| gold_job_id | 800504364677614 |

Silver and gold receive catalog chicago_taxi_dev, matching the fixed bronze destination. The existing
three-stage pipeline remains available for recovery. Use one complete pipeline at a time; concurrency 1
is scoped to each pipeline/job and does not prevent competing standalone/manual writers.

No new wheel is needed. Keep gold's tested **Standard v6 serverless Environment** and the 0.6.0 wheel.
The earlier environment used Python 3.10.12, which cannot install this package (Python >=3.12).
Do not change existing ingestion/bronze/silver dependencies for this orchestration-only PR.

## Debug after merge

1. Select main in ADF Git mode, refresh and open the new four-stage pipeline.
2. Stop competing manual, standalone and older pipeline runs. Keep dashboard refreshes stopped.
3. Click Debug, retain the four job defaults above and set:

| Parameter | Value |
|---|---|
| mode | explicit |
| start | 2025-01-01 |
| end | 2025-01-02 |
| year | 2025 |
| page_size | 1000 |
| landing_root | /Volumes/chicago_taxi_dev/landing/raw_files |

4. Verify ingest_taxi_landing → load_taxi_bronze → build_taxi_silver → build_taxi_gold all succeed.
5. Confirm the bronze audit uses the ADF pipeline Run ID, lowercased with hyphens removed.
6. Confirm silver fact + quarantine counts reconcile to the pinned bronze input and its new audit.
7. Compare gold and silver:

```sql
SELECT COUNT(*) AS trips, COUNT(trip_total) AS revenue_trips,
       SUM(trip_total) AS known_revenue_usd, COUNT(trip_km) AS distance_trips, SUM(trip_km) AS total_km
FROM chicago_taxi_dev.silver.fact_trip;
SELECT SUM(trip_count) AS trips, SUM(revenue_trip_count) AS revenue_trips,
       SUM(known_revenue_usd) AS known_revenue_usd, SUM(distance_trip_count) AS distance_trips,
       SUM(total_km) AS total_km
FROM chicago_taxi_dev.gold.mart_daily_summary;
SELECT * FROM chicago_taxi_dev.gold.reporting_loads ORDER BY completed_at_utc DESC;
SELECT * FROM chicago_taxi_dev.silver.warehouse_loads ORDER BY completed_at_utc DESC;
```

The result totals must match for the same frozen source snapshot. The new gold audit's silver_load_id
must identify the silver build from this pipeline run. Its pinned silver versions provide lineage.
8. Confirm every nonempty mart contains only that completed audit's gold_load_id:

```sql
SELECT 'daily' AS mart, gold_load_id FROM chicago_taxi_dev.gold.mart_daily_summary GROUP BY gold_load_id
UNION ALL
SELECT 'taxi', gold_load_id FROM chicago_taxi_dev.gold.mart_taxi_revenue_daily GROUP BY gold_load_id
UNION ALL
SELECT 'area', gold_load_id FROM chicago_taxi_dev.gold.mart_area_demand_daily GROUP BY gold_load_id
UNION ALL
SELECT 'company_payment', gold_load_id FROM chicago_taxi_dev.gold.mart_company_payment_daily GROUP BY gold_load_id
UNION ALL
SELECT 'distance', gold_load_id FROM chicago_taxi_dev.gold.mart_distance_extremes GROUP BY gold_load_id;
```

Empty marts can legitimately return no row. Do not assume all trips have known taxi IDs, distances or revenue.
9. After acceptance, Save and Publish all. Keep schedules unset. Do not enable both old/new pipeline schedules.

## Failure and recovery

- Each downstream job requires its predecessor to succeed. Gold is blocked on silver failure.
- If gold alone fails, check that its remote Databricks run has stopped, then rerun the saved gold job
  with catalog chicago_taxi_dev. No API extraction or silver rebuild is required when silver is unchanged.
- If gold rejects unpublished silver changes, finish/recover the saved silver job, then rebuild gold.
- If silver fails, recover silver first, then gold. If bronze fails, replay the original completed landing
  ID through bronze before rebuilding downstream stages. Avoid ADF rerun-from-failure creating a new
  landing identity for a folder that does not exist. A full new run creates a new immutable landing folder.
- ADF timeout/cancellation does not guarantee the remote job has stopped. Avoid overlapping recovery jobs.
- Silver/gold tables publish in separate transactions. Keep reporting blocked during rebuild/failure.
  A successful job, completed audit and matching mart load IDs are required before refreshing BI.

This PR adds no trigger, new package, cloud resource, CSV export, MySQL publication or dashboard.
The complete live run, annual capacity, late-arrival policy and dashboard refresh acceptance remain
separate checks. See gold_reporting.md for metric and population rules.
