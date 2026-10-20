# Combined landing → bronze → silver acceptance

Silver notebook counts reconciled, replay counts stayed unchanged, and saved silver job
`654018398720089` succeeded through the standalone ADF test. The new three-stage pipeline
still requires a live acceptance test after merge.

## What changes

`pl_taxi_landing_bronze_silver` preserves the tested two-stage activities and adds
`build_taxi_silver` after bronze Succeeded. Job IDs:

| Stage | Job ID |
|---|---|
| Landing | 553434583210414 |
| Bronze | 341771216415253 |
| Silver | 654018398720089 |

The existing two-stage pipeline remains available for recovery. The new pipeline has concurrency 1,
retries 0 and no trigger. It passes `catalog=chicago_taxi_dev` to silver because the current bronze
notebook writes that fixed catalog. A different catalog requires a reviewed change to both stages.
Silver rebuilds the complete current bronze snapshot, not only this pipeline's new landing run.

No new wheel or notebook import is required: keep the tested ingestion/bronze dependencies and
silver's 0.5.0 environment. Do not run this pipeline alongside standalone bronze/silver jobs or the
old two-stage pipeline; concurrency limits apply separately to each pipeline/job.

## Test after merge

1. In ADF Git mode, select `main` and refresh.
2. Open `pl_taxi_landing_bronze_silver` and click Debug.
3. Use the existing one-day test values:

| Parameter | Value |
|---|---|
| ingestion_job_id | 553434583210414 |
| bronze_job_id | 341771216415253 |
| silver_job_id | 654018398720089 |
| mode | explicit |
| start | 2025-01-01 |
| end | 2025-01-02 |
| year | 2025 |
| page_size | 1000 |
| landing_root | /Volumes/chicago_taxi_dev/landing/raw_files |

4. Verify all three activities succeed in order. The shared landing ID is the ADF pipeline Run ID
   without hyphens, in lowercase. Confirm its bronze landing_runs row.
5. Check:

```sql
SELECT
  (SELECT COUNT(*) FROM chicago_taxi_dev.bronze.taxi_trips) AS bronze_rows,
  (SELECT COUNT(*) FROM chicago_taxi_dev.silver.fact_trip) AS fact_rows,
  (SELECT COUNT(*) FROM chicago_taxi_dev.silver.quarantine_trip) AS quarantined_rows;
SELECT COUNT(*) AS fact_rows, COUNT(DISTINCT trip_id) AS unique_trips
FROM chicago_taxi_dev.silver.fact_trip;
SELECT * FROM chicago_taxi_dev.silver.warehouse_loads ORDER BY completed_at_utc DESC;
```

Confirm bronze_rows = fact_rows + quarantined_rows, uniqueness, and a new completed silver audit.
Use the audit's input_rows when bronze has changed since the pinned silver source snapshot.
For unchanged one-day source data, prior fact/quarantine counts should remain stable; source corrections
can legitimately change classification. Review reasons rather than forcing all records into fact.

6. Save and Publish all after acceptance. Leave schedules unset. Use the new pipeline for future
   complete runs; retain the old pipeline for recovery, without running both concurrently.

## Failure recovery and publication

- Failed landing blocks bronze and silver. Failed bronze blocks silver.
- If bronze fails after successful landing, manually replay the original landing ID through the
  standalone bronze job, then run the saved silver job. Do not derive a new landing ID from an
  ADF recovery run and attempt to consume a nonexistent folder.
- If silver fails, inspect the remote job and wait for it to stop, then rerun the saved silver job
  with catalog chicago_taxi_dev. This full snapshot rebuild requires no new API extraction.
- Silver overwrites are separate Delta transactions. Keep reporting blocked during rebuild/failure
  and resume only after the full job succeeds and its completed audit exists. There is no gold
  publication in this change. ADF timeout/cancellation can leave a remote job still running.
- A new full pipeline run creates a new immutable landing folder. Bronze merges keys; silver
  produces a new snapshot/audit. No automatic retries or scheduling is added.

CI validates wiring, upstream preservation and success-only dependency contracts. Actual ADF expression
execution and the new combined live run remain acceptance checks. Existing Spark/ETL/offline e2e suites
continue unchanged. This PR makes no source API call, cloud deployment, MySQL publication or BI change.
