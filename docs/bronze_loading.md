# Landing → bronze acceptance

This step loads one explicitly selected completed landing run into managed Delta tables:
`chicago_taxi_dev.bronze.taxi_trips` and `chicago_taxi_dev.bronze.landing_runs`.
It does not clean business values, create silver facts/dimensions, or schedule work.

## Contract

- Only a success manifest is accepted. Run identity, dates, contiguous windows, file containment,
  sizes, hashes, file row counts and total counts are checked before touching tables.
- Per-page JSONL staging bounds Python memory by one source page. Spark processes staged data
  with an explicit schema. Original landing files remain unchanged; temporary staging is removed.
- Raw payload retains every source field/value (JSON whitespace/key order is normalized), plus
  payload hash, run ID, source file, extraction timestamp and bronze load timestamp.
- Common source fields remain strings. Type conversion and business validation belong in silver.
- Key: `trip_id`. Identical duplicates collapse with stable file selection; conflicting payloads
  within a run fail. Delta MERGE inserts new trips and replaces matched trips only from a newer
  extraction. Run ID breaks extraction timestamp ties. Replaying the same run does not change trips.
- `landing_runs` records extracted and distinct input counts after the trip merge. They are separate
  Delta transactions; a failure writing audit may leave trips committed. Replay repairs that audit.
- Empty successful windows produce an audit with zero counts. Missing trips are not interpreted as
  deletions. Run one bronze writer at a time: Delta has no enforced unique trip key.
- Landing folders must remain immutable. Hard process termination can leave staging folders;
  remove orphan `_bronze_stage` folders manually only when no loader is running.

## First live test (Windows CMD)

After merge, from the repository folder:

```cmd
git switch main
git pull origin main
python -m pip install build
python -m build --wheel
```

1. Upload `dist/chicago_taxi_azure_lakehouse-0.3.0-py3-none-any.whl` into the landing volume's `libraries` folder.
2. Import `databricks/notebooks/load_taxi_bronze.py` as a notebook.
3. Choose serverless. Set its Environment dependency to the absolute uploaded **0.3.0** wheel path.
   Replace the old dependency rather than installing both versions.
4. Run the imports and widget cell. Set `landing_run_id` to the successful notebook test run
   `3d82e5dd38704205b4754da69d81348d`. Keep the default volume root.
5. Run the load cell. Expect success with extracted and distinct counts of 9,285 if there were no duplicates.
6. In a SQL notebook/warehouse, run:

```sql
SELECT COUNT(*) AS rows, COUNT(DISTINCT trip_id) AS unique_trips
FROM chicago_taxi_dev.bronze.taxi_trips;
SELECT * FROM chicago_taxi_dev.bronze.landing_runs;
```

7. Replay the same landing run. Trip count must remain unchanged.
8. Load the ADF-produced landing run by its own run ID. If it contains the same trip keys,
   count must remain unchanged; newer source values/provenance may update.
9. Save a separate `job_load_taxi_bronze` with maximum concurrency 1 and retries 0 after acceptance.
   Keep schedules unset. ADF → bronze handoff is deferred until the job and acceptance checks work.

## Validation limits

CI covers manifest integrity and actual local Spark transformation plans. It does not run Azure,
Unity Catalog or Delta MERGE. Live merge/replay and permissions checks above remain required.
One-year capacity/performance has not been benchmarked; start with a day, then a month.
