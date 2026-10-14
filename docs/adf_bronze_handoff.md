# ADF landing → bronze handoff

Both individual saved jobs were tested through ADF. This new combined pipeline still needs a live acceptance run.

`pl_taxi_landing_bronze` runs ingestion Job `553434583210414`, then bronze Job `341771216415253` only on ingestion success. It gives both jobs the same `landing_run_id`, derived from the current ADF pipeline RunId by removing hyphens and lowercasing. This identifies the exact immutable landing folder without selecting a latest folder or relying on Databricks activity return-value formatting.

## Deploy after merge (Windows CMD)

```cmd
git switch main
git pull origin main
python -m build --wheel
```

1. Upload `dist/chicago_taxi_azure_lakehouse-0.4.0-py3-none-any.whl` into the volume's `libraries` folder.
2. Re-import/replace the **ingestion notebook** from `databricks/notebooks/ingest_taxi_trips.py`.
   Make sure existing ingestion Job `553434583210414` points to that updated notebook.
3. Set the ingestion job task's serverless Environment dependency to the absolute **0.4.0** wheel path, replacing its earlier version. This is required: the earlier notebook/package generates an unrelated random ID.
4. Set bronze Job `341771216415253` to the same 0.4.0 wheel (its notebook code is unchanged).
5. In both jobs, add job parameter `landing_run_id` with an empty default and `landing_root` with the volume path. In ingestion also expose `mode`, `start`, `end`, `year`, and `page_size` as job parameters. Job-level notebook parameters override task defaults when provided by ADF.
6. Keep retries 0, maximum concurrent runs 1 and schedules unset. For manual bronze replay supply a real completed run ID; its empty default is intentionally invalid.
7. Refresh ADF on the `main` Git branch; open `pl_taxi_landing_bronze`.
8. Debug with `mode=explicit`, `start=2025-01-01`, `end=2025-01-02`, `page_size=1000` and the default job IDs/root.
9. Record the ADF pipeline Run ID. Remove hyphens and lowercase it; this must be the new landing folder ID and bronze audit ID.
10. Both activities must succeed. Check the new manifest, then verify:

```sql
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT trip_id) AS unique_trips
FROM chicago_taxi_dev.bronze.taxi_trips;
SELECT * FROM chicago_taxi_dev.bronze.landing_runs
ORDER BY loaded_at_utc DESC;
```

For the same day's source keys, the expected trip count remains 9,285 and a new audit row appears for this pipeline run. If the source has since added records, reconcile against the new manifest rather than assuming the old count remains authoritative.

11. After acceptance, Save and Publish all. No trigger is created by this change.

## Failure and recovery

- Ingestion failure blocks bronze; no success manifest is produced.
- Existing landing folders are never overwritten. Both success and failed IDs cannot be reused for extraction.
- If bronze fails after ingestion succeeds, use the standalone bronze job with the original landing ID. Do **not** use ADF 'rerun from failed activity': a recovery run can have a new pipeline RunId and point bronze at a folder that was never extracted.
- To repeat the full pipeline, start a new full run; it creates a new immutable folder. Trip MERGE handles replay by key.
- Timeouts/cancellation do not guarantee immediate termination of the remote Databricks job. Inspect its run state before retrying.
- Pipeline concurrency is 1, but manual standalone jobs can still overlap; do not run competing bronze writers.
- Default mode is daily T−1. Always select the explicit test window for acceptance. Source publication delays mean a successful T−1 run may be empty/partial; late-arrival overlap and backfill policy are later work, before scheduling.

## References

- [ADF system variables](https://learn.microsoft.com/en-us/azure/data-factory/control-flow-system-variables)
- [ADF Databricks Job activity](https://learn.microsoft.com/en-us/azure/data-factory/transform-data-databricks-job)
- [Databricks job parameters](https://learn.microsoft.com/en-us/azure/databricks/jobs/job-parameters)

Local tests prove shared folder identity, invalid-ID rejection, no-overwrite behavior, incomplete-run rejection and the ADF dependency/parameter contract. They do not execute Azure expressions or the combined live jobs. No schedule, annual backfill, silver/gold or MySQL publishing is deployed.
