# Taxi API → landing (review before activation)

## Behaviour

Socrata `ajtu-isnz` covers 2024 onward. The extractor requests Chicago wall-clock dates with an
inclusive start and exclusive end, orders by trip_id and advances by a strict trip-ID cursor.
Retries cover transient HTTP429/5xx, connection and timeout failures. Missing, duplicate,
out-of-order or stalled keys and page-limit exhaustion fail the run rather than hiding incompleteness.

Rows retain source JSON field values. Files land under
`/Volumes/chicago_taxi_dev/landing/raw_files/taxi_trips/run_id=<uuid>/start=<date>/part-<number>.json`.
Each run has an audit, per-file SHA256/bytes/row counts and a success manifest. Failed runs retain
partial raw files and a failed audit without a consumable success manifest. Exception text is not
persisted to avoid leaking request/auth details. Consumers must parse and verify complete success
manifests and hashes before processing, not scan all JSON indiscriminately.

Every replay has a unique immutable folder. Raw history intentionally contains repeated source
records across runs. Trip-key deduplication/MERGE belongs to the future bronze stage; it is not
implemented or claimed here. There is no shared watermark/latest pointer to race between writers.
Avoid overlapping production runs until downstream ordering/concurrency controls are implemented.

Modes:
- `daily`: Chicago T−1, with no fixed UTC-date assumption.
- `explicit`: supply start and exclusive end, including catch-up/repair windows.
- `backfill`: a completed year (2024+) divided into calendar-month API batches.

A year is still a single job with one final manifest: if a later month fails, the run is incomplete
and a retry re-extracts it. For practical backfill recovery, invoke explicit monthly jobs separately.
Daily T−1 is a request window, not a guarantee of source publishing freshness; inspect empty results
and use overlapping repair windows for late arrivals. No full-year throughput benchmark was run.

## Install the package on serverless

Do this after merge (or use the review branch deliberately).

1. Clone/pull the repo locally and build its wheel with Python 3.12:

```cmd
python -m pip install build
python -m build --wheel
```

2. In Databricks Catalog, open `chicago_taxi_dev.landing.raw_files`, create a `libraries` folder,
   and upload `dist/chicago_taxi_azure_lakehouse-0.2.0-py3-none-any.whl`.
3. Import `databricks/notebooks/ingest_taxi_trips.py` into Workspace as a Python notebook.
4. In its serverless Environment dependencies, add the absolute wheel path:

```text
/Volumes/chicago_taxi_dev/landing/raw_files/libraries/chicago_taxi_azure_lakehouse-0.2.0-py3-none-any.whl
```

5. Apply the environment. The execution identity needs READ VOLUME for the wheel and READ/WRITE
   VOLUME for landing data. Do not rely on relative workspace imports; serverless CWD isn't guaranteed.
6. Create saved job `job_ingest_taxi_landing`, choose this notebook and serverless compute,
   and start with retries zero, maximum concurrent runs one. Review its run-as identity.

Alternatively install the project as an absolute workspace-path dependency, as supported by
serverless environments. Do not assume a Git checkout automatically installs `src/` packages.

## First live test

Run the new job with `mode=explicit`, `start=2025-01-01`, `end=2025-01-02` and
`page_size=1000`. This calls the real public API and consumes serverless/storage credits.
No MySQL or Spark transformations are involved. Verify:
- Job succeeds and reports a run ID and extracted count.
- `audit.json` and `manifest.json` show success and the requested bounds.
- Page counts/hashes match the files; source payloads include trip_id.
- A second run preserves the first folder and produces the same expected grain after future bronze MERGE.

Anonymous API access is used initially. If throttled, add a token using reviewed Key Vault/Databricks
secret configuration; never pass it as a plain job/ADF parameter or put it in Git.

## ADF orchestration

After the job test succeeds, record its saved Job ID (not the run ID). In ADF Git mode, use a feature
branch and merge this PR before publishing the new pipeline. `pl_ingest_taxi_landing` contains a
Databricks Job activity using existing `ls_databricks_serverless_dev`.

Supply pipeline parameters:
- `ingestion_job_id`: the new saved ingestion job ID; the blank default is not runnable.
- `mode`, `start`, `end`, `year`: forwarded as job parameters.

Do not enter smoke-test Job ID `1079634501917575`; that job only tests storage. The imported smoke
pipeline remains unchanged. The ADF identity needs permission to invoke the ingestion job; its
run-as identity needs package/volume access. ADF invocation doesn't prove the ADF identity itself
has the same data permissions as the job run-as identity.

Publish reviewed changes, debug the explicit one-day window, then validate the new run's manifest.
Only after this acceptance should a daily trigger be introduced. This PR adds no trigger and runs
no Azure deployment. Weather, geography, bronze/silver/gold and MySQL publishing are next stages.
