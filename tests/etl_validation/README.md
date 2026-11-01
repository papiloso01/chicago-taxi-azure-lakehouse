# ETL validation and related test suites

These tests exercise production PySpark DataFrame transformations using small, deterministic datasets. No Azure account, Databricks cluster, live API or WireMock server is required for transformation validation.

## Responsibilities

| Suite | Executes | Checks |
| --- | --- | --- |
| `tests/functional` | Python functions and mocked publication orchestration | API logic, publication guards, all-table version pinning, stale/missing audit rejection and reference replay decisions |
| `tests/etl_validation` | Real local Spark transformations | Bronze duplicate/raw handling; silver types, conversions, quarantine, keys and joins; gold sums, null coverage, ranking/ties and area-role totals; map lineage/count preservation |
| `tests/e2e` | Offline landing through gold transformation functions | Stage compatibility, accepted/quarantined row accounting and reproducible identifiers |
| `tests/delta_integration` | Actual local Delta SQL and storage writes in isolated JVMs | Bronze MERGE replay, newer/stale/tied extraction precedence, historical reads, landing-run/audit replay and silver/gold overwrite round trips |

Mocked publication tests assert decisions and exact queries, not storage effects. Each silver table has a different mocked version to detect accidentally reusing another table's version. Transformation tests assert concrete expected values independently of reconciliation functions; reconciliation is an additional check, not the only oracle. Production schema contracts remain in the application package; tests compare actual transformation output against them.

## Fixtures and isolation

Readable source examples live in `tests/fixtures/etl/`. Shared builders in `tests/support/data.py` adapt these into explicitly typed DataFrames. Shared pytest fixtures in `tests/support/fixtures.py` provide fresh input objects per call. `--fixtures-dir PATH` selects the complete fixture root for all suites.

Community tests reuse the single geometry example already used by HTTP tests. Only tests of the production completeness rule expand that example into 77 uniquely identified/named synthetic records. These are not actual community boundaries.

One lazy Spark JVM is shared by ordinary transformation tests. Each test gets a fresh SQL session with UTC, ANSI mode, two shuffle partitions and cleared cache/temp views on teardown; the JVM is stopped at session end. Gold examples include several trips and dates deliberately: a single row cannot establish aggregate sums, ties, unknown-value coverage or join cardinality. Expected results remain explicitly asserted in each test. E2E staging files and warehouses use pytest temporary directories.

## Run locally

Python 3.12 and Java 17:

```bash
python -m pip install -e '.[test,spark-test]'
python -m pytest tests/functional
python -m pytest tests/etl_validation tests/e2e
python tools/download_delta.py
python -m pytest tests/delta_integration
```

The Delta downloader verifies pinned SHA-256 hashes for Delta 3.3.2 JARs, compatible with Spark 3.5 / Scala 2.12. The Delta suite fails clearly if tools are missing; it never silently skips or downloads during pytest. Use `--delta-jars-dir PATH` for another prepared tool folder. Both GitHub Actions and CircleCI run Delta integration as its own job. To run all tests together, also prepare WireMock as described in `tests/integration/README.md` and pass `--wiremock-jar PATH`.

## Boundaries

Local Delta tests execute production bronze MERGE and landing/audit loading code. Silver and gold checks execute production transformations followed by real Delta writes; they do not exercise the entire multi-table `load_warehouse` or `load_gold` publication transaction sequence. Tests do not claim Unity Catalog permissions, ADF orchestration, cloud concurrency, serverless compatibility or multi-table atomicity. Those need separate deployed-environment validation. Replay does not imply zero new Delta history entries; the checks establish stable business rows and one audit row per landing run.

References: https://docs.delta.io/releases/ and https://docs.delta.io/quick-start/
