# Test handover guide

Start here, then follow [HTTP integration](integration/README.md) or [ETL and Delta coverage](etl_validation/README.md).

## First run

Run from the repository root with Python 3.12 and Java 17 on PATH:

```text
python -m pip install -e ".[test,spark-test]"
python tools/download_wiremock.py
python tools/download_delta.py
python -m pytest tests --wiremock-jar .tools/wiremock.jar
```

Run one suite or one named test when investigating a failure. Descriptive pytest names and separate Delta cases identify the failed behaviour. Both CI configurations run the same five suite directories. Fixtures and tools are configurable with `--fixtures-dir`, `--wiremock-jar` and `--delta-jars-dir`.

## Where changes belong

| Change | Edit | Run first |
| --- | --- | --- |
| New JSON-array HTTP source | `fixtures/sources.json`, schema, GET mappings and response JSON | Functional fixture checks, then HTTP integration |
| Conversion or quarantine rule | Production transformation, input example and expected assertions | Relevant ETL validation test |
| Reporting metric | Gold transformation and independently stated expected values | Gold validation, then offline E2E |
| Publication guard | Production loader and corresponding functional publication test | Functional tests |
| Storage/replay rule | Production Delta logic and named Delta case | Delta integration |
| Environment identifiers | ADF exports and environment contract | ADF functional tests |

`source_fetch` currently adapts JSON-array APIs consumed by `request_rows`; it is not a universal adapter for every API response format. Add a separate adapter and matching tests for a different protocol or response envelope.

## Pagination defaults and overrides

No pagination configuration means `$limit=1000`. Per-source settings override this without changing shared code:

```json
{
  "pagination": {
    "limit_parameter": "page_size",
    "page_size": 50
  },
  "query_parameters": {
    "category": "taxi"
  }
}
```

Use `"pagination": null` for an API with no limit parameter. Explicit `query_parameters` values take precedence over generated defaults. GET mappings must match the configured parameters; fixture validation requires only the keys configured for that source, never a universal `$limit`. Cursor, date and ordering semantics belong to the individual adapter and its mappings. The taxi registry does not set `$where` or `$order`.

## Environment expectations

ADF functional tests read `fixtures/environments/default.json`. They check wiring and valid positive numeric job IDs rather than one developer's exact job IDs. To review exports for another catalog/linked service:

```text
python -m pytest tests/functional --adf-contract path/to/environment.json
```

The alternate contract has `catalog` and `linked_service` keys. This selects expectations for the committed exports; it does not deploy or reconfigure Azure. Metadata tests calculate the source count from metadata and explicitly exercise adding another source.

## Coverage

CI records Python statement and branch coverage, including the separate Delta subprocesses. GitHub Actions publishes a combined report across the five suites; CircleCI stores a report for each job. Download the coverage artifact and open `coverage-html/index.html`, or inspect `coverage.xml`/`coverage.json`. Individual suite percentages are partial views; use the combined report for repository-level assessment.

To measure the complete suite locally in Windows CMD:

```cmd
set COVERAGE_FILE=test-results/coverage-data
python -m coverage run -m pytest tests --wiremock-jar .tools/wiremock.jar
python tools/coverage_report.py
```

In PowerShell, replace the `set` line with `$env:COVERAGE_FILE = "test-results/coverage-data"`; in Bash use `export COVERAGE_FILE=test-results/coverage-data`. Reports are generated under `test-results/`. Coverage is scoped to `src/chicago_taxi`, not tests, tools or thin notebooks. Coverage measures executed Python paths; it does not measure Spark's JVM engine or prove business correctness. No percentage gate is set yet: review the combined baseline and important uncovered paths before adopting a threshold.

## Limits

Synthetic data checks logic and contracts without live API/Azure access. The test docs distinguish real DataFrame execution, mocked publication decisions, actual local Delta writes and cloud acceptance. Do not treat passing CI as evidence that deployed credentials, Unity Catalog permissions or live API schemas are healthy.

## Readability convention

Test code uses domain-specific shared builder names and Ruff's consistent 100-column formatting. For editing tests, install the optional `dev` extra and run:

```text
python -m pip install -e ".[dev]"
ruff format tests
ruff check tests --select F
```

Keep expected values visibly separate from production calculations. Use one named test case per behaviour; do not duplicate an implementation to calculate its expected result.

Measured handover baseline for this revision: 85.4% Python statement coverage and 78.0% branch coverage (83.3% combined). Important uncovered areas include geography publication and full silver/gold loader publication paths. This is a measured baseline, not a claim of complete correctness or cloud acceptance.
