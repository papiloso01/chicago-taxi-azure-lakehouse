# Source HTTP contract tests

Integration tests use a real, isolated WireMock 3.13.2 Java process. No live source API or Azure resource is contacted. Python 3.12 and Java 17 are required. Both GitHub Actions and CircleCI download the checksum-pinned JAR before this suite; pytest never downloads tools and fails if the JAR is missing.

```bash
python -m pip install -e '.[test]'
python tools/download_wiremock.py
python -m pytest tests/integration --wiremock-jar .tools/wiremock.jar
```

Run from the repo root. These commands work in Windows PowerShell too, provided Java is on PATH. Use `--fixtures-dir PATH` to pass another fixture folder with the same layout. Output files use pytest's unique `tmp_path`, separate from read-only response fixtures. Unit and Spark suites do not require WireMock.

## Add a data source

1. Add its logical name, expected production URL, schema path and named case routes to `tests/fixtures/sources.json`.
2. Add a response contract under `tests/fixtures/schemas/`. These JSON Schemas describe the raw source response, not Spark warehouse schemas. Allow extra fields for additive changes; model fields relevant to the adapter explicitly. Domain checks remain in the production transformations.
3. Put synthetic response bodies under `wiremock/__files/<source>/` and request rules under `wiremock/mappings/`. Each case has a unique route, preventing collisions as sources grow. Use exact query matchers for pagination. Never include credentials or personal data.
4. Add adapter tests using `source_fetch('<source>', '<case>')`. This redirects only the registered source endpoint, preserves query parameters, and rejects unexpected endpoints before making network requests. It does not validate responses itself, so malformed payloads still exercise production error handling.
5. Run functional fixture validation and the integration suite. Functional checks validate every registered happy response against its schema and ensure referenced files exist. Extend contracts and error cases when adding source fields or behaviour.

One session owns one loopback WireMock process on a dynamically allocated port. Requests and scenarios reset before every test; unmatched requests fail the test. Do not share mutable scenario state between tests. The managed process is terminated after the suite. No external shared `--wiremock-url` is offered: isolation avoids tests resetting another developer's or CI worker's mappings.

Request mappings use `get_<source>` names; response files use `<source>_response` names, with descriptive case suffixes. They are separate JSON files, not executable Databricks notebooks. Query parameters explicitly match limits, ordering, dates and pagination cursors.

The community HTTP fixture contains one area. Its raw response schema checks individual record structure; the existing functional reference tests still generate all 77 areas to validate production completeness. The HTTP suite no longer claims to test a complete community extraction.

Current cases cover taxi pagination, HTTP 429 recovery, permanent 403 without retry, malformed responses and invalid rows without a success manifest, plus a single-record community HTTP response. Fixtures are synthetic; schema tests do not detect changes at the live provider. Live contract monitoring is separate future work.

References: https://wiremock.org/docs/standalone/java-jar/ and https://wiremock.org/docs/stateful-behaviour/

The taxi registry only specifies the page limit. Date bounds, ordering and pagination cursors are produced by ingestion code and asserted in its GET mappings. Retry behaviour is checked through a WireMock 429-then-200 scenario and focused unit tests. The successful retry reuses taxi_trips_response.json.
