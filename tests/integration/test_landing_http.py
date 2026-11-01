"""Exercise the actual HTTP client against versioned WireMock contracts."""
from urllib.error import HTTPError
from urllib.parse import urlencode
import pytest
from chicago_taxi.ingestion.landing import ingest
import json
from urllib.request import build_opener, ProxyHandler

urlopen = build_opener(ProxyHandler({})).open


def request_url(registry):
    config = registry["taxi_trips"]
    return config["source_url"] + "?" + urlencode(config["query_parameters"])


def request_count(base):
    with urlopen(base + "/__admin/requests", timeout=2) as response:
        return len(json.load(response)["requests"])


def test_http_to_landing_manifest(tmp_path, source_fetch):
    result = ingest(tmp_path, '2025-01-01', '2025-01-02', page_size=2,
                    fetch=source_fetch('taxi_trips'))
    assert result['status'] == 'success' and result['extracted_rows'] == 2
    assert len(result['files']) == 1


def test_permanent_http_error_does_not_retry(source_fetch, source_registry, wiremock):
    with pytest.raises(HTTPError) as error:
        source_fetch('taxi_trips', 'forbidden')(request_url(source_registry))
    assert error.value.code == 403
    assert request_count(wiremock) == 1


def test_malformed_response_fails(source_fetch, source_registry):
    with pytest.raises(ValueError, match='JSON array'):
        source_fetch('taxi_trips', 'malformed')(request_url(source_registry))


def test_invalid_records_do_not_publish_manifest(tmp_path, source_fetch):
    with pytest.raises(ValueError, match='trip ID'):
        ingest(tmp_path, '2025-01-01', '2025-01-02', page_size=2, fetch=source_fetch('taxi_trips', 'invalid'))
    assert not list(tmp_path.glob('taxi_trips/*/manifest.json'))


def test_unexpected_endpoint_is_rejected_before_http(source_fetch, wiremock):
    with pytest.raises(AssertionError, match='Unexpected endpoint'):
        source_fetch('taxi_trips')('https://example.invalid/wrong-resource')
    assert request_count(wiremock) == 0
