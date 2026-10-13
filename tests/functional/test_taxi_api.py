import io
import json
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch
import pytest
from chicago_taxi.ingestion.taxi_api import pages, request_rows
from chicago_taxi.ingestion.landing import ingest, month_windows


def test_cursor_and_end_exclusive_bounds():
    calls = []
    def fetch(url, token):
        calls.append(parse_qs(urlparse(url).query))
        return [{"trip_id": "a"}, {"trip_id": "b"}] if len(calls) == 1 else [{"trip_id": "c"}]
    assert sum(len(batch) for batch in pages('2025-01-01', '2025-01-02', 2, fetch=fetch)) == 3
    assert "trip_id > 'b'" in calls[1]['$where'][0]
    assert "< '2025-01-02T00:00:00'" in calls[0]['$where'][0]


@pytest.mark.parametrize('rows', [[{}], [{'trip_id':'a'}, {'trip_id':'a'}], [{'trip_id':'b'}, {'trip_id':'a'}]])
def test_invalid_or_duplicate_keys_fail(rows):
    with pytest.raises(ValueError):
        list(pages('2025-01-01', '2025-01-02', 2, fetch=lambda *args: rows))


def test_nonadvancing_next_page_fails():
    with pytest.raises(ValueError):
        list(pages('2025-01-01', '2025-01-02', 1, fetch=lambda *args: [{'trip_id':'a'}]))


def test_page_limit_does_not_claim_complete():
    with pytest.raises(RuntimeError, match='incomplete'):
        list(pages('2025-01-01', '2025-01-02', 1, fetch=lambda *args: [{'trip_id':'a'}], max_pages=1))


def test_http_retry_and_permanent_error():
    with patch('chicago_taxi.ingestion.taxi_api.urlopen', side_effect=[HTTPError('x',429,'limit',{},None),io.BytesIO(b'[]')]) as call, patch('chicago_taxi.ingestion.taxi_api.time.sleep'):
        assert request_rows('https://example.invalid') == []
        assert call.call_count == 2
    with patch('chicago_taxi.ingestion.taxi_api.urlopen', side_effect=HTTPError('x',403,'denied',{},None)) as call:
        with pytest.raises(HTTPError):request_rows('https://example.invalid')
        assert call.call_count == 1


def test_month_batches_cover_leap_year_without_overlap():
    batches = list(month_windows('2024-01-01','2025-01-01'))
    assert len(batches) == 12
    assert batches[1] == ('2024-02-01','2024-03-01')
    assert all(left[1] == right[0] for left, right in zip(batches,batches[1:]))
    assert batches[-1][1] == '2025-01-01'


def test_landing_replay_retains_immutable_runs(tmp_path):
    rows = [{'trip_id':'a','fare':'bad','tips':None}]
    first = ingest(tmp_path,'2025-01-01','2025-01-02',fetch=lambda *args: rows)
    original = json.loads(open(first['files'][0]['path']).read())
    second = ingest(tmp_path,'2025-01-01','2025-01-02',fetch=lambda *args: rows)
    assert first['run_id'] != second['run_id']
    assert first['files'][0]['sha256'] == second['files'][0]['sha256']
    assert original == rows
    assert len(list(tmp_path.glob('taxi_trips/*/manifest.json'))) == 2


def test_partial_failure_audits_without_success_manifest(tmp_path):
    count = 0
    def broken(*args):
        nonlocal count
        count += 1
        if count == 1:return [{'trip_id':'a'}]
        raise RuntimeError('secret-token-must-not-be-logged')
    with pytest.raises(RuntimeError):
        ingest(tmp_path,'2025-01-01','2025-01-02',page_size=1,fetch=broken)
    audit_path = next(tmp_path.glob('taxi_trips/*/audit.json'))
    audit = json.loads(audit_path.read_text())
    assert audit['status'] == 'failed' and audit['extracted_rows'] == 1
    assert not list(tmp_path.glob('taxi_trips/*/manifest.json'))
    assert 'secret-token' not in audit_path.read_text()


def test_empty_window_is_success_with_zero_pages(tmp_path):
    report = ingest(tmp_path,'2025-01-01','2025-01-02',fetch=lambda *args: [])
    assert report['status'] == 'success' and report['extracted_rows'] == 0
    assert report['files'] == []


@pytest.mark.parametrize('start,end',[('2023-01-01','2023-01-02'),('2025-01-02','2025-01-01')])
def test_invalid_source_window(start,end):
    with pytest.raises(ValueError):list(pages(start,end))
