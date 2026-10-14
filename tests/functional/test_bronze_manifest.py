import hashlib
import json
from pathlib import Path
import pytest
from chicago_taxi.ingestion.landing import ingest
from chicago_taxi.transformations.bronze.landing_manifest import validate_manifest, stage_run
from chicago_taxi.transformations.bronze.taxi_trips import table_name, merge_sql


def run(tmp_path, rows=None):
    data = rows if rows is not None else [{"trip_id": "a", "fare": "12.50", "new_field": {"x": 1}}]
    report = ingest(tmp_path, "2025-01-01", "2025-01-02", fetch=lambda url, token: data)
    return tmp_path / "taxi_trips" / ("run_id=" + report["run_id"])


def mutate(folder, callback):
    p = folder / "manifest.json"
    m = json.loads(p.read_text())
    callback(m)
    p.write_text(json.dumps(m))


def test_stage_preserves_unknown_fields_and_provenance(tmp_path):
    folder = run(tmp_path)
    manifest, stage = stage_run(folder)
    row = json.loads(next(stage.glob('*.jsonl')).read_text())
    assert json.loads(row['raw_payload'])['new_field'] == {'x': 1}
    assert row['landing_run_id'] == manifest['run_id']
    assert row['payload_sha256'] == hashlib.sha256(row['raw_payload'].encode()).hexdigest()
    assert validate_manifest(folder)['extracted_rows'] == 1


@pytest.mark.parametrize('kind', ['status', 'total', 'rows', 'hash', 'identity', 'windows', 'timestamp', 'duplicate_path'])
def test_reject_invalid_manifest(tmp_path, kind):
    folder = run(tmp_path)
    def change(m):
        if kind == 'status': m['status'] = 'failed'
        elif kind == 'total': m['extracted_rows'] = 2
        elif kind == 'rows': m['files'][0]['rows'] = 2
        elif kind == 'hash': m['files'][0]['sha256'] = '0'*64
        elif kind == 'identity': m['run_id'] = '0'*32
        elif kind == 'windows': m['windows'][0]['start'] = '2025-01-02'
        elif kind == 'timestamp': m['started_at_utc'] = '2025-01-01'
        elif kind == 'duplicate_path': m['files'].append(m['files'][0])
    mutate(folder, change)
    with pytest.raises(ValueError): stage_run(folder)
    assert not (folder / '_bronze_stage').exists()


def test_external_path_rejected(tmp_path):
    folder = run(tmp_path)
    mutate(folder, lambda m: m['files'][0].update(path=str(tmp_path/'other.json')))
    with pytest.raises(ValueError, match='outside'): validate_manifest(folder)


def test_missing_manifest_rejected(tmp_path):
    folder = run(tmp_path)
    (folder/'manifest.json').unlink()
    with pytest.raises(FileNotFoundError): stage_run(folder)


def test_empty_success(tmp_path):
    folder = run(tmp_path, [])
    m, stage = stage_run(folder)
    assert m['extracted_rows'] == 0 and not list(stage.iterdir())


@pytest.mark.parametrize('name', ['bronze.taxi', 'a.b.c;DROP TABLE x', 'a.b.c.d'])
def test_identifier_injection_rejected(name):
    with pytest.raises(ValueError): table_name(name)


def test_merge_key_and_newer_run_guard():
    sql = merge_sql('chicago_taxi_dev.bronze.taxi_trips', 'bronze_stage_'+'a'*32)
    assert 't.trip_id = s.trip_id' in sql
    assert 's.extracted_at_utc > t.extracted_at_utc' in sql
    assert 'WHEN NOT MATCHED THEN INSERT *' in sql
