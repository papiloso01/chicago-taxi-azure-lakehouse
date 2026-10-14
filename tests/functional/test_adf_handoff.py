import json
from pathlib import Path
import pytest
from chicago_taxi.ingestion.landing import ingest
from chicago_taxi.transformations.bronze.landing_manifest import validate_manifest


def test_shared_id_links_exact_landing_run(tmp_path):
    run_id = 'b'*32
    report = ingest(tmp_path, '2025-01-01', '2025-01-02', run_id=run_id,
                    fetch=lambda *args: [{'trip_id': 'a'}])
    folder = tmp_path / 'taxi_trips' / ('run_id=' + run_id)
    assert report['run_id'] == run_id
    assert validate_manifest(folder)['extracted_rows'] == 1


@pytest.mark.parametrize('run_id', ['../escape', 'A'*32, '', 'a'*31, 123])
def test_invalid_shared_id_creates_no_folder(tmp_path, run_id):
    with pytest.raises(ValueError, match='run_id'):
        ingest(tmp_path, '2025-01-01', '2025-01-02', run_id=run_id, fetch=lambda *args: [])
    assert not list(tmp_path.iterdir())


def test_same_id_never_overwrites_completed_run(tmp_path):
    run_id = 'c'*32
    first = ingest(tmp_path, '2025-01-01', '2025-01-02', run_id=run_id, fetch=lambda *args: [{'trip_id':'a'}])
    manifest = tmp_path/'taxi_trips'/('run_id='+run_id)/'manifest.json'
    original = manifest.read_bytes()
    with pytest.raises(FileExistsError):
        ingest(tmp_path, '2025-01-01', '2025-01-02', run_id=run_id, fetch=lambda *args: [])
    assert manifest.read_bytes() == original
    assert first['extracted_rows'] == 1


def test_failed_shared_run_cannot_be_consumed(tmp_path):
    def fail(*args): raise RuntimeError('failed fetch')
    with pytest.raises(RuntimeError):
        ingest(tmp_path, '2025-01-01', '2025-01-02', run_id='d'*32, fetch=fail)
    folder = tmp_path/'taxi_trips'/('run_id='+'d'*32)
    with pytest.raises(FileNotFoundError): validate_manifest(folder)
    assert json.loads((folder/'audit.json').read_text())['status'] == 'failed'


def test_pipeline_contract():
    p = json.loads(Path('adf/pipeline/pl_taxi_landing_bronze.json').read_text())['properties']
    assert p['concurrency'] == 1
    ingest_activity, bronze_activity = p['activities']
    assert ingest_activity['dependsOn'] == []
    assert bronze_activity['dependsOn'] == [{'activity':'ingest_taxi_landing','dependencyConditions':['Succeeded']}]
    shared = "@toLower(replace(pipeline().RunId, '-', ''))"
    for activity in p['activities']:
        assert activity['type'] == 'DatabricksJob'
        assert activity['policy']['retry'] == 0
        assert activity['linkedServiceName']['referenceName'] == 'ls_databricks_serverless_dev'
        assert activity['typeProperties']['jobParameters']['landing_run_id']['value'] == shared
        assert activity['typeProperties']['jobParameters']['landing_root']['value'] == '@pipeline().parameters.landing_root'
    assert p['parameters']['ingestion_job_id']['defaultValue'] == '553434583210414'
    assert p['parameters']['bronze_job_id']['defaultValue'] == '341771216415253'
    assert 'runOutput' not in json.dumps(p)
