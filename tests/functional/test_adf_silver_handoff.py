"""Guard ordering, environment alignment and failure propagation for live job wiring."""
import json
from pathlib import Path


def test_combined_pipeline_preserves_tested_upstream_and_gates_silver():
    root = Path('adf/pipeline')
    original = json.loads((root/'pl_taxi_landing_bronze.json').read_text())['properties']
    pipeline = json.loads((root/'pl_taxi_landing_bronze_silver.json').read_text())
    p = pipeline['properties']
    assert pipeline['name'] == 'pl_taxi_landing_bronze_silver'
    assert p['activities'][:2] == original['activities']
    assert p['concurrency'] == 1
    assert {k:v for k,v in p['parameters'].items() if k != 'silver_job_id'} == original['parameters']
    silver = p['activities'][2]
    assert silver['dependsOn'] == [{'activity':'load_taxi_bronze','dependencyConditions':['Succeeded']}]
    assert silver['type'] == 'DatabricksJob'
    assert silver['typeProperties']['jobId'] == {'value':'@pipeline().parameters.silver_job_id','type':'Expression'}
    assert p['parameters']['silver_job_id']['defaultValue'] == '654018398720089'
    # Bronze currently writes a fixed dev catalog; silver must consume that same catalog.
    assert silver['typeProperties']['jobParameters'] == {'catalog':'chicago_taxi_dev'}
    assert silver['linkedServiceName']['referenceName'] == 'ls_databricks_serverless_dev'
    assert silver['policy']['retry'] == 0
    # Succeeded-only dependencies prevent publication after any upstream failure.
    names = set()
    for activity in p['activities']:
        assert activity['name'] not in names
        for dependency in activity['dependsOn']:
            assert dependency['activity'] in names
            assert dependency['dependencyConditions'] == ['Succeeded']
        names.add(activity['name'])
