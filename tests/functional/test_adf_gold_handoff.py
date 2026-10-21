"""Guard the reviewed full pipeline's success-only dependency chain and job environment."""
import json
from pathlib import Path


def test_four_stage_pipeline_preserves_upstream_and_gates_gold():
    root=Path('adf/pipeline')
    original=json.loads((root/'pl_taxi_landing_bronze_silver.json').read_text())['properties']
    pipeline=json.loads((root/'pl_taxi_landing_bronze_silver_gold.json').read_text())
    p=pipeline['properties']
    assert pipeline['name']=='pl_taxi_landing_bronze_silver_gold'
    assert p['activities'][:3]==original['activities']
    assert {k:v for k,v in p['parameters'].items() if k!='gold_job_id'}==original['parameters']
    assert p['concurrency']==1
    gold=p['activities'][3]
    assert gold['type']=='DatabricksJob'
    assert gold['dependsOn']==[{'activity':'build_taxi_silver','dependencyConditions':['Succeeded']}]
    assert gold['typeProperties']['jobId']=={'value':'@pipeline().parameters.gold_job_id','type':'Expression'}
    assert p['parameters']['gold_job_id']['defaultValue']=='800504364677614'
    assert gold['typeProperties']['jobParameters']=={'catalog':'chicago_taxi_dev'}
    assert gold['linkedServiceName']['referenceName']=='ls_databricks_serverless_dev'
    names=['ingest_taxi_landing','load_taxi_bronze','build_taxi_silver','build_taxi_gold']
    assert [activity['name'] for activity in p['activities']]==names
    for index,activity in enumerate(p['activities']):
        assert activity['policy']['retry']==0
        if index:
            assert activity['dependsOn']==[{'activity':names[index-1],'dependencyConditions':['Succeeded']}]
        else:
            assert activity['dependsOn']==[]
