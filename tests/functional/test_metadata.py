import importlib.util
spec = importlib.util.spec_from_file_location("metadata", "tools/validate_metadata.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def test_source_definitions_validate():
    assert module.validate() == 3


import json
import shutil
from pathlib import Path
import pytest

@pytest.mark.parametrize('defect',['duplicate','missing_key','foreign_key'])
def test_bad_warehouse_metadata_rejected(tmp_path,defect):
    shutil.copytree('metadata',tmp_path/'metadata')
    path = tmp_path/'metadata'/'silver_tables.yml'
    data = json.loads(path.read_text())
    if defect == 'duplicate': data['tables'].append(data['tables'][0])
    elif defect == 'missing_key': data['tables'][0]['key'] = 'missing'
    else: data['foreign_keys'][0]['target'] = 'dim_taxi.missing'
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError): module.validate(tmp_path/'metadata')
