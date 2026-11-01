import importlib.util

spec = importlib.util.spec_from_file_location("metadata", "tools/validate_metadata.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_source_definitions_validate():
    expected = yaml.safe_load(Path("metadata/sources.yml").read_text())["sources"]
    assert expected
    assert module.validate() == len(expected)


import json
import shutil
from pathlib import Path
import pytest
import yaml


@pytest.mark.parametrize("defect", ["duplicate", "missing_key", "foreign_key"])
def test_bad_warehouse_metadata_rejected(tmp_path, defect):
    shutil.copytree("metadata", tmp_path / "metadata")
    path = tmp_path / "metadata" / "silver_tables.yml"
    data = json.loads(path.read_text())
    if defect == "duplicate":
        data["tables"].append(data["tables"][0])
    elif defect == "missing_key":
        data["tables"][0]["key"] = "missing"
    else:
        data["foreign_keys"][0]["target"] = "dim_taxi.missing"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        module.validate(tmp_path / "metadata")


@pytest.mark.parametrize("defect", ["duplicate", "composite_key"])
def test_bad_gold_contract_rejected(tmp_path, defect):
    shutil.copytree("metadata", tmp_path / "metadata")
    path = tmp_path / "metadata" / "gold_tables.yml"
    data = json.loads(path.read_text())
    if defect == "duplicate":
        data["tables"].append(data["tables"][0])
    else:
        data["tables"][0]["key"] = ["trip_date", "missing_column"]
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        module.validate(tmp_path / "metadata")


def test_additional_source_does_not_require_a_fixed_source_count(tmp_path):
    shutil.copytree("metadata", tmp_path / "metadata")
    path = tmp_path / "metadata/sources.yml"
    data = yaml.safe_load(path.read_text())
    previous = len(data["sources"])
    data["sources"].append(
        dict(
            name="additional_api",
            endpoint="https://example.invalid/data",
            natural_key="id",
            status="planned",
        )
    )
    path.write_text(yaml.safe_dump(data))
    assert module.validate(tmp_path / "metadata") == previous + 1
