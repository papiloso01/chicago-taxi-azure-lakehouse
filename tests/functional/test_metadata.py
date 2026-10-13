import importlib.util
spec = importlib.util.spec_from_file_location("metadata", "tools/validate_metadata.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def test_source_definitions_validate():
    assert module.validate() == 3
