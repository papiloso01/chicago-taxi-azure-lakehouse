"""Every registered source must have valid schemas, routes and response files."""
import json
from urllib.parse import urlsplit
import pytest
from jsonschema import Draft202012Validator, ValidationError


def test_registered_source_contracts(fixtures_dir, source_registry):
    mappings = [json.loads(path.read_text()) for path in (fixtures_dir / 'wiremock/mappings').glob('*.json')]
    assert source_registry and mappings
    for source, config in source_registry.items():
        assert urlsplit(config['source_url']).scheme == 'https'
        schema = json.loads((fixtures_dir / config['schema']).read_text())
        Draft202012Validator.check_schema(schema)
        validator = Draft202012Validator(schema)
        assert 'happy' in config['cases'], source
        for case, route in config['cases'].items():
            matching = [m for m in mappings if m['request']['urlPath'] == route]
            assert matching, (source, case)
            for mapping in matching:
                # Every case declares its limit/filter contract explicitly.
                assert mapping['request']['queryParameters']
                assert '$limit' in mapping['request']['queryParameters']
                payload = json.loads((fixtures_dir / 'wiremock/__files' / mapping['response']['bodyFileName']).read_text())
                if case == 'happy':
                    # Raw response contracts allow small examples and empty final pages.
                    # Production reference completeness is checked separately.
                    validator.validate(payload)
                elif case == 'invalid':
                    with pytest.raises(ValidationError):
                        validator.validate(payload)
