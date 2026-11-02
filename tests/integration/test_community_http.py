"""One response example is sufficient to test the community HTTP adapter.

The separate functional tests enforce the production 77-area snapshot rule.
"""

import json
from urllib.parse import urlencode
from jsonschema import Draft202012Validator


def test_http_reference_response(source_fetch, source_registry, fixtures_dir):
    config = source_registry["community_areas"]
    url = config["source_url"] + "?" + urlencode(config["query_parameters"])
    rows = source_fetch("community_areas")(url)
    schema = json.loads((fixtures_dir / config["schema"]).read_text())
    Draft202012Validator(schema).validate(rows)
    assert len(rows) == 1
    assert rows[0]["area_num_1"] == "1"
    assert rows[0]["the_geom"]["type"] == "Polygon"
