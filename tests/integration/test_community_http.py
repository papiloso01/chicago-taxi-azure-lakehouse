"""Community reference ingestion through real HTTP and file-backed responses."""
from chicago_taxi.geography.community_areas import extract, read_snapshot


def test_http_reference_to_validated_snapshot(tmp_path, source_fetch):
    folder = extract(tmp_path, fetch=source_fetch('community_areas'))
    assert read_snapshot(folder)[0]['rows'] == 77
