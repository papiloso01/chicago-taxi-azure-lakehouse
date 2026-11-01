"""Repository-wide, configurable locations for source test contracts."""
import json
from pathlib import Path
import pytest


def pytest_addoption(parser):
    parser.addoption('--fixtures-dir', default=str(Path(__file__).parent / 'fixtures'))
    parser.addoption('--wiremock-jar', default=None, help='Pinned standalone JAR (integration tests only)')


@pytest.fixture(scope='session')
def fixtures_dir(pytestconfig):
    folder = Path(pytestconfig.getoption('--fixtures-dir')).resolve()
    if not (folder / 'sources.json').is_file():
        raise pytest.UsageError(f'Missing source registry: {folder / "sources.json"}')
    return folder


@pytest.fixture(scope='session')
def source_registry(fixtures_dir):
    return json.loads((fixtures_dir / 'sources.json').read_text(encoding='utf-8'))
