"""Repository-wide, configurable locations for source test contracts."""

import json
from pathlib import Path
import pytest
from tests.support.requests import request_parameters


def pytest_addoption(parser):
    parser.addoption(
        "--adf-contract", default=None, help="Environment contract JSON for ADF exports"
    )
    parser.addoption("--fixtures-dir", default=str(Path(__file__).parent / "fixtures"))
    parser.addoption(
        "--delta-jars-dir", default=".tools/delta", help="Pinned Delta test JAR folder"
    )
    parser.addoption(
        "--wiremock-jar", default=None, help="Pinned standalone JAR (integration tests only)"
    )


@pytest.fixture(scope="session")
def fixtures_dir(pytestconfig):
    folder = Path(pytestconfig.getoption("--fixtures-dir")).resolve()
    if not (folder / "sources.json").is_file():
        raise pytest.UsageError(f"Missing source registry: {folder / 'sources.json'}")
    return folder


@pytest.fixture(scope="session")
def source_registry(fixtures_dir):
    registry = json.loads((fixtures_dir / "sources.json").read_text(encoding="utf-8"))
    for config in registry.values():
        config["query_parameters"] = request_parameters(config)
    return registry


@pytest.fixture
def adf_environment(pytestconfig, fixtures_dir):
    configured = pytestconfig.getoption("--adf-contract")
    path = Path(configured) if configured else fixtures_dir / "environments/default.json"
    return json.loads(path.read_text(encoding="utf-8"))


pytest_plugins = ["tests.support.fixtures"]
