"""One isolated WireMock process; reset request state for every test."""
import json
import subprocess
import time
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import Request, build_opener, ProxyHandler

# Loopback traffic must never pass through a developer or CI HTTP proxy.
urlopen = build_opener(ProxyHandler({})).open
import pytest
from chicago_taxi.ingestion.taxi_api import request_rows


def admin(base, path, method='GET'):
    with urlopen(Request(base + '/__admin/' + path, method=method), timeout=2) as response:
        return json.load(response) if method == 'GET' else None


@pytest.fixture(scope='session')
def wiremock_server(pytestconfig, fixtures_dir, tmp_path_factory):
    jar = pytestconfig.getoption('--wiremock-jar')
    if not jar or not Path(jar).is_file():
        raise pytest.UsageError('Integration tests require --wiremock-jar PATH; see tests/integration/README.md')
    folder = tmp_path_factory.mktemp('wiremock')
    log_path = folder / 'server.log'
    with log_path.open('w', encoding='utf-8') as log:
        process = subprocess.Popen(['java', '-jar', str(Path(jar).resolve()), '--port', '0',
            '--bind-address', '127.0.0.1', '--root-dir', str(fixtures_dir / 'wiremock')],
            stdout=log, stderr=subprocess.STDOUT)
        try:
            import re
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                text = log_path.read_text(encoding='utf-8')
                match = re.search(r'port:\s+(\d+)', text)
                if match:
                    base = 'http://127.0.0.1:' + match.group(1)
                    try:
                        admin(base, 'mappings')
                        break
                    except (URLError, TimeoutError):
                        pass
                if process.poll() is not None:
                    raise RuntimeError('WireMock exited: ' + text)
                time.sleep(.1)
            else:
                raise RuntimeError('WireMock startup timed out: ' + log_path.read_text())
            yield base
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()


@pytest.fixture
def wiremock(wiremock_server):
    admin(wiremock_server, 'requests', 'DELETE')
    yield wiremock_server
    unmatched = admin(wiremock_server, 'requests/unmatched')['requests']
    assert not unmatched, f'Unmatched WireMock requests: {unmatched}'


@pytest.fixture
def source_fetch(wiremock, source_registry, monkeypatch):
    monkeypatch.setattr("chicago_taxi.ingestion.taxi_api.urlopen", urlopen)
    def for_case(source, case='happy'):
        contract = source_registry[source]
        route = contract['cases'][case]
        def fetch(url, token=''):
            parts = urlsplit(url)
            expected = urlsplit(contract['source_url'])
            if (parts.scheme, parts.netloc, parts.path) != (expected.scheme, expected.netloc, expected.path):
                raise AssertionError(f'Unexpected endpoint for {source}: {url}')
            return request_rows(wiremock + route + ('?' + parts.query if parts.query else ''), token)
        return fetch
    return for_case
