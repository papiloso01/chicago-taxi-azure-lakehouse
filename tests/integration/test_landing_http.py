"""End-to-end HTTP extraction adapter using a loopback fixture API, never a live source."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from chicago_taxi.ingestion.taxi_api import request_rows
from chicago_taxi.ingestion.landing import ingest


def test_http_to_landing_manifest(tmp_path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            where = parse_qs(urlparse(self.path).query)['$where'][0]
            rows = [] if "trip_id > 'b'" in where else [{'trip_id':'a'}, {'trip_id':'b'}]
            payload = json.dumps(rows).encode()
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.end_headers()
            self.wfile.write(payload)
        def log_message(self,*args):pass
    server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
    worker = threading.Thread(target=server.serve_forever,daemon=True)
    worker.start()
    try:
        def fetch(url, token):
            return request_rows(f'http://127.0.0.1:{server.server_port}/?' + urlparse(url).query, token)
        result = ingest(tmp_path,'2025-01-01','2025-01-02',page_size=2,fetch=fetch)
        assert result['status'] == 'success' and result['extracted_rows'] == 2
        assert len(result['files']) == 1
    finally:
        server.shutdown()
        worker.join()
        server.server_close()
