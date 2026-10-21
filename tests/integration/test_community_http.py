"""Real HTTP adapter against a fixture community API; no live service required in CI."""
import json
import threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import parse_qs,urlparse
from chicago_taxi.ingestion.taxi_api import request_rows
from chicago_taxi.geography.community_areas import extract,read_snapshot


def test_http_reference_to_validated_snapshot(tmp_path):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            assert parse_qs(urlparse(self.path).query)['$limit']==['1000']
            data=[]
            for area in range(1,78):
                x=-88+area/1000
                data.append(dict(area_num_1=str(area),community=f'AREA {area}',the_geom=dict(type='Polygon',
                    coordinates=[[[x,41.7],[x+.0005,41.7],[x+.0005,41.701],[x,41.701],[x,41.7]]])))
            self.send_response(200);self.send_header('Content-Type','application/json');self.end_headers()
            self.wfile.write(json.dumps(data).encode())
        def log_message(self,*args): pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        def fetch(url,token): return request_rows(f'http://127.0.0.1:{server.server_port}/?'+urlparse(url).query,token)
        folder=extract(tmp_path,fetch=fetch)
        assert read_snapshot(folder)[0]['rows']==77
    finally:
        server.shutdown();thread.join();server.server_close()
