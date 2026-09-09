"""Exercise trace/reuse and multi-process accounting against real HTTP/1.1 sockets."""
import importlib.util
import json
import sys
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock, Thread
from types import SimpleNamespace

import pytest

scripts = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0,str(scripts))
spec = importlib.util.spec_from_file_location("timing",scripts/"contention_timing.py")
timing = importlib.util.module_from_spec(spec)
spec.loader.exec_module(timing)
sys.path.remove(str(scripts))


@pytest.mark.parametrize("mode,workers",[("cold",1),("warm",4)])
def test_real_connections_and_merged_accounting(tmp_path,mode,workers):
    mutex,seen = Lock(),set()
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        def log_message(self,*args):
            pass
        def reply(self,status,body):
            data=json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Length",str(len(data)))
            self.send_header("Server-Timing","app;dur=0.01")
            self.end_headers()
            self.wfile.write(data)
        def do_GET(self):
            self.reply(200,{"status":"live"})
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            with mutex:
                winner = not seen
                seen.add(self.headers["Idempotency-Key"])
            self.reply(201,{"hold_id":"test-winner"}) if winner else self.reply(409,{"code":"SEAT_BUSY"})
    server = ThreadingHTTPServer(("127.0.0.1",0),Handler)
    thread = Thread(target=server.serve_forever,daemon=True)
    thread.start()
    manifest = tmp_path/"private.json"
    manifest.write_text(json.dumps({"schema_version":1,"environment":"development",
        "expires_at":(datetime.now(UTC)+timedelta(hours=1)).isoformat(),
        "origin":f"http://127.0.0.1:{server.server_port}","show_ids":["test-show"],
        "viewer_tokens":["FAKE-TOKEN"]*100}))
    output = tmp_path/"result"
    try:
        timing.coordinate(SimpleNamespace(manifest=manifest,output=output,contenders=100,
                                           lanes=4,seat=1,mode=mode,workers=workers))
        result=json.loads((output/"summary.json").read_text())
        assert result["one_http_winner_pass"] and result["accounting_pass"]
        assert result["statuses"] == {"201":1,"409":99}
        assert len(seen)==100
        assert result["timing_ms"]["app_ms"]["count"]==100
        assert result["timing_ms"]["total_ms"]["p95"] >= result["timing_ms"]["client_ms"]["p95"]
        if mode=="warm":
            assert result["warm_reuse_pass"] and result["connect_attempts"]==0
        else:
            assert result["connect_attempts"]==4
        assert "FAKE-TOKEN" not in ''.join(p.read_text() for p in output.glob('*.json'))
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
