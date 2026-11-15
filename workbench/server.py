"""HTTP 服务：JSON API + 静态网页。仅依赖标准库。

运行：python3 -m workbench.server --db workbench.db --port 8000
"""

from __future__ import annotations

import argparse
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

from .core import (Workbench, IngestError, NotFoundError, ConflictError)

WEB_DIR = Path(__file__).resolve().parent.parent / "web"


def _json_bytes(obj) -> bytes:
    return json.dumps(obj, ensure_ascii=False, indent=None).encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    server_version = "Workbench/0.1"
    wb: Workbench = None  # 由 serve() 注入

    # ---------------- 基础 ----------------

    def log_message(self, fmt, *args):  # 静默默认日志，保留错误
        pass

    def _send(self, code: int, body: bytes, ctype: str = "application/json; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, code: int, obj):
        self._send(code, _json_bytes(obj))

    def _error(self, code: int, msg: str):
        self._json(code, {"error": msg})

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _read_json(self) -> dict:
        raw = self._read_body()
        try:
            return json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise IngestError("请求体不是合法 JSON")

    def _handle(self, method: str):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        wb = self.wb
        try:
            # ---------- 静态 ----------
            if method == "GET" and path == "/":
                return self._static("index.html", "text/html; charset=utf-8")
            if method == "GET" and path.startswith("/static/"):
                name = path[len("/static/"):]
                if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
                    return self._error(400, "bad path")
                ctype = {"js": "text/javascript; charset=utf-8",
                         "css": "text/css; charset=utf-8",
                         "html": "text/html; charset=utf-8"}.get(
                             name.rsplit(".", 1)[-1], "application/octet-stream")
                return self._static(name, ctype)

            # ---------- API ----------
            if method == "GET" and path == "/api/state":
                return self._json(200, wb.state())
            if method == "GET" and path == "/api/packages":
                return self._json(200, {"packages": wb.list_packages()})
            m = re.fullmatch(r"/api/packages/([^/]+)", path)
            if method == "GET" and m:
                return self._json(200, wb.get_package(m.group(1)))
            if method == "POST" and path == "/api/packages":
                blob = self._read_body()
                if not blob:
                    return self._error(400, "空请求体")
                source = self.headers.get("X-Source", "sender")
                return self._json(200, wb.ingest_package(blob, source=source))
            if method == "GET" and path == "/api/rules":
                return self._json(200, {"rules": [
                    {**r, "spec": None} for r in wb.state()["rules"]]})
            if method == "POST" and path == "/api/rules":
                body = self._read_json()
                return self._json(200, wb.register_rule(
                    body.get("spec", ""), body.get("note", "")))
            if method == "GET" and path == "/api/snapshots":
                return self._json(200, {"snapshots": wb.state()["snapshots"]})
            m = re.fullmatch(r"/api/snapshots/(\d+)/rows", path)
            if method == "GET" and m:
                return self._json(200, wb.snapshot_rows(
                    int(m.group(1)),
                    limit=int(qs.get("limit", [200])[0]),
                    offset=int(qs.get("offset", [0])[0]),
                    status=qs.get("status", [None])[0],
                    q=qs.get("q", [None])[0]))
            if method == "POST" and path == "/api/replays":
                body = self._read_json()
                run_id = wb.start_replay(body["rule_version"])
                return self._json(200, {"run_id": run_id})
            if method == "GET" and path == "/api/replays":
                return self._json(200, {"replays": wb.state()["replays"]})
            m = re.fullmatch(r"/api/replays/(\d+)/diff", path)
            if method == "GET" and m:
                return self._json(200, wb.diff_replay(int(m.group(1))))
            m = re.fullmatch(r"/api/replays/(\d+)/preview", path)
            if method == "GET" and m:
                return self._json(200, wb.preview_switch(int(m.group(1))))
            m = re.fullmatch(r"/api/replays/(\d+)/publish", path)
            if method == "POST" and m:
                body = self._read_json()
                return self._json(200, wb.publish(
                    int(m.group(1)),
                    expected_snapshot_id=int(body["expected_snapshot_id"]),
                    expected_high_water=body.get("expected_high_water")))
            m = re.fullmatch(r"/api/replays/(\d+)/interrupt", path)
            if method == "POST" and m:
                wb.interrupt_replay(int(m.group(1)))
                return self._json(200, {"ok": True})
            m = re.fullmatch(r"/api/replays/(\d+)/resume", path)
            if method == "POST" and m:
                wb.resume_replay(int(m.group(1)))
                return self._json(200, {"ok": True})
            if method == "POST" and path == "/api/rollback":
                body = self._read_json()
                return self._json(200, wb.rollback(
                    to_snapshot_id=int(body["to_snapshot_id"]),
                    expected_snapshot_id=int(body["expected_snapshot_id"]),
                    reason=body.get("reason", ""),
                    expected_high_water=body.get("expected_high_water")))
            m = re.fullmatch(r"/api/records/([^/]+)/trace", path)
            if method == "GET" and m:
                return self._json(200, wb.trace_record(m.group(1)))
            if method == "GET" and path == "/api/events":
                return self._json(200, {"events": wb.list_events()})
            return self._error(404, f"未知路由: {method} {path}")
        except IngestError as e:
            return self._error(400, str(e))
        except NotFoundError as e:
            return self._error(404, str(e))
        except ConflictError as e:
            return self._error(409, str(e))
        except BrokenPipeError:
            pass
        except Exception as e:  # 未预期错误：500 + 审计可见性
            import traceback
            traceback.print_exc()
            return self._error(500, f"内部错误: {e}")

    def _static(self, name: str, ctype: str):
        p = WEB_DIR / name
        if not p.is_file():
            return self._error(404, "not found")
        self._send(200, p.read_bytes(), ctype)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")


def serve(db_path: str, host: str, port: int) -> ThreadingHTTPServer:
    wb = Workbench.open(db_path)
    Handler.wb = wb
    httpd = ThreadingHTTPServer((host, port), Handler)
    return httpd


def main():
    ap = argparse.ArgumentParser(description="接收工作台服务")
    ap.add_argument("--db", default="workbench.db")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8000)
    args = ap.parse_args()
    httpd = serve(args.db, args.host, args.port)
    print(f"接收工作台已启动: http://{args.host}:{args.port}  (db={args.db})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n停止。")


if __name__ == "__main__":
    main()
