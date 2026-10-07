import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .queue import Conflict, work_once


def serve(queue, port):
    web = Path(__file__).parent / "web"

    class Handler(BaseHTTPRequestHandler):
        def reply(self, code, value, kind="application/json"):
            raw = (
                value
                if isinstance(value, bytes)
                else json.dumps(value, ensure_ascii=False, allow_nan=False).encode()
            )
            self.send_response(code)
            self.send_header("Content-Type", kind + "; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self'; style-src 'self'; object-src 'none'; frame-ancestors 'none'",
            )
            self.end_headers()
            self.wfile.write(raw)

        def allowed(self):
            if self.headers.get("Host") not in (
                f"127.0.0.1:{port}",
                f"localhost:{port}",
            ):
                self.reply(403, {"error": "Loopback Host required"})
                return False
            origin = self.headers.get("Origin")
            if (
                origin
                and origin
                not in (f"http://127.0.0.1:{port}", f"http://localhost:{port}")
            ) or self.headers.get("Sec-Fetch-Site") == "cross-site":
                self.reply(403, {"error": "Cross-site operation refused"})
                return False
            return True

        def do_GET(self):
            if not self.allowed():
                return
            if self.path == "/api/jobs":
                rows = queue.list()
                for row in rows:
                    row.pop("token", None)
                self.reply(200, {"jobs": rows, "metrics": queue.metrics()})
                return
            if self.path.startswith("/api/events/"):
                self.reply(200, queue.history(self.path.split("/")[-1]))
                return
            files = {
                "/": ("index.html", "text/html"),
                "/app.js": ("app.js", "text/javascript"),
                "/style.css": ("style.css", "text/css"),
            }
            if self.path not in files:
                self.reply(404, {"error": "Not found"})
                return
            name, kind = files[self.path]
            self.reply(200, (web / name).read_bytes(), kind)

        def do_POST(self):
            if not self.allowed():
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= length <= 80000:
                    raise ValueError("Request max 80000 bytes")
                data = json.loads(self.rfile.read(length))
                if self.path == "/api/jobs":
                    row, created = queue.submit(
                        data["key"],
                        data["task"],
                        data["text"],
                        data.get("max_attempts", 3),
                    )
                    row.pop("token", None)
                    self.reply(
                        201 if created else 200, {"job": row, "created": created}
                    )
                    return
                if self.path == "/api/work":
                    self.reply(200, {"worked": work_once(queue, "browser-demo")})
                    return
                if self.path == "/api/cancel":
                    queue.cancel(data["id"])
                    self.reply(200, {"ok": True})
                    return
                if self.path == "/api/retry":
                    queue.retry(data["id"])
                    self.reply(200, {"ok": True})
                    return
                self.reply(404, {"error": "Not found"})
            except Conflict as exc:
                self.reply(409, {"error": str(exc)})
            except (ValueError, TypeError, KeyError) as exc:
                self.reply(400, {"error": str(exc)})

        def log_message(self, *args):
            pass

    print(f"QueueForge http://127.0.0.1:{port}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
