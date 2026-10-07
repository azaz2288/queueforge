import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        env = os.environ.copy()
        command = [
            sys.executable,
            "-m",
            "queueforge",
            "--db",
            str(Path(self.tmp.name) / "q.sqlite"),
            "serve",
            "--port",
            str(self.port),
        ]
        self.process = subprocess.Popen(
            command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env
        )
        self.addCleanup(self.stop)
        for _ in range(100):
            with socket.socket() as probe:
                if probe.connect_ex(("127.0.0.1", self.port)) == 0:
                    break
            if self.process.poll() is not None:
                self.fail("Server exited")
            time.sleep(0.03)
        else:
            self.fail("Server did not bind")

    def stop(self):
        self.process.terminate()
        self.process.wait(timeout=10)

    def request(self, path="/", data=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            raw = json.dumps(data) if data is not None else None
            connection.request(
                "POST" if data is not None else "GET",
                path,
                raw,
                {"Content-Type": "application/json", **(headers or {})},
            )
            response = connection.getresponse()
            body = response.read()
            return response.status, body, response.getheader("Content-Security-Policy")
        finally:
            connection.close()

    def test_host_origin_and_static_boundaries(self):
        code, body, csp = self.request()
        self.assertEqual(code, 200)
        self.assertIn(b"QueueForge", body)
        self.assertIn("script-src 'self'", csp)
        self.assertEqual(
            self.request("/api/jobs", headers={"Host": "evil.invalid"})[0], 403
        )
        self.assertEqual(
            self.request("/api/jobs", headers={"Origin": "https://evil.invalid"})[0],
            403,
        )
        self.assertEqual(self.request("/../../README.md")[0], 404)

    def test_submit_work_idempotency_and_token_privacy(self):
        data = {"key": "http", "task": "sha256", "text": "hello"}
        self.assertEqual(self.request("/api/jobs", data)[0], 201)
        self.assertEqual(self.request("/api/jobs", data)[0], 200)
        data["text"] = "changed"
        self.assertEqual(self.request("/api/jobs", data)[0], 409)
        self.assertEqual(self.request("/api/work", {})[0], 200)
        rows = json.loads(self.request("/api/jobs")[1])["jobs"]
        self.assertEqual(rows[0]["status"], "succeeded")
        self.assertNotIn("token", rows[0])
        self.assertEqual(
            len(json.loads(self.request("/api/events/" + rows[0]["id"])[1])), 3
        )
