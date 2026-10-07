import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from queueforge.queue import Conflict, Queue


class ProcessRecoveryTests(unittest.TestCase):
    def test_real_worker_process_exit_after_claim_recovered_and_fenced(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "queue.sqlite"
            marker = Path(directory) / "token"
            q = Queue(path)
            job, _ = q.submit("crash", "text_stats", "test")
            code = "from queueforge.queue import Queue;from pathlib import Path;import os,sys;q=Queue(sys.argv[1]);job=q.claim('crashed',.1);Path(sys.argv[2]).write_text(job['token']);os._exit(17)"
            result = subprocess.run(
                [sys.executable, "-c", code, str(path), str(marker)], timeout=10
            )
            self.assertEqual(result.returncode, 17)
            old = marker.read_text()
            time.sleep(0.15)
            recovered = Queue(path).claim("restart")
            self.assertEqual(recovered["id"], job["id"])
            with self.assertRaises(Conflict):
                q.finish(job["id"], old, {"stale": True})
            q.finish(job["id"], recovered["token"], {"ok": True})
            self.assertEqual(q.metrics(), {"succeeded": 1})
