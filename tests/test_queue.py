import concurrent.futures
import tempfile
import unittest
from pathlib import Path

from queueforge.queue import Conflict, Queue, work_once


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 1000
        self.path = Path(self.tmp.name) / "q.sqlite"
        self.q = Queue(self.path, lambda: self.now)

    def add(self, key="k", attempts=3):
        return self.q.submit(key, "text_stats", "hello\n世界", attempts)[0]

    def test_idempotent_and_conflicting_submit(self):
        a = self.add()
        b, created = self.q.submit("k", "text_stats", "hello\n世界")
        self.assertEqual(a["id"], b["id"])
        self.assertFalse(created)
        with self.assertRaises(Conflict):
            self.q.submit("k", "text_stats", "other")

    def test_concurrent_claim_no_double_owner(self):
        for i in range(20):
            self.add(str(i))
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(
                pool.map(
                    lambda i: Queue(self.path, lambda: self.now).claim(str(i)),
                    range(30),
                )
            )
        ids = [r["id"] for r in results if r]
        self.assertEqual(len(ids), 20)
        self.assertEqual(len(set(ids)), 20)

    def test_concurrent_idempotent_submission(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.add(), range(20)))
        self.assertEqual(len({r["id"] for r in results}), 1)

    def test_stale_token_cannot_commit_after_expiry_reclaim(self):
        self.add()
        old = self.q.claim("old", 1)
        self.now += 1
        new = self.q.claim("new", 10)
        with self.assertRaises(Conflict):
            self.q.finish(old["id"], old["token"], {"bad": True})
        self.q.finish(new["id"], new["token"], {"ok": True})
        self.assertEqual(self.q.list()[0]["status"], "succeeded")

    def test_heartbeat_requires_live_owner(self):
        self.add()
        job = self.q.claim("w", 1)
        self.now += 0.5
        self.q.heartbeat(job["id"], job["token"], 2)
        self.now += 1
        self.assertIsNone(self.q.claim("w2"))
        self.now += 2
        with self.assertRaises(Conflict):
            self.q.heartbeat(job["id"], job["token"])

    def test_retry_backoff_deadletter_manual_retry(self):
        self.add(attempts=2)
        job = self.q.claim("w")
        self.q.fail(job["id"], job["token"], "first")
        self.assertIsNone(self.q.claim("w"))
        self.now += 1
        job = self.q.claim("w")
        self.q.fail(job["id"], job["token"], "last")
        self.assertEqual(self.q.list()[0]["status"], "dead")
        self.q.retry(job["id"])
        self.assertEqual(self.q.list()[0]["attempts"], 0)
        self.assertEqual(self.q.history(job["id"])[-1]["kind"], "manual_retry")

    def test_cancel_fences_running_job(self):
        a = self.add()
        job = self.q.claim("w")
        self.q.cancel(a["id"])
        with self.assertRaises(Conflict):
            self.q.finish(a["id"], job["token"], {})
        self.assertEqual(self.q.list()[0]["status"], "cancelled")

    def test_crash_recovery_by_reopening_database(self):
        a = self.add()
        self.q.claim("crashed", 1)
        self.now += 2
        other = Queue(self.path, lambda: self.now)
        recovered = other.claim("new")
        self.assertEqual(recovered["id"], a["id"])
        self.assertEqual(recovered["attempts"], 2)

    def test_expired_final_attempt_dead(self):
        self.add(attempts=1)
        self.q.claim("w", 1)
        self.now += 2
        self.assertIsNone(self.q.claim("other"))
        self.assertEqual(self.q.metrics(), {"dead": 1})

    def test_actual_worker(self):
        self.add()
        self.assertTrue(work_once(self.q, "w"))
        self.assertEqual(self.q.list()[0]["status"], "succeeded")
        self.assertFalse(work_once(self.q, "w"))

    def test_no_arbitrary_execution_or_nonfinite_parameters(self):
        for task in ("shell", "download"):
            with self.assertRaises(ValueError):
                self.q.submit("key", task, "x")
        with self.assertRaises(ValueError):
            self.q.claim("w", float("nan"))
        self.add()
        job = self.q.claim("w")
        with self.assertRaises(ValueError):
            self.q.finish(job["id"], job["token"], float("inf"))

    def test_rollback_on_error(self):
        self.add()
        before = len(self.q.history(self.q.list()[0]["id"]))
        with self.assertRaises(Conflict):
            self.q.retry(self.q.list()[0]["id"])
        self.assertEqual(len(self.q.history(self.q.list()[0]["id"])), before)
