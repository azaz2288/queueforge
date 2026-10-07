from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import uuid
from contextlib import closing, contextmanager
from pathlib import Path


class Conflict(ValueError):
    pass


class Queue:
    def __init__(self, path, clock=time.time):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        with closing(self.connect()) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
            CREATE TABLE IF NOT EXISTS jobs(
                id TEXT PRIMARY KEY, idempotency TEXT NOT NULL UNIQUE, fingerprint TEXT NOT NULL,
                task TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL,
                attempts INTEGER NOT NULL, max_attempts INTEGER NOT NULL, available REAL NOT NULL,
                token TEXT, worker TEXT, expires REAL, result TEXT, error TEXT,
                created REAL NOT NULL, updated REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS ready ON jobs(status,available,created);
            CREATE INDEX IF NOT EXISTS expiry ON jobs(status,expires);
            CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,
                time REAL NOT NULL,kind TEXT NOT NULL,detail TEXT NOT NULL);
            """)

    def connect(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=10000")
        return db

    @contextmanager
    def transaction(self):
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def now(self):
        now = float(self.clock())
        if not math.isfinite(now):
            raise ValueError("Nonfinite clock")
        return now

    @staticmethod
    def event(db, id, now, kind, detail=""):
        db.execute(
            "INSERT INTO events(job_id,time,kind,detail) VALUES (?,?,?,?)",
            (id, now, kind, detail),
        )

    def submit(self, key, task, payload, max_attempts=3):
        if not isinstance(key, str) or not 1 <= len(key) <= 100:
            raise ValueError("Idempotency key: 1–100 characters")
        if task not in ("text_stats", "sha256"):
            raise ValueError("Unsupported task; only text_stats / sha256")
        if not isinstance(payload, str) or len(payload.encode("utf-8")) > 65536:
            raise ValueError("Text payload max 64 KiB")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 10:
            raise ValueError("Attempts 1–10")
        fingerprint = hashlib.sha256(
            json.dumps([task, payload, max_attempts], ensure_ascii=False).encode()
        ).hexdigest()
        now = self.now()
        with self.transaction() as db:
            old = db.execute(
                "SELECT * FROM jobs WHERE idempotency=?", (key,)
            ).fetchone()
            if old:
                if old["fingerprint"] != fingerprint:
                    raise Conflict("Idempotency key reused with different content")
                return dict(old), False
            identifier = uuid.uuid4().hex
            db.execute(
                "INSERT INTO jobs(id,idempotency,fingerprint,task,payload,status,attempts,max_attempts,available,created,updated) VALUES (?,?,?,?,?,'pending',0,?,?,?,?)",
                (
                    identifier,
                    key,
                    fingerprint,
                    task,
                    payload,
                    max_attempts,
                    now,
                    now,
                    now,
                ),
            )
            self.event(db, identifier, now, "submitted")
            return dict(
                db.execute("SELECT * FROM jobs WHERE id=?", (identifier,)).fetchone()
            ), True

    def claim(self, worker, lease_seconds=30):
        if not isinstance(worker, str) or not 1 <= len(worker) <= 100:
            raise ValueError("Worker name required")
        if (
            not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or not 0.1 <= lease_seconds <= 300
        ):
            raise ValueError("Lease .1–300 seconds")
        now = self.now()
        with self.transaction() as db:
            for row in db.execute(
                "SELECT * FROM jobs WHERE status='running' AND expires<=?", (now,)
            ).fetchall():
                status = "dead" if row["attempts"] >= row["max_attempts"] else "pending"
                db.execute(
                    "UPDATE jobs SET status=?,available=?,token=NULL,worker=NULL,expires=NULL,error=?,updated=? WHERE id=?",
                    (status, now, "Lease expired", now, row["id"]),
                )
                self.event(db, row["id"], now, "expired", status)
            row = db.execute(
                "SELECT * FROM jobs WHERE status='pending' AND available<=? ORDER BY available,created,rowid LIMIT 1",
                (now,),
            ).fetchone()
            if not row:
                return None
            token = uuid.uuid4().hex
            db.execute(
                "UPDATE jobs SET status='running',attempts=attempts+1,token=?,worker=?,expires=?,updated=? WHERE id=?",
                (token, worker, now + lease_seconds, now, row["id"]),
            )
            self.event(db, row["id"], now, "claimed", worker)
            return dict(
                db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
            )

    @staticmethod
    def owned(db, id, token, now):
        row = db.execute("SELECT * FROM jobs WHERE id=?", (id,)).fetchone()
        if (
            not row
            or row["status"] != "running"
            or not isinstance(token, str)
            or row["token"] != token
            or row["expires"] <= now
        ):
            raise Conflict("Stale, expired or cancelled lease; result refused")
        return row

    def heartbeat(self, id, token, lease_seconds=30):
        if (
            not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or not 0.1 <= lease_seconds <= 300
        ):
            raise ValueError("Invalid lease duration")
        now = self.now()
        with self.transaction() as db:
            self.owned(db, id, token, now)
            db.execute(
                "UPDATE jobs SET expires=?,updated=? WHERE id=?",
                (now + lease_seconds, now, id),
            )
            self.event(db, id, now, "heartbeat")

    def finish(self, id, token, result):
        encoded = json.dumps(result, ensure_ascii=False, allow_nan=False)
        if len(encoded.encode()) > 65536:
            raise ValueError("Result max 64 KiB")
        now = self.now()
        with self.transaction() as db:
            self.owned(db, id, token, now)
            db.execute(
                "UPDATE jobs SET status='succeeded',result=?,token=NULL,worker=NULL,expires=NULL,updated=? WHERE id=?",
                (encoded, now, id),
            )
            self.event(db, id, now, "succeeded")

    def fail(self, id, token, error):
        now = self.now()
        with self.transaction() as db:
            row = self.owned(db, id, token, now)
            status = "dead" if row["attempts"] >= row["max_attempts"] else "pending"
            delay = min(60, 2 ** (row["attempts"] - 1))
            db.execute(
                "UPDATE jobs SET status=?,available=?,error=?,token=NULL,worker=NULL,expires=NULL,updated=? WHERE id=?",
                (status, now + delay, str(error)[:1000], now, id),
            )
            self.event(db, id, now, "failed", status)

    def cancel(self, id):
        now = self.now()
        with self.transaction() as db:
            row = db.execute("SELECT status FROM jobs WHERE id=?", (id,)).fetchone()
            if not row or row["status"] not in ("pending", "running"):
                raise Conflict("Only pending/running jobs can be cancelled")
            db.execute(
                "UPDATE jobs SET status='cancelled',token=NULL,worker=NULL,expires=NULL,updated=? WHERE id=?",
                (now, id),
            )
            self.event(db, id, now, "cancelled")

    def retry(self, id):
        now = self.now()
        with self.transaction() as db:
            row = db.execute("SELECT status FROM jobs WHERE id=?", (id,)).fetchone()
            if not row or row["status"] != "dead":
                raise Conflict("Only dead-letter jobs can be retried")
            db.execute(
                "UPDATE jobs SET status='pending',attempts=0,available=?,error=NULL,result=NULL,updated=? WHERE id=?",
                (now, now, id),
            )
            self.event(
                db,
                id,
                now,
                "manual_retry",
                "New attempt budget; previous events retained",
            )

    def list(self, limit=100):
        with closing(self.connect()) as db:
            return [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM jobs ORDER BY created DESC,rowid DESC LIMIT ?",
                    (limit,),
                )
            ]

    def history(self, id):
        with closing(self.connect()) as db:
            return [
                dict(r)
                for r in db.execute(
                    "SELECT * FROM events WHERE job_id=? ORDER BY seq", (id,)
                )
            ]

    def metrics(self):
        with closing(self.connect()) as db:
            return dict(
                db.execute(
                    "SELECT status,COUNT(*) FROM jobs GROUP BY status"
                ).fetchall()
            )


def execute(job):
    text = job["payload"]
    if job["task"] == "sha256":
        return {
            "sha256": hashlib.sha256(text.encode()).hexdigest(),
            "bytes": len(text.encode()),
        }
    if job["task"] == "text_stats":
        return {
            "lines": len(text.splitlines()),
            "words": len(text.split()),
            "characters": len(text),
            "bytes": len(text.encode()),
        }
    raise ValueError("Unknown task")


def work_once(queue, worker):
    job = queue.claim(worker)
    if not job:
        return False
    try:
        result = execute(job)
        queue.finish(job["id"], job["token"], result)
    except Conflict:
        pass  # Another lease owns the state; never write stale output.
    except Exception as exc:
        try:
            queue.fail(job["id"], job["token"], type(exc).__name__ + ": " + str(exc))
        except Conflict:
            pass
    return True
