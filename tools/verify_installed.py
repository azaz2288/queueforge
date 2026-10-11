import importlib.resources
import subprocess
import sys
import tempfile
from pathlib import Path

import queueforge
from queueforge.queue import Queue, work_once

assert queueforge.__version__ == "0.2.0", queueforge.__version__
root = importlib.resources.files("queueforge")
for name in ("index.html", "app.js", "style.css"):
    assert root.joinpath("web", name).is_file(), name
with tempfile.TemporaryDirectory() as directory:
    queue = Queue(Path(directory) / "db.sqlite")
    queue.submit("installed", "sha256", "hello")
    assert work_once(queue, "installed", lease_seconds=1, heartbeat_interval=.1)
    assert queue.metrics() == {"succeeded": 1}
    subprocess.run([
        sys.executable, "-I", "-m", "queueforge", "--db", str(queue.path),
        "worker", "--once", "--lease-seconds", "1", "--heartbeat-interval", ".1",
    ], cwd=directory, check=True, timeout=15)
    # -I removes the checkout from sys.path; child process kill/recovery also
    # runs from the temporary directory against the newly installed wheel.
    subprocess.run([
        sys.executable, "-I", str(Path(__file__).resolve().parents[1] /
                                    "tests/test_worker_heartbeat.py"), "-v",
    ], cwd=directory, check=True, timeout=60)
print("Installed package durable worker and UI verified:", queueforge.__file__)
