import importlib.resources
import tempfile
from pathlib import Path

import queueforge
from queueforge.queue import Queue, work_once

root = importlib.resources.files("queueforge")
for name in ("index.html", "app.js", "style.css"):
    assert root.joinpath("web", name).is_file(), name
with tempfile.TemporaryDirectory() as directory:
    queue = Queue(Path(directory) / "db.sqlite")
    queue.submit("installed", "sha256", "hello")
    assert work_once(queue, "installed")
    assert queue.metrics() == {"succeeded": 1}
print("Installed package durable worker and UI verified:", queueforge.__file__)
