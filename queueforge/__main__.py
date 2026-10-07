import argparse
import json
import time

from .queue import Queue, work_once


def main():
    parser = argparse.ArgumentParser(description="QueueForge durable lease queue")
    parser.add_argument("--db", default="data/queue.sqlite")
    sub = parser.add_subparsers(dest="command", required=True)
    worker = sub.add_parser("worker")
    worker.add_argument("--name", default="worker-1")
    worker.add_argument("--once", action="store_true")
    server = sub.add_parser("serve")
    server.add_argument("--port", type=int, default=8781)
    submit = sub.add_parser("submit")
    submit.add_argument("--key", required=True)
    submit.add_argument(
        "--task", choices=["sha256", "text_stats"], default="text_stats"
    )
    submit.add_argument("--text", required=True)
    sub.add_parser("list")
    args = parser.parse_args()
    queue = Queue(args.db)
    if args.command == "worker":
        try:
            while True:
                active = work_once(queue, args.name)
                if args.once:
                    break
                if not active:
                    time.sleep(0.5)
        except KeyboardInterrupt:
            pass
    elif args.command == "serve":
        from .server import serve

        serve(queue, args.port)
    elif args.command == "submit":
        print(
            json.dumps(
                queue.submit(args.key, args.task, args.text)[0], ensure_ascii=False
            )
        )
    else:
        print(json.dumps(queue.list(), ensure_ascii=False))


if __name__ == "__main__":
    main()
