"""命令行入口：启动服务、查看事件、解释请求变化。"""

from __future__ import annotations

import argparse
import json
from datetime import datetime

from .api import serve
from .engine import Orchestrator
from .store import EventStore


def _print(data: object) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="emergency-connectivity", description="应急通信保障编排服务")
    parser.add_argument("--store", help="事件日志 JSONL 路径；不填则仅使用内存")
    sub = parser.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="启动 HTTP API")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8080)

    p_explain = sub.add_parser("explain", help="解释某请求在每次变化后为何被保留/降级/切换/终止")
    p_explain.add_argument("request_code")
    p_explain.add_argument("--at", help="ISO-8601 评估时刻，默认当前时间")

    p_events = sub.add_parser("events", help="输出追加事件审计流")

    args = parser.parse_args(argv)
    store = EventStore(args.store) if args.store else EventStore()

    if args.command == "serve":
        serve(args.host, args.port, store)
    elif args.command == "explain":
        at = datetime.fromisoformat(args.at) if args.at else None
        _print(Orchestrator(store).explain(args.request_code, at))
    elif args.command == "events":
        _print({"events": [e.to_dict() for e in store.events()]})


if __name__ == "__main__":
    main()
