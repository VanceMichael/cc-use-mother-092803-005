"""地质灾害响应 的本地调用入口。

用法::

    # 单条请求（JSON 对象从 stdin 读入）
    echo '{"actor":"cq01","action":"dashboard","payload":{},"request_id":"q1"}' \
        | python3 -m app.api --data response.db

    # 批量：JSON 数组，或 {"requests": [...]}，或每行一条 JSON（NDJSON）
    cat requests.jsonl | python3 -m app.api --data response.db

    # 进程重启后续办未送达的转移通知
    python3 -m app.api --data response.db --recover --recover-actor system

数据默认保存在 --db 指定的 SQLite 文件；不给定时落到环境变量
``HAZARD_DB``，再没有则用内存库（进程结束即清空，仅用于试用）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from .contracts import RETRY_PENDING_NOTIFICATIONS, Request, ServiceError
from .service import HazardResponseService


def _load_requests(raw: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # NDJSON：每行一条请求
        parsed = [json.loads(line) for line in raw.splitlines() if line.strip()]
    if isinstance(parsed, dict) and "requests" in parsed:
        parsed = parsed["requests"]
    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        raise ValueError("输入必须是请求对象、请求数组或 NDJSON")
    return parsed


def _to_request(item: dict[str, Any]) -> Request:
    return Request(
        actor=str(item.get("actor", "")),
        action=str(item.get("action", "")),
        payload=dict(item.get("payload", {})),
        request_id=str(item.get("request_id", "")),
    )


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="地质灾害响应后端 CLI")
    parser.add_argument("--db", default=os.environ.get("HAZARD_DB", ":memory:"),
                        help="SQLite 数据库文件（默认读 HAZARD_DB，否则内存库）")
    parser.add_argument("--recover", action="store_true",
                        help="启动时续发所有 pending 转移通知")
    parser.add_argument("--recover-actor", default="system",
                        help="续办动作的操作人（默认 system）")
    parser.add_argument("--recover-id", default=None,
                        help="续办请求的幂等键（默认按时间生成）")
    args = parser.parse_args(argv)

    raw = sys.stdin.read()
    try:
        items = _load_requests(raw)
    except (ValueError, json.JSONDecodeError) as exc:
        print(json.dumps({"accepted": False, "code": "bad_input",
                          "message": f"输入解析失败：{exc}"}, ensure_ascii=False))
        return 2

    service = HazardResponseService(args.db)
    outputs: list[dict[str, Any]] = []
    exit_code = 0

    if args.recover:
        import uuid
        rid = args.recover_id or f"recover-{uuid.uuid4().hex[:12]}"
        try:
            result = service.handle(Request(
                args.recover_actor, RETRY_PENDING_NOTIFICATIONS, {}, rid))
            outputs.append(result.to_dict())
        except ServiceError as exc:
            outputs.append({"accepted": False, "code": exc.code,
                            "message": exc.message})
            exit_code = 1

    for index, item in enumerate(items):
        try:
            result = service.handle(_to_request(item))
            outputs.append(result.to_dict())
            if not result.accepted:
                exit_code = 1
        except ServiceError as exc:
            outputs.append({"accepted": False, "code": exc.code,
                            "message": exc.message,
                            "request_id": str(item.get("request_id", ""))})
            exit_code = 1
        except (ValueError, TypeError) as exc:
            outputs.append({"accepted": False, "code": "invalid_request",
                            "message": str(exc),
                            "request_id": str(item.get("request_id", ""))})
            exit_code = 1

    if len(outputs) == 1:
        print(json.dumps(outputs[0], ensure_ascii=False))
    else:
        print(json.dumps({"results": outputs}, ensure_ascii=False))
    return exit_code


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
