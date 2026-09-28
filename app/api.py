"""地质灾害响应 的轻量本地调用入口。"""
import json
import sys
from datetime import datetime
from .contracts import Request
from .service import HazardResponseService

def main() -> int:
    service = HazardResponseService()
    raw = sys.stdin.read().strip()
    if not raw:
        return 2
    item = json.loads(raw)
    request = Request(str(item.get("actor", "")), str(item.get("action", "")), dict(item.get("payload", {})), str(item.get("request_id", "")), datetime.utcnow())
    result = service.handle(request)
    print(json.dumps({"accepted": result.accepted, "state": result.state, "message": result.message, "data": result.data}, ensure_ascii=False))
    return 0 if result.accepted else 1

if __name__ == "__main__":
    raise SystemExit(main())
