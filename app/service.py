"""风险区预警与转移回执的状态机占位实现。"""
from dataclasses import dataclass
from threading import RLock
from .contracts import Request, Result, validate_request

@dataclass
class Record:
    key: str
    state: str = "draft"
    version: int = 0

class HazardResponseService:
    def __init__(self) -> None:
        self._records: dict[str, Record] = {}
        self._results: dict[str, Result] = {}
        self._lock = RLock()

    def handle(self, request: Request) -> Result:
        validate_request(request)
        with self._lock:
            previous = self._results.get(request.request_id)
            if previous is not None:
                return previous
            key = str(request.payload.get("key", "")).strip()
            if not key:
                result = Result(False, "rejected", "缺少业务键")
            else:
                record = self._records.setdefault(key, Record(key))
                if request.action == "open":
                    record.state = "open"
                    record.version += 1
                    result = Result(True, record.state, "已受理", {"key": key, "version": record.version})
                elif request.action == "close" and record.state == "open":
                    record.state = "closed"
                    record.version += 1
                    result = Result(True, record.state, "已关闭", {"key": key, "version": record.version})
                else:
                    result = Result(False, record.state, "当前状态不允许该动作", {"key": key, "version": record.version})
            self._results[request.request_id] = result
            return result
