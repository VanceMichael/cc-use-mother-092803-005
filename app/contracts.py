"""地质灾害响应 的输入输出约定。

请求由调用方（部门系统、值守台）构造，``request_id`` 是请求级幂等键：
同一请求因网络重试重复到达时，服务返回首次结果，不产生第二次副作用。

业务对象另有来源去重键（观测 ``source_ref``、回执 ``receipt_code``），
保证"上游重复推送 / 居民重复回复"都落到同一事件、同一名单项。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)

# ---- 预警等级（只升不降的全序）-----------------------------------------
LEVELS = ["blue", "yellow", "orange", "red"]
LEVEL_ORDER = {level: index for index, level in enumerate(LEVELS)}
LEVEL_NAMES = {"blue": "蓝色预警", "yellow": "黄色预警", "orange": "橙色预警", "red": "红色预警"}

# ---- 业务动作 -------------------------------------------------------------
# 基础数据
REGISTER_ZONE = "register_zone"            # 登记风险区
GRANT_AREA = "grant_area"                  # 部门授权某行政区域
UPSERT_RESIDENT = "upsert_resident"        # 登记/更新转移居民
PUT_RULE = "put_rule"                      # 配置预警规则
# 观测与研判
SUBMIT_OBSERVATION = "submit_observation"  # 上报雨量观测，触发研判
# 事件处置
ESCALATE_EVENT = "escalate_event"          # 人工升级（会商决定）
ISSUE_DIRECTIVE = "issue_directive"        # 下达处置指令
DISPATCH_TASK = "dispatch_task"            # 向责任组派单（自动生成转移名单）
ACK_TASK = "ack_task"                      # 责任组接单
COMPLETE_TASK = "complete_task"            # 责任组报处置完成
# 通知与回执
RECORD_RECEIPT = "record_receipt"          # 登记居民确认回执
RETRY_PENDING_NOTIFICATIONS = "retry_pending_notifications"  # 崩溃恢复：续发通知
CLOSE_EVENT = "close_event"                # 会商解除：关闭进行中的事件
# 查询
DASHBOARD = "dashboard"                    # 按风险区的值守视图
ZONE_DETAIL = "zone_detail"                # 单风险区明细（含升级履历/审计）
PENDING_WORK = "pending_work"              # 全区域待办视图

WRITE_ACTIONS = {
    REGISTER_ZONE, GRANT_AREA, UPSERT_RESIDENT, PUT_RULE,
    SUBMIT_OBSERVATION, ESCALATE_EVENT, ISSUE_DIRECTIVE,
    DISPATCH_TASK, ACK_TASK, COMPLETE_TASK, RECORD_RECEIPT,
    RETRY_PENDING_NOTIFICATIONS, CLOSE_EVENT,
}
READ_ACTIONS = {DASHBOARD, ZONE_DETAIL, PENDING_WORK}


class ServiceError(Exception):
    """业务拒绝。code 供调用方程序化处理，message 面向值班人员。"""

    def __init__(self, code: str, message: str, status: int = 422) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


@dataclass(frozen=True)
class Request:
    actor: str               # 操作人/系统账号
    action: str              # WRITE_ACTIONS / READ_ACTIONS 之一
    payload: dict[str, Any]
    request_id: str          # 请求级幂等键
    created_at: datetime = field(default_factory=_utcnow)


@dataclass
class Result:
    accepted: bool
    state: str
    message: str
    data: dict[str, Any] = field(default_factory=dict)
    code: str = "ok"

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted": self.accepted,
            "state": self.state,
            "message": self.message,
            "code": self.code,
            "data": dict(self.data),
        }


def validate_request(request: Request) -> None:
    if not request.actor or not request.action or not request.request_id:
        raise ValueError("请求缺少身份、动作或幂等键")
    if not isinstance(request.payload, dict):
        raise TypeError("请求数据必须是对象")
