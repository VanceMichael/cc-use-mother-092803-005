"""地质灾害响应核心服务。

一条请求的处理边界：

    inbox 幂等检查 -> 校验/授权 -> 事务内落库 -> 事务外尽力送达通知

* 同一 ``request_id`` 重试：直接回放首次结果，无任何重复副作用；
* 不同请求携带同一上游键（观测 ``source_ref``、回执 ``receipt_code``）：
  识别为重复送达，落到原事件/原名单，不再触发升级或通知；
* 升级自动作废旧指令（superseded），新指令派新单，名单沿事件接续：
  已确认户保持确认且不再通知，未确认户继续等待，杜绝同等级重复通知；
* 通知在事务提交后尽力发送，进程被杀时未发项仍是 pending，
  重启后用 retry_pending_notifications 续办，全部状态由 SQLite 承载。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from threading import RLock
from typing import Any, Callable, Iterable

from . import rules as rule_engine
from .contracts import (
    LEVELS, LEVEL_NAMES, LEVEL_ORDER, READ_ACTIONS, WRITE_ACTIONS,
    ACK_TASK, CLOSE_EVENT, COMPLETE_TASK, DASHBOARD, DISPATCH_TASK,
    ESCALATE_EVENT, GRANT_AREA, ISSUE_DIRECTIVE, PUT_RULE,
    RECORD_RECEIPT, REGISTER_ZONE, RETRY_PENDING_NOTIFICATIONS,
    SUBMIT_OBSERVATION, UPSERT_RESIDENT, ZONE_DETAIL,
    Request, Result, ServiceError, validate_request,
)
from .storage import Store, area_covers

Notifier = Callable[[str, dict[str, Any]], bool]


def _default_notifier(task_id: str, item: dict[str, Any]) -> bool:
    """默认通知通道：假定提交后即可送达。测试可替换为会失败的通道。"""
    return True


class HazardResponseService:
    def __init__(self, store: Store | str = ":memory:",
                 notifier: Notifier | None = None) -> None:
        self.store = store if isinstance(store, Store) else Store(store)
        self._notifier = notifier or _default_notifier
        self._lock = RLock()

    def close(self) -> None:
        self.store.close()

    # ---- 入口 ------------------------------------------------------------
    def handle(self, request: Request) -> Result:
        validate_request(request)
        if request.action not in WRITE_ACTIONS | READ_ACTIONS:
            raise ServiceError("unknown_action", f"未知动作 {request.action}", 400)
        with self._lock:
            if request.action in READ_ACTIONS:
                return self._dispatch_read(request)
            if request.action == RETRY_PENDING_NOTIFICATIONS:
                # 该动作自行按行提交（每送达一户即落盘），避免进程中断后重复通知
                cached = self.store.inbox_get(request.request_id)
                if cached is not None:
                    return Result(**cached)
                result = self._dispatch_write(request)
                self._store_inbox(request, result)
                return self._after_write(result)
            with self.store.transaction():
                cached = self.store.inbox_get(request.request_id)
                if cached is not None:
                    return Result(**cached)
                result = self._dispatch_write(request)
                persisted = result.to_dict()
                # 内部的"事务后续发通知"标记不入库：重放只回读状态，
                # 是否真的重发由名单中仍为 pending 的项决定，不会重复通知
                persisted["data"].pop("_flush_notifications", None)
                self.store.inbox_put(
                    request.request_id, request.action,
                    json.dumps(persisted, ensure_ascii=False),
                )
            return self._after_write(result)

    def _store_inbox(self, request: Request, result: Result) -> None:
        with self.store.transaction():
            self.store.inbox_put(
                request.request_id, request.action,
                json.dumps(result.to_dict(), ensure_ascii=False),
            )

    def _after_write(self, result: Result) -> Result:
        flush = result.data.pop("_flush_notifications", None)
        if result.accepted and flush:
            self._flush_notifications(flush)
        return result

    # ---- 读视图 ----------------------------------------------------------
    def _dispatch_read(self, request: Request) -> Result:
        if request.action == DASHBOARD:
            return self._dashboard(request)
        if request.action == ZONE_DETAIL:
            return self._zone_detail(request)
        return self._pending_work(request)

    def _visible_zones(self, actor: str) -> list[Any]:
        zones = self.store.list_zones()
        if self.store.is_admin(actor):
            return zones
        granted = self.store.granted_areas(actor)
        return [z for z in zones
                if any(area_covers(a, z["admin_area"]) for a in granted)]

    def _dashboard(self, request: Request) -> Result:
        rows = []
        for zone in self._visible_zones(request.actor):
            event = self.store.get_open_event(zone["zone_id"])
            if event is None:
                rows.append({
                    "zone_id": zone["zone_id"], "name": zone["name"],
                    "admin_area": zone["admin_area"], "status": "calm",
                })
                continue
            rows.append(self._event_summary(zone, event))
        return Result(True, "ok", "值守视图", {"zones": rows})

    def _zone_detail(self, request: Request) -> Result:
        zone = self._require_zone(request.payload.get("zone_id"))
        self._require_area(request.actor, zone)
        event = self.store.get_open_event(zone["zone_id"])
        detail: dict[str, Any] = {
            "zone_id": zone["zone_id"], "name": zone["name"],
            "admin_area": zone["admin_area"],
        }
        if event is not None:
            detail["event"] = self._event_summary(zone, event, roster=True)
            detail["escalations"] = [
                self._level_dict(r) for r in self.store.level_history(event["event_id"])
            ]
        detail["audit"] = [
            {"at": r["created_at"], "actor": r["actor"], "action": r["action"],
             "summary": r["summary"]}
            for r in self.store.audit_list(zone_id=zone["zone_id"])
        ]
        return Result(True, "ok", "风险区明细", detail)

    def _pending_work(self, request: Request) -> Result:
        items: list[dict[str, Any]] = []
        for zone in self._visible_zones(request.actor):
            event = self.store.get_open_event(zone["zone_id"])
            if event is None:
                continue
            summary = self._event_summary(zone, event)
            for pending in summary["pending_actions"]:
                items.append({"zone_id": zone["zone_id"], "name": zone["name"],
                              "event_id": event["event_id"], **pending})
        return Result(True, "ok", "全区域待办", {"items": items})

    def _event_summary(self, zone: Any, event: Any, roster: bool = False) -> dict[str, Any]:
        active_directive = self.store.active_directive(event["event_id"])
        task = (self.store.task_for_directive(active_directive["directive_id"])
                if active_directive is not None else None)
        stats = self.store.roster_stats(task["task_id"]) if task else {
            "total": 0, "confirmed": 0, "unconfirmed": 0, "pending_send": 0}
        pending_actions: list[dict[str, Any]] = []
        if active_directive is None:
            latest = self.store.latest_directive(event["event_id"])
            if latest is not None and latest["status"] == "completed":
                pending_actions.append({
                    "kind": "awaiting_close",
                    "directive_id": latest["directive_id"],
                    "detail": "处置已完成，等待会商解除、关闭事件",
                })
            else:
                pending_actions.append({
                    "kind": "awaiting_directive",
                    "detail": f"当前{LEVEL_NAMES[event['current_level']]}尚无生效指令，等待下达",
                })
        elif active_directive["status"] == "pending":
            pending_actions.append({
                "kind": "awaiting_dispatch", "directive_id": active_directive["directive_id"],
                "detail": f"指令《{active_directive['title']}》尚未派给责任组",
            })
        if task is not None and task["status"] == "dispatched":
            pending_actions.append({
                "kind": "awaiting_ack", "task_id": task["task_id"],
                "dept_id": task["dept_id"],
                "detail": "责任组尚未接单",
            })
        if task is not None and task["status"] == "acked":
            pending_actions.append({
                "kind": "awaiting_completion", "task_id": task["task_id"],
                "dept_id": task["dept_id"],
                "detail": "处置进行中，尚未反馈完成",
            })
        if stats["pending_send"]:
            pending_actions.append({
                "kind": "awaiting_notification", "count": stats["pending_send"],
                "detail": f"{stats['pending_send']} 户转移通知尚未送达",
            })
        unconfirmed = stats["unconfirmed"] - stats["pending_send"]
        if unconfirmed > 0:
            pending_actions.append({
                "kind": "awaiting_confirmation", "count": unconfirmed,
                "detail": f"{unconfirmed} 户已通知未确认",
            })
        escalations = [self._level_dict(r)
                       for r in self.store.level_history(event["event_id"])]
        summary = {
            "zone_id": zone["zone_id"], "name": zone["name"],
            "admin_area": zone["admin_area"], "status": "active",
            "event_id": event["event_id"],
            "level": event["current_level"],
            "level_name": LEVEL_NAMES[event["current_level"]],
            "opened_by": event["opened_by"], "opened_at": event["opened_at"],
            "directive": None if active_directive is None else {
                "directive_id": active_directive["directive_id"],
                "title": active_directive["title"],
                "status": active_directive["status"],
                "level": active_directive["level"],
                "issued_by": active_directive["issued_by"],
                "issued_at": active_directive["issued_at"],
            },
            "task": None if task is None else {
                "task_id": task["task_id"], "dept_id": task["dept_id"],
                "status": task["status"], "dispatched_by": task["dispatched_by"],
                "dispatched_at": task["dispatched_at"],
                "acked_by": task["acked_by"], "completed_by": task["completed_by"],
                "carry_of_task_id": task["carry_of_task_id"],
            },
            "residents": {"total": stats["total"], "confirmed": stats["confirmed"],
                          "unconfirmed": stats["unconfirmed"],
                          "pending_notification": stats["pending_send"]},
            "pending_actions": pending_actions,
            "escalations": escalations,
        }
        if roster and task is not None:
            summary["roster"] = [{
                "resident_id": r["resident_id"],
                "notification_status": r["notification_status"],
                "receipt_status": r["receipt_status"],
                "confirmed_at": r["confirmed_at"],
            } for r in self.store.roster_for_task(task["task_id"])]
        return summary

    @staticmethod
    def _level_dict(row: Any) -> dict[str, Any]:
        return {
            "from_level": row["from_level"], "to_level": row["to_level"],
            "to_level_name": LEVEL_NAMES[row["to_level"]],
            "reason": row["reason"], "rule_id": row["rule_id"],
            "metric": row["metric"], "observed_value": row["observed_value"],
            "threshold": row["threshold"], "source_ref": row["source_ref"],
            "by": row["actor"], "at": row["created_at"],
        }

    # ---- 写动作 ----------------------------------------------------------
    def _dispatch_write(self, request: Request) -> Result:
        action = request.action
        if action == REGISTER_ZONE:
            return self._register_zone(request)
        if action == GRANT_AREA:
            return self._grant_area(request)
        if action == UPSERT_RESIDENT:
            return self._upsert_resident(request)
        if action == PUT_RULE:
            return self._put_rule(request)
        if action == SUBMIT_OBSERVATION:
            return self._submit_observation(request)
        if action == ESCALATE_EVENT:
            return self._escalate(request)
        if action == ISSUE_DIRECTIVE:
            return self._issue_directive(request)
        if action == DISPATCH_TASK:
            return self._dispatch_task(request)
        if action == ACK_TASK:
            return self._task_transition(request, "acked")
        if action == COMPLETE_TASK:
            return self._task_transition(request, "completed")
        if action == RECORD_RECEIPT:
            return self._record_receipt(request)
        if action == RETRY_PENDING_NOTIFICATIONS:
            return self._retry_notifications(request)
        if action == CLOSE_EVENT:
            return self._close_event(request)
        raise ServiceError("unknown_action", f"未知动作 {action}", 400)

    def _register_zone(self, request: Request) -> Result:
        self._require_admin(request.actor)
        zone_id = _text(request.payload, "zone_id")
        name = _text(request.payload, "name")
        admin_area = _text(request.payload, "admin_area").rstrip("/")
        with self.store.transaction():
            created = self.store.insert_zone(zone_id, name, admin_area, request.actor)
            if not created:
                raise ServiceError("zone_exists", f"风险区 {zone_id} 已存在", 409)
            self.store.audit(request.request_id, request.actor, REGISTER_ZONE,
                             f"登记风险区 {name}（{admin_area}）", zone_id=zone_id)
        return Result(True, "registered", "风险区已登记", {"zone_id": zone_id})

    def _grant_area(self, request: Request) -> Result:
        self._require_admin(request.actor)
        dept_id = _text(request.payload, "dept_id")
        admin_area = _text(request.payload, "admin_area").rstrip("/")
        dept_name = str(request.payload.get("dept_name", dept_id))
        members = request.payload.get("members", [])
        if not isinstance(members, list):
            raise ServiceError("invalid_payload", "members 必须是账号列表")
        with self.store.transaction():
            self.store.upsert_department(dept_id, dept_name)
            self.store.grant_area(dept_id, admin_area, request.actor)
            for actor in members:
                self.store.add_member(dept_id, str(actor))
            self.store.audit(request.request_id, request.actor, GRANT_AREA,
                             f"授权 {dept_name} 负责 {admin_area}，成员 {members}")
        return Result(True, "granted", "区域授权已生效",
                      {"dept_id": dept_id, "admin_area": admin_area})

    def _upsert_resident(self, request: Request) -> Result:
        zone = self._require_zone(request.payload.get("zone_id"))
        self._require_area(request.actor, zone)
        resident_id = _text(request.payload, "resident_id")
        name = _text(request.payload, "name")
        phone = str(request.payload.get("phone", ""))
        address = str(request.payload.get("address", ""))
        with self.store.transaction():
            self.store.upsert_resident(resident_id, zone["zone_id"], name,
                                       phone, address, request.actor)
            self.store.audit(request.request_id, request.actor, UPSERT_RESIDENT,
                             f"登记/更新转移对象 {name}（{resident_id}）",
                             zone_id=zone["zone_id"])
        return Result(True, "saved", "居民信息已保存",
                      {"zone_id": zone["zone_id"], "resident_id": resident_id})

    def _put_rule(self, request: Request) -> Result:
        rule = rule_engine.normalize_rule(request.payload)
        if rule["zone_id"]:
            zone = self._require_zone(rule["zone_id"])
            self._require_area(request.actor, zone)
        elif rule["admin_area"]:
            self._require_admin_or_area(request.actor, rule["admin_area"])
        else:
            self._require_admin(request.actor)
        rule["created_by"] = request.actor
        with self.store.transaction():
            try:
                self.store.insert_rule(rule)
            except Exception as exc:  # 主键重复
                raise ServiceError("rule_exists", f"规则 {rule['rule_id']} 已存在", 409) from exc
            scope = rule["zone_id"] or rule["admin_area"] or "全域"
            self.store.audit(request.request_id, request.actor, PUT_RULE,
                             f"配置规则 {rule['rule_id']}：{scope} {rule['metric']} "
                             f"{rule['aggregate']}>={rule['threshold']} -> "
                             f"{LEVEL_NAMES[rule['target_level']]}",
                             zone_id=rule["zone_id"])
        return Result(True, "saved", "预警规则已生效", {"rule_id": rule["rule_id"]})

    def _submit_observation(self, request: Request) -> Result:
        zone = self._require_zone(request.payload.get("zone_id"))
        self._require_area(request.actor, zone)
        metric = _text(request.payload, "metric")
        source_ref = _text(request.payload, "source_ref")
        observed_at = _parse_ts(_text(request.payload, "observed_at"))
        try:
            value = float(request.payload["value"])
        except (KeyError, TypeError, ValueError):
            raise ServiceError("invalid_payload", "观测值 value 必须是数字")
        dept_id = request.payload.get("dept_id")

        flush: list[str] = []
        out: Result | None = None
        data: dict[str, Any] = {}
        with self.store.transaction():
            duplicate = self.store.get_observation_by_ref(source_ref)
            if duplicate is not None:
                # 上游重发：落回原事件，不再研判、不再通知
                event = (self.store.get_event(duplicate["event_id"])
                         if duplicate["event_id"] else None)
                self.store.audit(request.request_id, request.actor,
                                 SUBMIT_OBSERVATION,
                                 f"重复观测 {source_ref} 已忽略，归属原事件"
                                 f" {duplicate['event_id'] or '（未立案）'}",
                                 zone_id=zone["zone_id"],
                                 event_id=duplicate["event_id"])
                state = event["current_level"] if event else "observed"
                out = Result(True, state, "重复观测已归并到原事件", {
                    "deduplicated": True, "source_ref": source_ref,
                    "event_id": duplicate["event_id"],
                })
            else:
                open_event = self.store.get_open_event(zone["zone_id"])
                obs_id = self.store.insert_observation(
                    zone["zone_id"], metric, value,
                    observed_at.isoformat(timespec="seconds"), source_ref,
                    request.request_id,
                    open_event["event_id"] if open_event else None)
                hit = rule_engine.evaluate(
                    self.store, zone, metric,
                    observed_at.isoformat(timespec="seconds"))
                if hit is None:
                    self.store.audit(request.request_id, request.actor,
                                     SUBMIT_OBSERVATION,
                                     f"观测 {metric}={value}（{source_ref}）未达预警阈值",
                                     zone_id=zone["zone_id"],
                                     event_id=open_event["event_id"] if open_event else None)
                    out = Result(True,
                                 open_event["current_level"] if open_event else "observed",
                                 "观测已记录，暂不触发预警",
                                 {"observation_id": obs_id,
                                  "event_id": open_event["event_id"] if open_event else None})
                elif open_event is None:
                    event_id = self._open_event(zone, hit, request, source_ref)
                    self.store.attach_observations(zone["zone_id"], event_id)
                    if dept_id:
                        directive_id, task_id = self._auto_dispatch(
                            zone["zone_id"], event_id, hit.target_level, request,
                            title=f"{LEVEL_NAMES[hit.target_level]}转移令", dept_id=dept_id)
                        flush.append(task_id)
                    state = hit.target_level
                    data = {"source_ref": source_ref, "event_id": event_id,
                            "level": state, "trigger": hit.as_dict()}
                    out = Result(True, state,
                                 f"达到{LEVEL_NAMES[hit.target_level]}，事件已立案", data)
                elif LEVEL_ORDER[hit.target_level] > LEVEL_ORDER[open_event["current_level"]]:
                    event_id = open_event["event_id"]
                    self._apply_escalation(zone, open_event, hit.target_level,
                                           request, reason_from_hit(hit),
                                           hit=hit, source_ref=source_ref,
                                           dept_id=dept_id, flush=flush)
                    state = hit.target_level
                    data = {"source_ref": source_ref, "event_id": event_id,
                            "level": state, "trigger": hit.as_dict()}
                    out = Result(True, state,
                                 f"预警升级为{LEVEL_NAMES[hit.target_level]}", data)
                else:
                    event_id = open_event["event_id"]
                    self.store.audit(request.request_id, request.actor,
                                     SUBMIT_OBSERVATION,
                                     f"观测 {metric}={value} 命中规则 {hit.rule_id}，"
                                     f"未高于当前等级，等级不变",
                                     zone_id=zone["zone_id"], event_id=event_id)
                    data = {"source_ref": source_ref, "event_id": event_id,
                            "level": open_event["current_level"],
                            "trigger": hit.as_dict()}
                    out = Result(True, open_event["current_level"],
                                 "观测已记录，当前等级无需调整", data)
        if flush:
            out.data["_flush_notifications"] = flush
        return out

    def _open_event(self, zone: Any, hit: rule_engine.RuleHit,
                    request: Request, source_ref: str) -> str:
        event_id = f"evt-{uuid.uuid4().hex[:12]}"
        self.store.insert_event(event_id, zone["zone_id"], hit.target_level, request.actor)
        self.store.set_open_event(zone["zone_id"], event_id)
        self.store.add_level_history({
            "event_id": event_id, "from_level": None,
            "to_level": hit.target_level,
            "reason": reason_from_hit(hit), "rule_id": hit.rule_id,
            "metric": hit.metric, "observed_value": hit.value,
            "threshold": hit.threshold, "source_ref": source_ref,
            "actor": request.actor, "request_id": request.request_id,
        })
        self.store.audit(request.request_id, request.actor, SUBMIT_OBSERVATION,
                         f"立案并发布{LEVEL_NAMES[hit.target_level]}：{reason_from_hit(hit)}",
                         zone_id=zone["zone_id"], event_id=event_id)
        return event_id

    def _escalate(self, request: Request) -> Result:
        zone = self._require_zone(request.payload.get("zone_id"))
        self._require_area(request.actor, zone)
        target = _text(request.payload, "target_level")
        if target not in LEVELS:
            raise ServiceError("invalid_payload", f"目标等级必须是 {LEVELS} 之一")
        reason = _text(request.payload, "reason")
        dept_id = request.payload.get("dept_id")
        flush: list[str] = []
        with self.store.transaction():
            event = self.store.get_open_event(zone["zone_id"])
            if event is None:
                raise ServiceError("no_open_event", "该风险区没有进行中的事件", 409)
            if LEVEL_ORDER[target] <= LEVEL_ORDER[event["current_level"]]:
                raise ServiceError(
                    "level_not_higher",
                    f"当前已为{LEVEL_NAMES[event['current_level']]}，只能向更高等级升级", 409)
            self._apply_escalation(zone, event, target, request, reason,
                                   dept_id=dept_id, flush=flush)
        data: dict[str, Any] = {"event_id": event["event_id"], "level": target}
        if flush:
            data["_flush_notifications"] = flush
        return Result(True, target, f"会商升级为{LEVEL_NAMES[target]}，{reason}", data)

    def _apply_escalation(self, zone: Any, event: Any, target: str,
                          request: Request, reason: str,
                          hit: rule_engine.RuleHit | None = None,
                          source_ref: str | None = None,
                          dept_id: str | None = None,
                          flush: list[str] | None = None) -> None:
        """事务内：记履历、作废旧指令；需要时自动下达新指令并派单。"""
        event_id = event["event_id"]
        self.store.add_level_history({
            "event_id": event_id, "from_level": event["current_level"],
            "to_level": target, "reason": reason,
            "rule_id": hit.rule_id if hit else None,
            "metric": hit.metric if hit else None,
            "observed_value": hit.value if hit else None,
            "threshold": hit.threshold if hit else None,
            "source_ref": source_ref, "actor": request.actor,
            "request_id": request.request_id,
        })
        self.store.update_event_level(event_id, target)
        superseded = self.store.supersede_directives(event_id)
        audit_action = (ESCALATE_EVENT if request.action == ESCALATE_EVENT
                        else SUBMIT_OBSERVATION)
        self.store.audit(request.request_id, request.actor, audit_action,
                         f"升级 {LEVEL_NAMES.get(event['current_level'], event['current_level'])}"
                         f"→{LEVEL_NAMES[target]}：{reason}"
                         + (f"；旧指令 {superseded} 自动作废" if superseded else ""),
                         zone_id=zone["zone_id"], event_id=event_id)
        if dept_id:
            _, task_id = self._auto_dispatch(
                zone["zone_id"], event_id, target, request,
                title=f"{LEVEL_NAMES[target]}转移令（升级）", dept_id=dept_id)
            if flush is not None:
                flush.append(task_id)

    def _auto_dispatch(self, zone_id: str, event_id: str, level: str,
                       request: Request, title: str, dept_id: str) -> tuple[str, str]:
        """观测升级联动：建指令、派责任组、生成接续名单（同一事务内）。"""
        dept = self.store.get_department(dept_id)
        if dept is None:
            raise ServiceError("dept_unknown", f"责任组 {dept_id} 不存在")
        zone = self.store.get_zone(zone_id)
        if not self.store.is_admin(request.actor) and not any(
                area_covers(a, zone["admin_area"]) for a in self.store.dept_areas(dept_id)):
            raise ServiceError("out_of_jurisdiction",
                               f"责任组 {dept_id} 未被授权该风险区所在区域")
        directive_id = f"dir-{uuid.uuid4().hex[:12]}"
        self.store.insert_directive({
            "directive_id": directive_id, "event_id": event_id, "level": level,
            "title": title, "content": "", "issued_by": request.actor,
            "request_id": request.request_id,
        })
        task_id = self._create_task_with_roster(
            directive_id, event_id, dept_id, request)
        return directive_id, task_id

    def _issue_directive(self, request: Request) -> Result:
        event = self._require_event(request.payload.get("event_id"))
        zone = self._require_zone(event["zone_id"])
        self._require_area(request.actor, zone)
        title = _text(request.payload, "title")
        content = str(request.payload.get("content", ""))
        level = str(request.payload.get("level") or event["current_level"])
        if level not in LEVELS or LEVEL_ORDER[level] < LEVEL_ORDER[event["current_level"]]:
            raise ServiceError("invalid_payload", "指令等级不能低于事件当前等级")
        with self.store.transaction():
            directive_id = f"dir-{uuid.uuid4().hex[:12]}"
            self.store.insert_directive({
                "directive_id": directive_id, "event_id": event["event_id"],
                "level": level, "title": title, "content": content,
                "issued_by": request.actor, "request_id": request.request_id,
            })
            self.store.audit(request.request_id, request.actor, ISSUE_DIRECTIVE,
                             f"下达指令《{title}》（{LEVEL_NAMES[level]}，待派单）",
                             zone_id=zone["zone_id"], event_id=event["event_id"],
                             directive_id=directive_id)
        return Result(True, "pending", "指令已下达，等待派给责任组",
                      {"directive_id": directive_id, "event_id": event["event_id"]})

    def _dispatch_task(self, request: Request) -> Result:
        directive = self.store.get_directive(_text(request.payload, "directive_id"))
        if directive is None:
            raise ServiceError("directive_unknown", "指令不存在", 404)
        event = self.store.get_event(directive["event_id"])
        zone = self._require_zone(event["zone_id"])
        dept_id = _text(request.payload, "dept_id")
        dept = self.store.get_department(dept_id)
        if dept is None:
            raise ServiceError("dept_unknown", f"责任组 {dept_id} 不存在")
        if directive["status"] != "pending":
            raise ServiceError("directive_not_open",
                               f"指令当前状态为 {directive['status']}，不能派单", 409)
        if not self.store.is_admin(request.actor) and not any(
                area_covers(a, zone["admin_area"]) for a in self.store.dept_areas(dept_id)):
            raise ServiceError("out_of_jurisdiction",
                               f"责任组 {dept_id} 未被授权该风险区所在区域")
        with self.store.transaction():
            task_id = self._create_task_with_roster(
                directive["directive_id"], event["event_id"], dept_id, request)
        return Result(True, "dispatched", f"已派给 {dept_id}，转移名单已生成",
                      {"task_id": task_id, "directive_id": directive["directive_id"],
                       "dept_id": dept_id, "event_id": event["event_id"],
                       "_flush_notifications": [task_id]})

    def _create_task_with_roster(self, directive_id: str, event_id: str,
                                 dept_id: str, request: Request) -> str:
        task_id = f"task-{uuid.uuid4().hex[:12]}"
        previous = self.store.latest_task(event_id)
        self.store.insert_task({
            "task_id": task_id, "directive_id": directive_id,
            "event_id": event_id, "dept_id": dept_id,
            "carry_of_task_id": previous["task_id"] if previous else None,
            "dispatched_by": request.actor, "request_id": request.request_id,
        })
        zone = self.store.get_zone(self.store.get_event(event_id)["zone_id"])
        carried = 0
        for resident in self.store.active_residents(zone["zone_id"]):
            note_status, receipt_status, confirmed_at, receipt_code = (
                "pending", "unconfirmed", None, None)
            if previous is not None:
                prior = self.store.roster_item(previous["task_id"], resident["resident_id"])
                if prior is not None:
                    # 接续：已确认户保持确认且不再通知；已通知未确认户不重复通知，
                    # 继续等待回执；从未送达的户保持 pending 待发。
                    receipt_status = prior["receipt_status"]
                    confirmed_at = prior["confirmed_at"]
                    receipt_code = prior["receipt_code"]
                    if receipt_status == "confirmed":
                        note_status = "sent"
                    elif prior["notification_status"] == "sent":
                        note_status = "sent"
                    carried += 1
            self.store.insert_roster_item(
                task_id, event_id, resident["resident_id"],
                notification_status=note_status, receipt_status=receipt_status,
                confirmed_at=confirmed_at, receipt_code=receipt_code)
        self.store.update_directive_status(directive_id, "dispatched")
        self.store.audit(request.request_id, request.actor, DISPATCH_TASK,
                         f"派单给 {dept_id}"
                         + (f"，沿事件接续上一单 {previous['task_id']}（{carried} 户状态沿用）"
                            if previous else "，生成转移名单"),
                         zone_id=zone["zone_id"], event_id=event_id,
                         directive_id=directive_id, task_id=task_id)
        return task_id

    def _task_transition(self, request: Request, target: str) -> Result:
        task = self.store.get_task(_text(request.payload, "task_id"))
        if task is None:
            raise ServiceError("task_unknown", "任务不存在", 404)
        event = self.store.get_event(task["event_id"])
        zone = self._require_zone(event["zone_id"])
        if not self.store.is_admin(request.actor) and not self.store.is_member(
                request.actor, task["dept_id"]):
            raise ServiceError("not_responsible",
                               f"仅 {task['dept_id']} 成员可操作该任务", 403)
        verb = {"acked": ("接单", "dispatched", "acked", ACK_TASK),
                "completed": ("反馈处置完成", "acked", "completed", COMPLETE_TASK)}[target]
        label, required, new_status, action = verb
        if task["status"] != required:
            raise ServiceError("invalid_transition",
                               f"任务当前为 {task['status']}，不能{label}", 409)
        with self.store.transaction():
            self.store.update_task_status(task["task_id"], new_status, actor=request.actor)
            if new_status == "completed":
                self.store.complete_directive_if_open(task["directive_id"])
            self.store.audit(request.request_id, request.actor, action,
                             f"{label}（{task['dept_id']}）",
                             zone_id=zone["zone_id"], event_id=event["event_id"],
                             directive_id=task["directive_id"], task_id=task["task_id"])
        return Result(True, new_status, f"已{label}",
                      {"task_id": task["task_id"], "status": new_status})

    def _record_receipt(self, request: Request) -> Result:
        receipt_code = _text(request.payload, "receipt_code")
        zone = self._require_zone(request.payload.get("zone_id"))
        self._require_area(request.actor, zone)
        resident_id = _text(request.payload, "resident_id")
        resident = self.store.get_resident(zone["zone_id"], resident_id)
        if resident is None:
            raise ServiceError("resident_unknown", "名单中没有该居民", 404)
        with self.store.transaction():
            existing = self.store.roster_item_by_code(receipt_code)
            if existing is not None:
                # 同一回执重复到达（换了 request_id 重推）：只认第一次
                self.store.audit(request.request_id, request.actor, RECORD_RECEIPT,
                                 f"重复回执 {receipt_code} 已归并到原确认",
                                 zone_id=zone["zone_id"],
                                 event_id=existing["event_id"],
                                 task_id=existing["task_id"])
                return Result(True, "confirmed", "重复回执已归并，确认状态不变", {
                    "deduplicated": True, "receipt_code": receipt_code,
                    "task_id": existing["task_id"],
                    "event_id": existing["event_id"], "resident_id": resident_id,
                })
            event = self.store.get_open_event(zone["zone_id"])
            if event is None:
                raise ServiceError("no_open_event", "该风险区没有进行中的事件", 409)
            item = self.store.roster_item_for_event_resident(
                event["event_id"], resident_id)
            if item is None:
                raise ServiceError("not_in_roster", "该居民不在当前转移名单中", 404)
            if item["receipt_status"] == "confirmed":
                return Result(True, "confirmed", "该户已确认，无需重复登记", {
                    "deduplicated": True, "receipt_code": receipt_code,
                    "task_id": item["task_id"], "event_id": event["event_id"],
                    "resident_id": resident_id,
                })
            self.store.confirm_roster_item(item["id"], receipt_code)
            self.store.audit(request.request_id, request.actor, RECORD_RECEIPT,
                             f"登记 {resident['name']}（{resident_id}）转移确认 {receipt_code}",
                             zone_id=zone["zone_id"], event_id=event["event_id"],
                             task_id=item["task_id"])
            stats = self.store.roster_stats(item["task_id"])
        return Result(True, "confirmed", "居民确认已登记",
                      {"receipt_code": receipt_code, "task_id": item["task_id"],
                       "event_id": event["event_id"], "resident_id": resident_id,
                       "roster": stats})

    def _retry_notifications(self, request: Request) -> Result:
        zones = {z["zone_id"]: z for z in self._visible_zones(request.actor)}
        pending = [r for r in self.store.pending_notifications()]
        # 再次按授权过滤（pending 已 join 事件，但不携带 zone 行）
        allowed: list[Any] = []
        for row in pending:
            event = self.store.get_event(row["event_id"])
            if event and event["zone_id"] in zones:
                allowed.append(row)
        sent, failed = 0, 0
        for row in allowed:
            item = {"resident_id": row["resident_id"]}
            if self._notifier(row["task_id"], item):
                with self.store.transaction():
                    self.store.mark_notification_sent(row["id"])
                sent += 1
            else:
                failed += 1
        if sent:
            with self.store.transaction():
                self.store.audit(request.request_id, request.actor,
                                 RETRY_PENDING_NOTIFICATIONS,
                                 f"续发送达 {sent} 户，失败 {failed} 户")
        message = f"续发完成：送达 {sent} 户" + (f"，{failed} 户仍失败" if failed else "")
        return Result(failed == 0, "retried", message,
                      {"sent": sent, "failed": failed, "remaining": failed})

    def _close_event(self, request: Request) -> Result:
        zone = self._require_zone(request.payload.get("zone_id"))
        self._require_area(request.actor, zone)
        with self.store.transaction():
            event = self.store.get_open_event(zone["zone_id"])
            if event is None:
                raise ServiceError("no_open_event", "该风险区没有进行中的事件", 409)
            self.store.close_event(event["event_id"], request.actor)
            self.store.set_open_event(zone["zone_id"], None)
            self.store.audit(request.request_id, request.actor, CLOSE_EVENT,
                             "会商解除，关闭事件", zone_id=zone["zone_id"],
                             event_id=event["event_id"])
        return Result(True, "closed", "事件已关闭", {"event_id": event["event_id"]})

    # ---- 通知送达（事务外，尽力而为，可续办）------------------------------
    def _flush_notifications(self, task_ids: Iterable[str]) -> None:
        for task_id in task_ids:
            for row in self.store.roster_for_task(task_id):
                if row["notification_status"] != "pending":
                    continue
                item = {"resident_id": row["resident_id"]}
                if self._notifier(task_id, item):
                    with self.store.transaction():
                        self.store.mark_notification_sent(row["id"])
                # 失败则保留 pending，由 retry_pending_notifications 续办

    # ---- 授权与基础校验 ---------------------------------------------------
    def _require_admin(self, actor: str) -> None:
        if not self.store.is_admin(actor):
            raise ServiceError("forbidden", "仅会商管理员可执行该操作", 403)

    def _require_admin_or_area(self, actor: str, admin_area: str) -> None:
        if self.store.is_admin(actor):
            return
        if any(area_covers(a, admin_area) for a in self.store.granted_areas(actor)):
            return
        raise ServiceError("out_of_jurisdiction", "未授权该行政区域", 403)

    def _require_area(self, actor: str, zone: Any) -> None:
        if self.store.is_admin(actor):
            return
        if any(area_covers(a, zone["admin_area"])
               for a in self.store.granted_areas(actor)):
            return
        raise ServiceError("out_of_jurisdiction",
                           f"{actor} 未被授权风险区 {zone['zone_id']} 所在区域", 403)

    def _require_zone(self, value: Any) -> Any:
        zone_id = str(value or "").strip()
        if not zone_id:
            raise ServiceError("invalid_payload", "缺少 zone_id")
        zone = self.store.get_zone(zone_id)
        if zone is None:
            raise ServiceError("zone_unknown", f"风险区 {zone_id} 不存在", 404)
        return zone

    def _require_event(self, value: Any) -> Any:
        event_id = str(value or "").strip()
        if not event_id:
            raise ServiceError("invalid_payload", "缺少 event_id")
        event = self.store.get_event(event_id)
        if event is None:
            raise ServiceError("event_unknown", f"事件 {event_id} 不存在", 404)
        return event


# ---- 小工具 ---------------------------------------------------------------
def _text(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key, "")).strip()
    if not value:
        raise ServiceError("invalid_payload", f"缺少必填字段 {key}")
    return value


def _parse_ts(value: str) -> datetime:
    from .storage import parse_iso
    try:
        return parse_iso(value)
    except ValueError as exc:
        raise ServiceError("invalid_payload",
                           "observed_at 需为 ISO8601 时间，如 2026-10-03T08:00:00Z") from exc


def reason_from_hit(hit: rule_engine.RuleHit) -> str:
    agg_name = {"latest": "最新值", "sum": "累计雨量", "max": "最大值",
                "avg": "平均值", "min": "最小值"}[hit.aggregate]
    return (f"规则 {hit.rule_id} 命中：近 {hit.window_minutes} 分钟 "
            f"{hit.samples} 个观测，{agg_name} {hit.value} 达到阈值 {hit.threshold}")
