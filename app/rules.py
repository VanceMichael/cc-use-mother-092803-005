"""规则引擎：观测值 -> 适用规则匹配 -> 应达到的预警等级。

规则三选一确定适用范围，精确性从高到低：
1. 绑定具体风险区（zone_id）；
2. 绑定行政区域（admin_area），按层级覆盖，如 "重庆" 覆盖 "重庆/巫溪"；
3. 全域兜底规则（两者皆空）。

每条规则规定观测指标、回看窗口、最少样本数、聚合方式与阈值；
窗口内样本数不足时规则不成立，避免单站偶发值误触发。
同一观测命中多条规则时取最高目标等级，等级相同时取更精确、
阈值更高的规则（便于解释"为何升级"）。
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from .contracts import LEVELS, LEVEL_ORDER, ServiceError
from .storage import area_covers, parse_iso

AGGREGATES = {"latest", "sum", "max", "avg", "min"}


@dataclass(frozen=True)
class RuleHit:
    rule_id: str
    target_level: str
    metric: str
    value: float
    threshold: float
    aggregate: str
    window_minutes: int
    samples: int
    scope: str  # zone / area / global

    def as_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "target_level": self.target_level,
            "metric": self.metric,
            "value": self.value,
            "threshold": self.threshold,
            "aggregate": self.aggregate,
            "window_minutes": self.window_minutes,
            "samples": self.samples,
            "scope": self.scope,
        }


def normalize_rule(payload: dict[str, Any]) -> dict[str, Any]:
    """校验并规范化 put_rule 的载荷。"""
    rule_id = str(payload.get("rule_id", "")).strip()
    metric = str(payload.get("metric", "")).strip()
    if not rule_id:
        raise ServiceError("invalid_rule", "规则缺少 rule_id")
    if not metric:
        raise ServiceError("invalid_rule", "规则缺少观测指标 metric")
    zone_id = payload.get("zone_id")
    admin_area = payload.get("admin_area")
    zone_id = str(zone_id).strip() if zone_id else None
    admin_area = str(admin_area).strip().rstrip("/") if admin_area else None
    if zone_id and admin_area:
        raise ServiceError("invalid_rule", "规则只能绑定风险区或行政区域之一")
    try:
        window = int(payload.get("window_minutes"))
        min_samples = int(payload.get("min_samples"))
        threshold = float(payload.get("threshold"))
    except (TypeError, ValueError):
        raise ServiceError("invalid_rule", "窗口、样本数或阈值不是数字")
    if window < 1:
        raise ServiceError("invalid_rule", "窗口分钟数必须 >= 1")
    if min_samples < 1:
        raise ServiceError("invalid_rule", "最少样本数必须 >= 1")
    aggregate = str(payload.get("aggregate", "latest")).strip()
    if aggregate not in AGGREGATES:
        raise ServiceError("invalid_rule", f"聚合方式必须是 {sorted(AGGREGATES)} 之一")
    target_level = str(payload.get("target_level", "")).strip()
    if target_level not in LEVELS:
        raise ServiceError("invalid_rule", f"目标等级必须是 {LEVELS} 之一")
    return {
        "rule_id": rule_id,
        "zone_id": zone_id,
        "admin_area": admin_area,
        "metric": metric,
        "window_minutes": window,
        "min_samples": min_samples,
        "aggregate": aggregate,
        "threshold": threshold,
        "target_level": target_level,
    }


def _scope(rule: sqlite3.Row) -> str:
    if rule["zone_id"]:
        return "zone"
    if rule["admin_area"]:
        return "area"
    return "global"


def rule_applies(rule: sqlite3.Row, zone: sqlite3.Row) -> bool:
    if rule["zone_id"]:
        return rule["zone_id"] == zone["zone_id"]
    if rule["admin_area"]:
        return area_covers(rule["admin_area"], zone["admin_area"])
    return True


def _specificity(rule: sqlite3.Row) -> tuple[int, int]:
    """越大越精确：风险区规则 > 深路径区域规则 > 全域规则。"""
    if rule["zone_id"]:
        return (2, 0)
    if rule["admin_area"]:
        return (1, rule["admin_area"].count("/") + 1)
    return (0, 0)


def _aggregate(values: list[float], mode: str) -> float:
    if mode == "latest":
        return values[-1]
    if mode == "sum":
        return sum(values)
    if mode == "max":
        return max(values)
    if mode == "min":
        return min(values)
    return sum(values) / len(values)


def evaluate(store: Any, zone: sqlite3.Row, metric: str,
             observed_at: str) -> RuleHit | None:
    """以本次观测时刻为准，评估适用于该风险区的规则是否被触发。"""
    rules = [r for r in store.candidate_rules(zone)
             if r["metric"] == metric and rule_applies(r, zone)]
    if not rules:
        return None
    until = parse_iso(observed_at)
    best: RuleHit | None = None
    best_rule: sqlite3.Row | None = None
    for rule in rules:
        since = (until - timedelta(minutes=rule["window_minutes"])).isoformat()
        rows = store.observations_in_window(
            zone["zone_id"], metric, since, observed_at
        )
        values = [r["value"] for r in rows]
        if len(values) < rule["min_samples"]:
            continue
        value = _aggregate(values, rule["aggregate"])
        if value < rule["threshold"]:
            continue
        hit = RuleHit(
            rule_id=rule["rule_id"],
            target_level=rule["target_level"],
            metric=metric,
            value=round(value, 4),
            threshold=rule["threshold"],
            aggregate=rule["aggregate"],
            window_minutes=rule["window_minutes"],
            samples=len(values),
            scope=_scope(rule),
        )
        if best is None or best_rule is None or _hit_ranks_higher(hit, rule, best, best_rule):
            best = hit
            best_rule = rule
    return best


def _hit_ranks_higher(hit: RuleHit, rule: sqlite3.Row,
                      best: RuleHit, best_rule: sqlite3.Row) -> bool:
    """同窗口多规则命中时的择优：等级高 > 范围精确 > 阈值高。"""
    if LEVEL_ORDER[hit.target_level] != LEVEL_ORDER[best.target_level]:
        return LEVEL_ORDER[hit.target_level] > LEVEL_ORDER[best.target_level]
    spec_new, spec_best = _specificity(rule), _specificity(best_rule)
    if spec_new != spec_best:
        return spec_new > spec_best
    return hit.threshold > best.threshold
