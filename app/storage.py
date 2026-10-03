"""SQLite 持久化层。

一个连接、一张 inbox、全程事务：
- inbox 以 request_id 为主键，保证请求重试的精确一次语义；
- 业务去重另有唯一约束（observations.source_ref、roster 的
  (task_id, resident_id) 与 receipt_code），保证不同请求重发同一
  上游报文/同一居民回执时，仍然落到同一条事件记录；
- events 上的部分唯一索引保证一个风险区至多一个进行中的事件。

进程崩溃后只需用同一数据库文件重新构造服务：已提交的状态全部保留，
未送达的通知仍为 pending，可通过 retry_pending_notifications 续办。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS zones (
    zone_id       TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    admin_area    TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active',
    open_event_id TEXT,
    created_by    TEXT NOT NULL,
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS departments (
    dept_id    TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS department_areas (
    dept_id    TEXT NOT NULL REFERENCES departments(dept_id),
    admin_area TEXT NOT NULL,
    granted_by TEXT NOT NULL,
    granted_at TEXT NOT NULL,
    PRIMARY KEY (dept_id, admin_area)
);

CREATE TABLE IF NOT EXISTS department_members (
    dept_id   TEXT NOT NULL REFERENCES departments(dept_id),
    actor     TEXT NOT NULL,
    joined_at TEXT NOT NULL,
    PRIMARY KEY (dept_id, actor)
);

CREATE TABLE IF NOT EXISTS actor_roles (
    actor TEXT PRIMARY KEY,
    role  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS residents (
    resident_id TEXT NOT NULL,
    zone_id     TEXT NOT NULL REFERENCES zones(zone_id),
    name        TEXT NOT NULL,
    phone       TEXT NOT NULL DEFAULT '',
    address     TEXT NOT NULL DEFAULT '',
    active      INTEGER NOT NULL DEFAULT 1,
    updated_by  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (zone_id, resident_id)
);

CREATE TABLE IF NOT EXISTS rules (
    rule_id        TEXT PRIMARY KEY,
    zone_id        TEXT REFERENCES zones(zone_id),
    admin_area     TEXT,
    metric         TEXT NOT NULL,
    window_minutes INTEGER NOT NULL,
    min_samples    INTEGER NOT NULL,
    aggregate      TEXT NOT NULL,
    threshold      REAL NOT NULL,
    target_level   TEXT NOT NULL,
    active         INTEGER NOT NULL DEFAULT 1,
    created_by     TEXT NOT NULL,
    created_at     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT PRIMARY KEY,
    zone_id       TEXT NOT NULL REFERENCES zones(zone_id),
    status        TEXT NOT NULL DEFAULT 'open',
    current_level TEXT NOT NULL,
    opened_by     TEXT NOT NULL,
    opened_at     TEXT NOT NULL,
    closed_at     TEXT,
    closed_by     TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS events_one_open
    ON events(zone_id) WHERE status = 'open';

CREATE TABLE IF NOT EXISTS observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    zone_id        TEXT NOT NULL REFERENCES zones(zone_id),
    event_id       TEXT REFERENCES events(event_id),
    metric         TEXT NOT NULL,
    value          REAL NOT NULL,
    observed_at    TEXT NOT NULL,
    source_ref     TEXT NOT NULL UNIQUE,
    request_id     TEXT NOT NULL,
    received_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS obs_zone_metric_time
    ON observations(zone_id, metric, observed_at);

CREATE TABLE IF NOT EXISTS event_levels (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id               TEXT NOT NULL REFERENCES events(event_id),
    from_level             TEXT,
    to_level               TEXT NOT NULL,
    reason                 TEXT NOT NULL,
    rule_id                TEXT,
    metric                 TEXT,
    observed_value         REAL,
    threshold              REAL,
    source_ref             TEXT,
    actor                  TEXT NOT NULL,
    request_id             TEXT NOT NULL,
    created_at             TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS directives (
    directive_id   TEXT PRIMARY KEY,
    event_id       TEXT NOT NULL REFERENCES events(event_id),
    level          TEXT NOT NULL,
    title          TEXT NOT NULL,
    content        TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL,
    issued_by      TEXT NOT NULL,
    issued_at      TEXT NOT NULL,
    request_id     TEXT NOT NULL,
    superseded_at  TEXT
);
CREATE INDEX IF NOT EXISTS directives_event ON directives(event_id);

CREATE TABLE IF NOT EXISTS tasks (
    task_id          TEXT PRIMARY KEY,
    directive_id     TEXT NOT NULL UNIQUE REFERENCES directives(directive_id),
    event_id         TEXT NOT NULL REFERENCES events(event_id),
    dept_id          TEXT NOT NULL REFERENCES departments(dept_id),
    status           TEXT NOT NULL,
    carry_of_task_id TEXT REFERENCES tasks(task_id),
    dispatched_by    TEXT NOT NULL,
    dispatched_at    TEXT NOT NULL,
    request_id       TEXT NOT NULL,
    acked_by         TEXT,
    acked_at         TEXT,
    completed_by     TEXT,
    completed_at     TEXT
);
CREATE INDEX IF NOT EXISTS tasks_event ON tasks(event_id);

CREATE TABLE IF NOT EXISTS roster_items (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id             TEXT NOT NULL REFERENCES tasks(task_id),
    event_id            TEXT NOT NULL REFERENCES events(event_id),
    resident_id         TEXT NOT NULL,
    notification_status TEXT NOT NULL DEFAULT 'pending',
    notified_at         TEXT,
    receipt_status      TEXT NOT NULL DEFAULT 'unconfirmed',
    confirmed_at        TEXT,
    receipt_code        TEXT,
    UNIQUE(task_id, resident_id)
);
CREATE INDEX IF NOT EXISTS roster_event_resident
    ON roster_items(event_id, resident_id);
CREATE INDEX IF NOT EXISTS roster_receipt_code
    ON roster_items(receipt_code);

CREATE TABLE IF NOT EXISTS inbox (
    request_id TEXT PRIMARY KEY,
    action     TEXT NOT NULL,
    result     TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at   TEXT NOT NULL,
    request_id   TEXT NOT NULL,
    actor        TEXT NOT NULL,
    action       TEXT NOT NULL,
    zone_id      TEXT,
    event_id     TEXT,
    directive_id TEXT,
    task_id      TEXT,
    summary      TEXT NOT NULL
);
"""

SEED_ADMIN = "admin"
# 系统续办账号：进程重启后 retry_pending_notifications 以其身份执行，
# 需可见全部区域
RECOVERY_ACTOR = "system"


def now_iso() -> str:
    return (datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
            .isoformat() + "Z")


def parse_iso(value: str) -> datetime:
    """宽容解析 ISO8601（兼容末尾 Z），统一归一化为 UTC naive，便于字符串比较。"""
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def area_covers(granted_area: str, target_area: str) -> bool:
    """授权区域是否覆盖目标区域：相等或为其上级路径段。

    行政区域以 ``/`` 分层，例如 "重庆/巫溪"，省级授权 "重庆" 覆盖其下所有县区。
    """
    if granted_area == target_area:
        return True
    return target_area.startswith(granted_area.rstrip("/") + "/")


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._tx_depth = 0
        self.conn.execute("PRAGMA foreign_keys=ON")
        if path != ":memory:":
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
        self.initialize()

    def initialize(self) -> None:
        self.conn.executescript(SCHEMA)
        self.conn.executemany(
            "INSERT OR IGNORE INTO actor_roles(actor, role) VALUES (?, 'admin')",
            [(SEED_ADMIN,), (RECOVERY_ACTOR,)],
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """可重入事务：外层 BEGIN IMMEDIATE，内层用 SAVEPOINT。

        服务层因此可以把"业务写入 + inbox 幂等登记"包进同一个提交边界，
        各动作内部的 with 块在已有事务中退化为保存点。
        """
        if self._tx_depth == 0:
            self.conn.execute("BEGIN IMMEDIATE")
            self._tx_depth = 1
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            finally:
                self._tx_depth = 0
        else:
            name = f"sp_{self._tx_depth}"
            self._tx_depth += 1
            self.conn.execute(f"SAVEPOINT {name}")
            try:
                yield self.conn
                self.conn.execute(f"RELEASE SAVEPOINT {name}")
            except Exception:
                self.conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
                self.conn.execute(f"RELEASE SAVEPOINT {name}")
                raise
            finally:
                self._tx_depth -= 1

    # ---- inbox / 审计 ----------------------------------------------------
    def inbox_get(self, request_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            "SELECT result FROM inbox WHERE request_id=?", (request_id,)
        ).fetchone()
        return json.loads(row["result"]) if row else None

    def inbox_put(self, request_id: str, action: str, result_json: str) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO inbox(request_id, action, result, created_at) "
            "VALUES (?,?,?,?)",
            (request_id, action, result_json, now_iso()),
        )

    def audit(self, request_id: str, actor: str, action: str, summary: str,
              zone_id: str | None = None, event_id: str | None = None,
              directive_id: str | None = None, task_id: str | None = None) -> None:
        self.conn.execute(
            "INSERT INTO audit_log(created_at,request_id,actor,action,zone_id,"
            "event_id,directive_id,task_id,summary) VALUES (?,?,?,?,?,?,?,?,?)",
            (now_iso(), request_id, actor, action, zone_id, event_id,
             directive_id, task_id, summary),
        )

    def audit_list(self, event_id: str | None = None, zone_id: str | None = None,
                   limit: int = 100) -> list[sqlite3.Row]:
        sql = "SELECT * FROM audit_log WHERE 1=1"
        args: list[Any] = []
        if event_id:
            sql += " AND event_id=?"
            args.append(event_id)
        if zone_id:
            sql += " AND zone_id=?"
            args.append(zone_id)
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        return list(self.conn.execute(sql, args).fetchall())

    # ---- 组织与授权 ------------------------------------------------------
    def is_admin(self, actor: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM actor_roles WHERE actor=? AND role='admin'", (actor,)
        ).fetchone()
        return row is not None

    def upsert_department(self, dept_id: str, name: str) -> None:
        self.conn.execute(
            "INSERT INTO departments(dept_id,name,created_at) VALUES(?,?,?) "
            "ON CONFLICT(dept_id) DO UPDATE SET name=excluded.name",
            (dept_id, name, now_iso()),
        )

    def grant_area(self, dept_id: str, admin_area: str, actor: str) -> None:
        self.conn.execute(
            "INSERT INTO department_areas(dept_id,admin_area,granted_by,granted_at) "
            "VALUES(?,?,?,?) ON CONFLICT DO NOTHING",
            (dept_id, admin_area, actor, now_iso()),
        )

    def add_member(self, dept_id: str, actor: str) -> None:
        self.conn.execute(
            "INSERT INTO department_members(dept_id,actor,joined_at) VALUES(?,?,?) "
            "ON CONFLICT DO NOTHING",
            (dept_id, actor, now_iso()),
        )

    def is_member(self, actor: str, dept_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM department_members WHERE actor=? AND dept_id=?",
            (actor, dept_id),
        ).fetchone()
        return row is not None

    def member_dept_ids(self, actor: str) -> list[str]:
        return [r["dept_id"] for r in self.conn.execute(
            "SELECT dept_id FROM department_members WHERE actor=?", (actor,)
        ).fetchall()]

    def granted_areas(self, actor: str) -> list[str]:
        return [r["admin_area"] for r in self.conn.execute(
            "SELECT DISTINCT da.admin_area FROM department_areas da "
            "JOIN department_members dm ON dm.dept_id=da.dept_id "
            "WHERE dm.actor=?", (actor,)
        ).fetchall()]

    def dept_areas(self, dept_id: str) -> list[str]:
        return [r["admin_area"] for r in self.conn.execute(
            "SELECT admin_area FROM department_areas WHERE dept_id=?", (dept_id,)
        ).fetchall()]

    def get_department(self, dept_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM departments WHERE dept_id=?", (dept_id,)
        ).fetchone()

    # ---- 风险区 / 居民 ---------------------------------------------------
    def insert_zone(self, zone_id: str, name: str, admin_area: str, actor: str) -> bool:
        cur = self.conn.execute(
            "INSERT INTO zones(zone_id,name,admin_area,created_by,created_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(zone_id) DO NOTHING",
            (zone_id, name, admin_area, actor, now_iso()),
        )
        return cur.rowcount > 0

    def get_zone(self, zone_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM zones WHERE zone_id=?", (zone_id,)
        ).fetchone()

    def list_zones(self) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM zones WHERE status='active' ORDER BY zone_id"
        ).fetchall())

    def set_open_event(self, zone_id: str, event_id: str | None) -> None:
        self.conn.execute(
            "UPDATE zones SET open_event_id=? WHERE zone_id=?", (event_id, zone_id)
        )

    def upsert_resident(self, resident_id: str, zone_id: str, name: str,
                        phone: str, address: str, actor: str) -> None:
        self.conn.execute(
            "INSERT INTO residents(resident_id,zone_id,name,phone,address,active,"
            "updated_by,updated_at) VALUES(?,?,?,?,?,1,?,?) "
            "ON CONFLICT(resident_id,zone_id) DO UPDATE SET name=excluded.name,"
            "phone=excluded.phone,address=excluded.address,active=1,"
            "updated_by=excluded.updated_by,updated_at=excluded.updated_at",
            (resident_id, zone_id, name, phone, address, actor, now_iso()),
        )

    def active_residents(self, zone_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM residents WHERE zone_id=? AND active=1 ORDER BY resident_id",
            (zone_id,),
        ).fetchall())

    def get_resident(self, zone_id: str, resident_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM residents WHERE zone_id=? AND resident_id=?",
            (zone_id, resident_id),
        ).fetchone()

    # ---- 规则 ------------------------------------------------------------
    def insert_rule(self, rule: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO rules(rule_id,zone_id,admin_area,metric,window_minutes,"
            "min_samples,aggregate,threshold,target_level,active,created_by,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,1,?,?)",
            (rule["rule_id"], rule.get("zone_id"), rule.get("admin_area"),
             rule["metric"], rule["window_minutes"], rule["min_samples"],
             rule["aggregate"], rule["threshold"], rule["target_level"],
             rule["created_by"], now_iso()),
        )

    def candidate_rules(self, zone: sqlite3.Row) -> list[sqlite3.Row]:
        """可能适用于该风险区的全部启用规则，精确性由规则引擎判定。"""
        return list(self.conn.execute(
            "SELECT * FROM rules WHERE active=1 AND (zone_id IS NULL OR zone_id=?)",
            (zone["zone_id"],),
        ).fetchall())

    # ---- 观测 ------------------------------------------------------------
    def get_observation_by_ref(self, source_ref: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM observations WHERE source_ref=?", (source_ref,)
        ).fetchone()

    def insert_observation(self, zone_id: str, metric: str, value: float,
                           observed_at: str, source_ref: str, request_id: str,
                           event_id: str | None) -> int:
        cur = self.conn.execute(
            "INSERT INTO observations(zone_id,event_id,metric,value,observed_at,"
            "source_ref,request_id,received_at) VALUES(?,?,?,?,?,?,?,?)",
            (zone_id, event_id, metric, value, observed_at, source_ref,
             request_id, now_iso()),
        )
        return int(cur.lastrowid)

    def observations_in_window(self, zone_id: str, metric: str,
                               since: str, until: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM observations WHERE zone_id=? AND metric=? "
            "AND observed_at>=? AND observed_at<=? ORDER BY observed_at",
            (zone_id, metric, since, until),
        ).fetchall())

    def attach_observations(self, zone_id: str, event_id: str) -> None:
        """事件开立时，把此前未归属事件的观测并入本事件。"""
        self.conn.execute(
            "UPDATE observations SET event_id=? WHERE zone_id=? AND event_id IS NULL",
            (event_id, zone_id),
        )

    # ---- 事件与等级 ------------------------------------------------------
    def insert_event(self, event_id: str, zone_id: str, level: str, actor: str) -> None:
        self.conn.execute(
            "INSERT INTO events(event_id,zone_id,status,current_level,opened_by,opened_at) "
            "VALUES(?,?, 'open', ?,?,?)",
            (event_id, zone_id, level, actor, now_iso()),
        )

    def get_event(self, event_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()

    def get_open_event(self, zone_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM events WHERE zone_id=? AND status='open' ORDER BY opened_at DESC",
            (zone_id,),
        ).fetchone()

    def update_event_level(self, event_id: str, level: str) -> None:
        self.conn.execute(
            "UPDATE events SET current_level=? WHERE event_id=?", (level, event_id)
        )

    def close_event(self, event_id: str, actor: str) -> None:
        self.conn.execute(
            "UPDATE events SET status='closed',closed_at=?,closed_by=? WHERE event_id=?",
            (now_iso(), actor, event_id),
        )

    def add_level_history(self, item: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO event_levels(event_id,from_level,to_level,reason,rule_id,"
            "metric,observed_value,threshold,source_ref,actor,request_id,created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (item["event_id"], item.get("from_level"), item["to_level"],
             item["reason"], item.get("rule_id"), item.get("metric"),
             item.get("observed_value"), item.get("threshold"),
             item.get("source_ref"), item["actor"], item["request_id"], now_iso()),
        )

    def level_history(self, event_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM event_levels WHERE event_id=? ORDER BY id", (event_id,)
        ).fetchall())

    # ---- 指令 / 派单 / 名单 ----------------------------------------------
    def insert_directive(self, d: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO directives(directive_id,event_id,level,title,content,status,"
            "issued_by,issued_at,request_id) VALUES(?,?,?,?,?, 'pending', ?,?,?)",
            (d["directive_id"], d["event_id"], d["level"], d["title"],
             d.get("content", ""), d["issued_by"], now_iso(), d["request_id"]),
        )

    def get_directive(self, directive_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM directives WHERE directive_id=?", (directive_id,)
        ).fetchone()

    def directives_for_event(self, event_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM directives WHERE event_id=? ORDER BY issued_at, rowid",
            (event_id,),
        ).fetchall())

    def latest_directive(self, event_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM directives WHERE event_id=? ORDER BY rowid DESC LIMIT 1",
            (event_id,),
        ).fetchone()

    def active_directive(self, event_id: str) -> sqlite3.Row | None:
        """事件当前生效的指令：状态未终结（completed/superseded）的最新一条。"""
        return self.conn.execute(
            "SELECT * FROM directives WHERE event_id=? AND status NOT IN "
            "('completed','superseded') ORDER BY rowid DESC LIMIT 1",
            (event_id,),
        ).fetchone()

    def supersede_directives(self, event_id: str) -> list[str]:
        rows = self.conn.execute(
            "UPDATE directives SET status='superseded',superseded_at=? "
            "WHERE event_id=? AND status NOT IN ('completed','superseded') "
            "RETURNING directive_id",
            (now_iso(), event_id),
        ).fetchall()
        return [r["directive_id"] for r in rows]

    def update_directive_status(self, directive_id: str, status: str) -> None:
        self.conn.execute(
            "UPDATE directives SET status=? WHERE directive_id=?",
            (status, directive_id),
        )

    def complete_directive_if_open(self, directive_id: str) -> None:
        """任务报完成时联动；已作废的指令保持 superseded，不被改回 completed。"""
        self.conn.execute(
            "UPDATE directives SET status='completed' WHERE directive_id=? "
            "AND status IN ('pending','dispatched')",
            (directive_id,),
        )

    def latest_task(self, event_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM tasks WHERE event_id=? ORDER BY rowid DESC LIMIT 1",
            (event_id,),
        ).fetchone()

    def task_for_directive(self, directive_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM tasks WHERE directive_id=?", (directive_id,)
        ).fetchone()

    def get_task(self, task_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()

    def insert_task(self, t: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO tasks(task_id,directive_id,event_id,dept_id,status,"
            "carry_of_task_id,dispatched_by,dispatched_at,request_id) "
            "VALUES(?,?,?,?,'dispatched',?,?,?,?)",
            (t["task_id"], t["directive_id"], t["event_id"], t["dept_id"],
             t.get("carry_of_task_id"), t["dispatched_by"], now_iso(),
             t["request_id"]),
        )

    def update_task_status(self, task_id: str, status: str, actor: str | None = None,
                           when: str | None = None) -> None:
        ts = when or now_iso()
        if status == "acked":
            self.conn.execute(
                "UPDATE tasks SET status='acked',acked_by=?,acked_at=? WHERE task_id=?",
                (actor, ts, task_id),
            )
        elif status == "completed":
            self.conn.execute(
                "UPDATE tasks SET status='completed',completed_by=?,completed_at=? "
                "WHERE task_id=?",
                (actor, ts, task_id),
            )
        else:
            self.conn.execute(
                "UPDATE tasks SET status=? WHERE task_id=?", (status, task_id)
            )

    def insert_roster_item(self, task_id: str, event_id: str, resident_id: str,
                           notification_status: str = "pending",
                           receipt_status: str = "unconfirmed",
                           confirmed_at: str | None = None,
                           receipt_code: str | None = None) -> None:
        self.conn.execute(
            "INSERT INTO roster_items(task_id,event_id,resident_id,notification_status,"
            "notified_at,receipt_status,confirmed_at,receipt_code) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (task_id, event_id, resident_id, notification_status,
             now_iso() if notification_status == "sent" else None,
             receipt_status, confirmed_at, receipt_code),
        )

    def roster_for_task(self, task_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM roster_items WHERE task_id=? ORDER BY resident_id",
            (task_id,),
        ).fetchall())

    def roster_item(self, task_id: str, resident_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM roster_items WHERE task_id=? AND resident_id=?",
            (task_id, resident_id),
        ).fetchone()

    def roster_item_by_code(self, receipt_code: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM roster_items WHERE receipt_code=?", (receipt_code,)
        ).fetchone()

    def roster_item_for_event_resident(self, event_id: str,
                                       resident_id: str) -> sqlite3.Row | None:
        """事件最新名单中的住户项（升级接续后落在最新任务上）。"""
        return self.conn.execute(
            "SELECT ri.* FROM roster_items ri JOIN tasks t ON t.task_id=ri.task_id "
            "WHERE ri.event_id=? AND ri.resident_id=? ORDER BY t.rowid DESC LIMIT 1",
            (event_id, resident_id),
        ).fetchone()

    def mark_notification_sent(self, item_id: int) -> None:
        self.conn.execute(
            "UPDATE roster_items SET notification_status='sent',notified_at=? "
            "WHERE id=? AND notification_status='pending'",
            (now_iso(), item_id),
        )

    def pending_notifications(self, zone_id: str | None = None) -> list[sqlite3.Row]:
        sql = ("SELECT ri.* FROM roster_items ri JOIN tasks t ON t.task_id=ri.task_id "
               "JOIN events e ON e.event_id=t.event_id WHERE ri.notification_status='pending'")
        args: list[Any] = []
        if zone_id:
            sql += " AND e.zone_id=?"
            args.append(zone_id)
        return list(self.conn.execute(sql, args).fetchall())

    def confirm_roster_item(self, item_id: int, receipt_code: str) -> None:
        self.conn.execute(
            "UPDATE roster_items SET receipt_status='confirmed',confirmed_at=?,"
            "receipt_code=? WHERE id=?",
            (now_iso(), receipt_code, item_id),
        )

    def roster_stats(self, task_id: str) -> dict[str, int]:
        row = self.conn.execute(
            "SELECT COUNT(*) AS total,"
            "SUM(CASE WHEN receipt_status='confirmed' THEN 1 ELSE 0 END) AS confirmed,"
            "SUM(CASE WHEN notification_status='pending' THEN 1 ELSE 0 END) AS pending_send "
            "FROM roster_items WHERE task_id=?",
            (task_id,),
        ).fetchone()
        return {
            "total": row["total"] or 0,
            "confirmed": row["confirmed"] or 0,
            "unconfirmed": (row["total"] or 0) - (row["confirmed"] or 0),
            "pending_send": row["pending_send"] or 0,
        }
