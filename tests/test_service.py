"""渝陕跨省暴雨会商的端到端行为测试。

覆盖：
1. 区域授权隔离（重庆队不能处置陕西风险区）；
2. 规则引擎：窗口聚合、样本数、区域/全域规则择优；
3. 观测去重（source_ref）与请求重试（request_id）落到同一事件；
4. 自动立案、升级、旧指令自动作废、派单接续；
5. 同一户不重复通知，已确认户升级后不再通知；
6. 回执去重（receipt_code）、重复请求回放；
7. 进程崩溃后用同一 DB 文件重建服务，未送达通知可续办；
8. 值班视图：确认情况、等待处置、升级原因、操作人可追溯。
"""
import json
import os
import tempfile
import unittest

from app.contracts import (
    Request, ServiceError, SUBMIT_OBSERVATION,
)
from app.service import HazardResponseService
from app.storage import Store


def req(actor, action, payload, rid):
    return Request(actor, action, payload, rid)


class ChongqingShaanxiScenarioTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "response.db")
        self.svc = HazardResponseService(self.db_path)
        self.sent = []  # 捕获通知调用

        def spy_notifier(task_id, item):
            self.sent.append((task_id, item["resident_id"]))
            return True

        self.svc._notifier = spy_notifier

    def tearDown(self):
        self.svc.close()
        self.tmp.cleanup()

    def _bootstrap(self):
        s = self.svc.handle
        # 重庆队、陕西队分别只授权本省
        s(req("admin", "register_zone", {
            "zone_id": "CQ-WX-01", "name": "巫溪中梁乡边坡",
            "admin_area": "重庆/巫溪"}, "b1"))
        s(req("admin", "register_zone", {
            "zone_id": "SX-AK-07", "name": "安康镇坪县沟谷",
            "admin_area": "陕西/镇坪"}, "b2"))
        s(req("admin", "grant_area", {
            "dept_id": "cq-team", "dept_name": "重庆地灾防治队",
            "admin_area": "重庆", "members": ["cq01", "cq02"]}, "b3"))
        s(req("admin", "grant_area", {
            "dept_id": "sx-team", "dept_name": "陕西安康地灾队",
            "admin_area": "陕西", "members": ["sx01"]}, "b4"))
        # 规则：巫溪站 60 分钟累计雨量 >= 50 -> 黄；全域规则 >= 90 -> 橙
        s(req("admin", "put_rule", {
            "rule_id": "cq-yellow", "zone_id": "CQ-WX-01",
            "metric": "rain_1h", "window_minutes": 60, "min_samples": 1,
            "aggregate": "sum", "threshold": 50, "target_level": "yellow"}, "b5"))
        s(req("admin", "put_rule", {
            "rule_id": "global-orange", "metric": "rain_1h",
            "window_minutes": 60, "min_samples": 2,
            "aggregate": "sum", "threshold": 90, "target_level": "orange"}, "b6"))
        # 转移对象：两户人家，其中一户已确认过黄色
        s(req("cq01", "upsert_resident", {
            "zone_id": "CQ-WX-01", "resident_id": "fam-wang",
            "name": "王友田", "phone": "13800000001"}, "b7"))
        s(req("cq01", "upsert_resident", {
            "zone_id": "CQ-WX-01", "resident_id": "fam-li",
            "name": "李桂兰", "phone": "13800000002"}, "b8"))

    # ---- 1. 授权隔离 ------------------------------------------------------
    def test_01_jurisdiction_isolation(self):
        self._bootstrap()
        with self.assertRaises(ServiceError) as cm:
            self.svc.handle(req("sx01", "upsert_resident", {
                "zone_id": "CQ-WX-01", "resident_id": "x", "name": "越权"}, "t1"))
        self.assertEqual(cm.exception.code, "out_of_jurisdiction")
        with self.assertRaises(ServiceError) as cq:
            self.svc.handle(req("cq01", "submit_observation", {
                "zone_id": "SX-AK-07", "metric": "rain_1h", "value": 999,
                "observed_at": "2026-10-03T08:00:00Z",
                "source_ref": "sx-obs-1"}, "t2"))
        self.assertEqual(cq.exception.code, "out_of_jurisdiction")
        # 非管理员不能授权
        with self.assertRaises(ServiceError) as cm:
            self.svc.handle(req("cq01", "grant_area", {
                "dept_id": "cq-team", "admin_area": "陕西"}, "t3"))
        self.assertEqual(cm.exception.code, "forbidden")
        # 省队对本县可见，dashboard 只能看到自己省的风险区
        dash = self.svc.handle(req("sx01", "dashboard", {}, "t4"))
        visible = {z["zone_id"] for z in dash.data["zones"]}
        self.assertEqual(visible, {"SX-AK-07"})

    # ---- 2. 规则引擎 ------------------------------------------------------
    def test_02_rule_engine_window_and_scope(self):
        self._bootstrap()
        # min_samples=2 的橙规则：单条 100 也不触发橙
        r1 = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 100,
            "observed_at": "2026-10-03T08:00:00Z",
            "source_ref": "o1", "dept_id": "cq-team"}, "t5"))
        self.assertEqual(r1.state, "yellow")
        self.assertEqual(r1.data["trigger"]["rule_id"], "cq-yellow")
        # 10 分钟后再来 5（窗口内 sum=105，样本=2，触发橙）
        r2 = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 5,
            "observed_at": "2026-10-03T08:10:00Z",
            "source_ref": "o2"}, "t6"))
        self.assertEqual(r2.state, "orange")
        # 61 分钟后旧值滑出窗口，sum 仅 5，不能触发任何东西
        r3 = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 5,
            "observed_at": "2026-10-03T09:01:00Z",
            "source_ref": "o3"}, "t7"))
        self.assertEqual(r3.state, "orange")  # 等级只升不降
        self.assertNotIn("升级", r3.message)
        event_id = r1.data["event_id"]
        history = self.store_levels(event_id)
        self.assertEqual([h["to_level"] for h in history], ["yellow", "orange"])

    def store_levels(self, event_id):
        return list(self.svc.store.conn.execute(
            "SELECT * FROM event_levels WHERE event_id=? ORDER BY id", (event_id,)))

    # ---- 3. 观测去重 + 请求重试 ------------------------------------------
    def test_03_observation_dedup_and_retry(self):
        self._bootstrap()
        first = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 60,
            "observed_at": "2026-10-03T08:00:00Z",
            "source_ref": "dup-1", "dept_id": "cq-team"}, "p1"))
        # 网络重试：同一 request_id，返回同一结果
        replay = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 60,
            "observed_at": "2026-10-03T08:00:00Z",
            "source_ref": "dup-1", "dept_id": "cq-team"}, "p1"))
        self.assertEqual(first, replay)
        # 上游换 request_id 重推同一报文：dedup 并指向原事件
        retyped = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 60,
            "observed_at": "2026-10-03T08:00:00Z", "source_ref": "dup-1"}, "p2"))
        self.assertTrue(retyped.data["deduplicated"])
        self.assertEqual(retyped.data["event_id"], first.data["event_id"])
        # 物理上只有一条观测
        count = self.svc.store.conn.execute(
            "SELECT COUNT(*) c FROM observations WHERE source_ref='dup-1'").fetchone()["c"]
        self.assertEqual(count, 1)
        # 通知只发了一次（每户一条）
        self.assertEqual(len(self.sent), 2)

    # ---- 4/5. 升级接续：旧指令作废、不重复通知、确认保持 ------------------
    def test_04_escalation_carryover_no_duplicate_notice(self):
        self._bootstrap()
        r1 = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 60,
            "observed_at": "2026-10-03T08:00:00Z",
            "source_ref": "e1", "dept_id": "cq-team"}, "u1"))
        event_id = r1.data["event_id"]
        first_task = self.latest_task(event_id)
        # 王友田确认黄色转移
        self.svc.handle(req("cq01", "record_receipt", {
            "zone_id": "CQ-WX-01", "resident_id": "fam-wang",
            "receipt_code": "RC-20261003-01"}, "u2"))
        # 会商手动升橙并联动同一责任组
        esc = self.svc.handle(req("cq01", "escalate_event", {
            "zone_id": "CQ-WX-01", "target_level": "orange",
            "reason": "上游水位超警戒，连夜加密会商决定升级",
            "dept_id": "cq-team"}, "u3"))
        self.assertEqual(esc.state, "orange")
        second_task = self.latest_task(event_id)
        self.assertNotEqual(first_task, second_task)
        self.assertEqual(
            self.svc.store.get_task(second_task)["carry_of_task_id"], first_task)
        # 旧指令已自动作废，新指令 dispatched
        dirs = {d["directive_id"]: d["status"]
                for d in self.svc.store.directives_for_event(event_id)}
        self.assertIn("superseded", set(dirs.values()))
        self.assertIn("dispatched", set(dirs.values()))
        # 整个过程只通知过两户一次（升级不再打扰已通知/已确认户）
        residents_notified = sorted(r for _, r in self.sent)
        self.assertEqual(residents_notified, ["fam-li", "fam-wang"])
        # 新名单上：王友田仍确认；李桂兰已通知但未确认，等待其回执
        second_roster = {
            r["resident_id"]: (r["notification_status"], r["receipt_status"])
            for r in self.svc.store.roster_for_task(second_task)}
        self.assertEqual(second_roster["fam-wang"], ("sent", "confirmed"))
        self.assertEqual(second_roster["fam-li"], ("sent", "unconfirmed"))

    def latest_task(self, event_id):
        return self.svc.store.latest_task(event_id)["task_id"]

    # ---- 6. 回执去重 ------------------------------------------------------
    def test_05_receipt_dedup_and_latest_roster_routing(self):
        self._bootstrap()
        r1 = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 60,
            "observed_at": "2026-10-03T08:00:00Z",
            "source_ref": "f1", "dept_id": "cq-team"}, "v1"))
        event_id = r1.data["event_id"]
        ok = self.svc.handle(req("cq01", "record_receipt", {
            "zone_id": "CQ-WX-01", "resident_id": "fam-wang",
            "receipt_code": "RC-X"}, "v2"))
        self.assertEqual(ok.state, "confirmed")
        # 同一确认短信被两个网关各推一次（不同 request_id）
        again = self.svc.handle(req("cq01", "record_receipt", {
            "zone_id": "CQ-WX-01", "resident_id": "fam-wang",
            "receipt_code": "RC-X"}, "v3"))
        self.assertTrue(again.data["deduplicated"])
        # 升级后迟到的回执仍落到事件最新名单
        self.svc.handle(req("cq01", "escalate_event", {
            "zone_id": "CQ-WX-01", "target_level": "orange",
            "reason": "雨强持续", "dept_id": "cq-team"}, "v4"))
        late = self.svc.handle(req("cq01", "record_receipt", {
            "zone_id": "CQ-WX-01", "resident_id": "fam-li",
            "receipt_code": "RC-Y"}, "v5"))
        self.assertEqual(late.state, "confirmed")
        latest = self.latest_task(event_id)
        stats = self.svc.store.roster_stats(latest)
        self.assertEqual((stats["total"], stats["confirmed"]), (2, 2))

    # ---- 7. 崩溃恢复 ------------------------------------------------------
    def test_06_crash_recovery_pending_notifications_continue(self):
        self._bootstrap()
        # 通道故障：通知发不出去
        self.svc._notifier = lambda task_id, item: False
        r1 = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 60,
            "observed_at": "2026-10-03T08:00:00Z",
            "source_ref": "g1", "dept_id": "cq-team"}, "w1"))
        self.assertEqual(r1.accepted, True)
        task_id = self.latest_task(r1.data["event_id"])
        # 模拟进程被杀：丢弃服务实例，只留 DB 文件
        self.svc.close()
        reborn = HazardResponseService(self.db_path)
        try:
            # 事件/等级/名单全部还在
            event_row = reborn.store.get_open_event("CQ-WX-01")
            self.assertIsNotNone(event_row)
            self.assertEqual(event_row["current_level"], "yellow")
            pending = reborn.store.pending_notifications()
            self.assertEqual(len(pending), 2)
            # 重启后对同一派单请求的重试：精确回放，不重建任务
            replay = reborn.handle(req("cq01", "submit_observation", {
                "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 60,
                "observed_at": "2026-10-03T08:00:00Z",
                "source_ref": "g1", "dept_id": "cq-team"}, "w1"))
            self.assertEqual(replay.state, "yellow")
            tasks = [r["task_id"] for r in reborn.store.conn.execute(
                "SELECT task_id FROM tasks")]
            self.assertEqual(tasks, [task_id])
            # 通道恢复，值班台发起续办
            sent_after_reboot = []
            reborn._notifier = lambda t, i: sent_after_reboot.append(i["resident_id"]) or True
            done = reborn.handle(req("system", "retry_pending_notifications",
                                    {}, "w2"))
            self.assertEqual(done.data["sent"], 2)
            self.assertEqual(sorted(sent_after_reboot), ["fam-li", "fam-wang"])
            # 再次发起续办（新的请求；旧请求 w2 重放只会回放首次结果）
            done2 = reborn.handle(req("system", "retry_pending_notifications",
                                     {}, "w3"))
            self.assertEqual(done2.data["sent"], 0)
            pending_after = reborn.store.pending_notifications()
            self.assertEqual(pending_after, [])
        finally:
            reborn.close()

    # ---- 8. 值班视图 ------------------------------------------------------
    def test_07_duty_dashboard_shows_who_what_why(self):
        self._bootstrap()
        r1 = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 60,
            "observed_at": "2026-10-03T08:00:00Z",
            "source_ref": "d1", "dept_id": "cq-team"}, "x1"))
        event_id = r1.data["event_id"]
        task_id = self.latest_task(event_id)
        # 一户确认
        self.svc.handle(req("cq01", "record_receipt", {
            "zone_id": "CQ-WX-01", "resident_id": "fam-wang",
            "receipt_code": "RC-Z1"}, "x2"))
        dash = self.svc.handle(req("cq01", "dashboard", {}, "x3")).data["zones"]
        wx = next(z for z in dash if z["zone_id"] == "CQ-WX-01")
        kinds = {p["kind"]: p for p in wx["pending_actions"]}
        self.assertIn("awaiting_ack", kinds)          # 责任组未接单
        self.assertIn("awaiting_confirmation", kinds)  # 李桂兰未确认
        self.assertEqual(kinds["awaiting_confirmation"]["count"], 1)
        self.assertEqual(wx["residents"]["confirmed"], 1)
        # 升级原因可追溯
        self.assertIn("cq-yellow", wx["escalations"][0]["reason"])
        self.assertEqual(wx["escalations"][0]["by"], "cq01")
        # 接单 -> 待办变为处置中
        self.svc.handle(req("cq01", "ack_task", {"task_id": task_id}, "x4"))
        wx = self.zone_view("cq01", "x5")
        kinds = {p["kind"] for p in wx["pending_actions"]}
        self.assertIn("awaiting_completion", kinds)
        self.assertNotIn("awaiting_ack", kinds)
        # 全区域待办视图：本队能看到该待办，陕西队看不到
        cq_pending = self.svc.handle(req("cq01", "pending_work", {}, "x5b")).data["items"]
        self.assertTrue(any(i["zone_id"] == "CQ-WX-01" for i in cq_pending))
        sx_pending = self.svc.handle(req("sx01", "pending_work", {}, "x5c")).data["items"]
        self.assertFalse(any(i["zone_id"] == "CQ-WX-01" for i in sx_pending))
        # zone_detail 含审计：操作人可查
        detail = self.svc.handle(req("cq01", "zone_detail", {
            "zone_id": "CQ-WX-01"}, "x6")).data
        actors = {a["actor"] for a in detail["audit"]}
        self.assertIn("cq01", actors)
        summaries = " ".join(a["summary"] for a in detail["audit"])
        self.assertIn("立案", summaries)

    def zone_view(self, actor, rid):
        dash = self.svc.handle(req(actor, "dashboard", {}, rid)).data["zones"]
        return next(z for z in dash if z["zone_id"] == "CQ-WX-01")

    # ---- 手动指令与派单 ---------------------------------------------------
    def test_08_manual_directive_dispatch_lifecycle(self):
        self._bootstrap()
        # 没规则的指标不能自动立案；先手动立案替代：用观测触发但不给 dept
        r0 = self.svc.handle(req("cq01", "submit_observation", {
            "zone_id": "CQ-WX-01", "metric": "rain_1h", "value": 60,
            "observed_at": "2026-10-03T08:00:00Z", "source_ref": "m1"}, "y1"))
        event_id = r0.data["event_id"]
        # 此时应等待指令
        z = self.zone_view("cq01", "y2")
        self.assertTrue(any(p["kind"] == "awaiting_directive"
                            for p in z["pending_actions"]))
        d = self.svc.handle(req("cq01", "issue_directive", {
            "event_id": event_id, "title": "黄色预警：组织低洼地带转移"}, "y3"))
        directive_id = d.data["directive_id"]
        # 派给未授权该区域的队伍被拒
        with self.assertRaises(ServiceError) as cm:
            self.svc.handle(req("cq01", "dispatch_task", {
                "directive_id": directive_id, "dept_id": "sx-team"}, "y4"))
        self.assertEqual(cm.exception.code, "out_of_jurisdiction")
        t = self.svc.handle(req("cq01", "dispatch_task", {
            "directive_id": directive_id, "dept_id": "cq-team"}, "y5"))
        self.assertEqual(t.state, "dispatched")
        task_id = t.data["task_id"]
        # 重复派单被状态机拒绝（解决"已升级指令仍显示待处理"）
        with self.assertRaises(ServiceError) as cm:
            self.svc.handle(req("cq01", "dispatch_task", {
                "directive_id": directive_id, "dept_id": "cq-team"}, "y6"))
        self.assertEqual(cm.exception.code, "directive_not_open")
        # 未接单不能直接完成
        with self.assertRaises(ServiceError) as cm:
            self.svc.handle(req("cq01", "complete_task",
                                {"task_id": task_id}, "y7"))
        self.assertEqual(cm.exception.code, "invalid_transition")
        # 责任组成员接单、完成；事件待办转为等待解除
        self.svc.handle(req("cq02", "ack_task", {"task_id": task_id}, "y8"))
        self.svc.handle(req("cq02", "complete_task", {"task_id": task_id}, "y9"))
        z = self.zone_view("cq01", "y10")
        self.assertTrue(any(p["kind"] == "awaiting_close"
                            for p in z["pending_actions"]))
        closed = self.svc.handle(req("cq01", "close_event", {
            "zone_id": "CQ-WX-01"}, "y11"))
        self.assertEqual(closed.state, "closed")


class StoreOnlyTest(unittest.TestCase):
    def test_area_covers_hierarchy(self):
        from app.storage import area_covers
        self.assertTrue(area_covers("重庆", "重庆/巫溪"))
        self.assertTrue(area_covers("重庆/巫溪", "重庆/巫溪"))
        self.assertFalse(area_covers("重庆/巫山", "重庆/巫溪"))
        self.assertFalse(area_covers("陕西", "重庆/巫溪"))
        # 防止前缀误判：重庆/巫 不应覆盖 重庆/巫溪
        self.assertFalse(area_covers("重庆/巫", "重庆/巫溪"))


if __name__ == "__main__":
    unittest.main()
