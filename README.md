# 地质灾害响应后端

面向跨省暴雨会商的风险区预警与转移回执后端：把分散的雨量观测按**适用地区
与规则**转换成可升级预警，将转移任务派给被授权区域的责任组，收集居民确认，
并保证重复报文归并、崩溃后可续办、全程操作可追溯。

## 能力一览

- **规则驱动的研判**：规则可绑定单个风险区、一个行政区域（省级规则按
  层级覆盖辖区县区，如 `重庆` 覆盖 `重庆/巫溪`），或作为全域兜底；支持
  时间窗口、最少样本数与 latest/sum/max/avg/min 聚合。
- **事件与升级**：观测达到阈值自动立案、发布预警（蓝→黄→橙→红，只升不
  降）；支持会商人工升级。每次升级记录原因、命中规则、观测值、操作人与
  时间，旧指令自动作废并联动新指令、新派单。
- **授权隔离**：部门按行政区域授权，成员只能查看/处置授权区域内的风险
  区；派单目标责任组也必须被授权该区域。
- **转移接续、不重复打扰**：升级产生的新名单沿同一事件接续——已确认户
  保持确认且不再通知，已通知未确认户不重复通知，从未送达的户继续等待。
- **两层幂等**：
  - 请求级 `request_id`：网络重试精确回放首次结果，无重复副作用；
  - 业务去重键：观测 `source_ref` 重复推送归并到原事件，回执
    `receipt_code` 重复到达只认第一次。
- **崩溃恢复**：全部状态在 SQLite（WAL）中，进程重启后用同一数据库文件
  重建即可；通知在事务提交后尽力送达，失败项保持 `pending`，由
  `retry_pending_notifications` 续办。
- **值守视图**：按风险区查看谁已确认、哪项处置在等待（待派单/待接单/
  处置中/通知未送达/待确认/待解除）、指令为何升级、每步由谁操作。

## 目录

- `app/contracts.py`：请求/结果约定、动作常量、预警等级、业务错误。
- `app/storage.py`：SQLite schema、可重入事务、仓储查询。
- `app/rules.py`：规则校验、适用范围匹配、窗口聚合研判。
- `app/service.py`：核心状态机与全部业务动作。
- `app/api.py`：stdin/JSON 命令行入口（支持批量与续办）。
- `tests/`：渝陕会商端到端行为测试。

## 测试

```bash
python3 -m unittest discover -s tests
python3 -m compileall app
```

## CLI 用法

数据默认保存在 `--db` 指定的 SQLite 文件（也可用环境变量 `HAZARD_DB`）。

```bash
# 单条请求
echo '{"actor":"cq01","action":"dashboard","payload":{},"request_id":"q1"}' \
    | python3 -m app.api --db response.db

# 批量：JSON 数组 / {"requests":[...]} / NDJSON
cat requests.jsonl | python3 -m app.api --db response.db

# 进程重启后，续发所有未送达的转移通知
python3 -m app.api --db response.db --recover --recover-actor system
```

## 一次完整会商的动作序列

| 动作 | 说明 |
| --- | --- |
| `register_zone` | 登记风险区（zone_id、名称、行政区域） |
| `grant_area` | 管理员授权部门负责某行政区域并登记成员 |
| `upsert_resident` | 维护风险区转移对象名单 |
| `put_rule` | 配置预警规则（区域/全域、窗口、聚合、阈值、目标等级） |
| `submit_observation` | 上报观测；自动研判、立案/升级，可带 `dept_id` 联动派单 |
| `escalate_event` | 会商决定人工升级，记录原因，可联动重新派单 |
| `issue_directive` / `dispatch_task` | 下达处置指令、派给授权责任组 |
| `ack_task` / `complete_task` | 责任组接单、反馈处置完成（仅本组成员） |
| `record_receipt` | 登记居民确认回执（按回执码去重，迟到回执落最新名单） |
| `retry_pending_notifications` | 崩溃恢复后续发未送达通知 |
| `close_event` | 会商解除，关闭事件 |
| `dashboard` / `zone_detail` / `pending_work` | 值守视图、单区明细（含升级履历与审计）、全区域待办 |

初始管理员账号为 `admin`，系统续办账号为 `system`（二者可见全部区域）。
