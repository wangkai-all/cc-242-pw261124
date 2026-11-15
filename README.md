# 接收工作台（pointwise-delivery）

来源压缩包同时携带**明细**与**更正**记录，解析规则会随时间升级。本工作台做到：

- **原始包只收一次**：按内容 sha256 去重；同包号不同内容拒绝（409）。
- **新规则重放**：规则是版本化的声明式 JSON，随时注册新版本并对全部已收包影子重放。
- **对照旧结果**：重放与现行快照逐记录 diff（新增/移除/字段级变更），网页上确认后再切换。
- **重放期间新包继续到达**：切换事务内用**新规则**对增量追平（catch-up），
  发布的每份数据集 100% 单一规则，绝不混入两套规则的半成品。
- **网页追溯**：每条结果可追到原包与规则版本；重放、差异确认、回滚都在网页上操作。

## 切换策略决策：影子核对后整体切换（不采用按批次切换）

| 维度 | 按批次切换 | 影子核对后整体切换（本系统） |
|---|---|---|
| 下游一致性 | 同一发布期内两套规则并存，下游必须理解批次边界，否则就是把半成品混成完整数据集 | 每次发布都是完整、单一规则的数据集，下游零负担 |
| 等待成本 | 省掉"追平增量"这一小段 | 重放在影子中进行，不阻塞现行发布与新包接入；切换本身是 O(1) 指针移动（新快照行已物化）。真实成本只是重放计算时间 |
| 失败回退 | 已流出的批次难以收回 | 影子期随时可弃；发布后回滚 = 按旧规则重算生成新快照，旧快照从未被改动 |
| 失败面 | 批次边界是持续的出错源 | 一次原子事务，没有边界问题 |

结论：按批次切换唯一省下的等待，恰恰以牺牲"不混半成品"这条硬约束为代价，因此采用**影子核对后整体切换**。

## 架构：两层存储

```
压缩包 ──收包──▶ 原始层（规则无关）            派生层（规则相关）
  zip           raw_events   事件日志(append-only)   snapshots  open(恰好一个)/sealed(只读)
  ├ manifest    raw_records  当前状态(物化缓存)      rows       每行带(快照,规则,原包,记录)
  ├ details     raw_pending  目标未到的更正/撤销     pointer    当前快照指针(单行)
  └ corrections                                  replay_runs/replay_rows  影子重放
```

- **原始层**负责明细、跨包更正、撤销与乱序（撤销先于原件 ⇒ 悬挂，原件到达时同事务解决，
  出生即撤销）。规则升级不影响原始层。
- **派生层**把规则应用到原始状态得到结果行。重放 = 按启动时冻结的高水位从
  `raw_events` 折叠重建状态，用新规则重算，写入独立的 `replay_rows`，与现行发布完全隔离。

## 关键不变式与并发语义

1. **收包原子**：一个包的事件、状态、悬挂、现行派生行在同一个 SQLite 事务内提交；
   坏包整体回滚，不留痕迹。
2. **发布 CAS**：`publish` 需带 `expected_snapshot_id`（网页还会带 `expected_high_water`）。
   两个重放同时完成时，只有一个能切换成功；另一个收到 409，必须基于新当前快照**重新预检**。
   预检后有新数据到达也会 409——操作员没看过的数据不会悄悄进入已确认数据集。
3. **崩溃恢复**：重启时仅把 `running` 重放标记为 `interrupted`（可续跑，幂等），
   写审计事件；**不触碰**任何已封存快照与指针。测试验证：收包/更正/重放/恢复/回滚
   之后，已封存行逐条哈希不变。
4. **回滚**：以目标封存快照的规则版本对当前全部原始状态重算，生成**新**开放快照；
   历史是 append-only 的，回滚本身也是一次可审计的发布。

## 运行

```bash
# 服务（仅标准库，Python 3.11+）
python3 -m workbench.server --db workbench.db --port 8000
# 打开 http://127.0.0.1:8000

# 本地发送器
python3 sender/sender.py demo                # 演示剧本：跨包更正/撤销先于原件/原件晚到
python3 sender/sender.py resend-duplicate    # 字节相同的重发 → duplicate
python3 sender/sender.py send-conflict       # 同包号不同内容 → 409
python3 sender/sender.py stream --n 8 --interval 2   # 间隔发送（模拟重放期间新包到达）

# 测试（18 个用例：去重/乱序/追平/CAS/恢复/回滚/不可变性/HTTP 冒烟）
python3 -m unittest discover -s tests -v
```

## 包格式（zip）

```
manifest.json      {"package_id": "PKG-0001", "seq": 1, "source": "..."}
details.jsonl      {"record_id","order_date","sku","qty","unit_price","currency","channel"}
corrections.jsonl  {"target","mode":"merge|replace|revoke","fields":{...},"reason":"..."}
```

## 规则格式（版本化 JSON）

```json
{
  "version": "v2",
  "key_field": "record_id",
  "transforms": [
    {"op": "rename", "from": "order_date", "to": "date"},
    {"op": "date_norm", "field": "date", "formats": ["%Y-%m-%d", "%d/%m/%Y", "%Y%m%d"]},
    {"op": "convert_currency", "field": "unit_price", "currency_field": "currency",
     "to": "CNY", "rates": {"CNY": 1.0, "USD": 7.2}, "out_field": "unit_price_cny"},
    {"op": "compute", "out_field": "amount_cny", "expr": "qty*unit_price_cny", "round": 2}
  ],
  "filters": [{"field": "channel", "op": "eq", "value": "test"}]
}
```

算子：`rename / upper / lower / round / date_norm / convert_currency / compute / set / drop_fields`；
过滤：`eq / neq / in / not_in / exists / missing`（任一命中即过滤）。
`compute` 表达式经 AST 白名单校验，只允许数字、字段引用与 `+ - * /`。

## API 摘要

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/packages` | 收包（body=zip 字节；重复→duplicate，冲突→409） |
| GET  | `/api/state` `/api/packages[/{id}]` `/api/events` | 总览/包详情/审计 |
| POST | `/api/rules` | 注册规则版本 `{spec, note}` |
| POST | `/api/replays` | 发起重放 `{rule_version}` |
| GET  | `/api/replays/{id}/diff` | 重放 vs 当前快照（同口径） |
| GET  | `/api/replays/{id}/preview` | 切换预检（含追平，全量） |
| POST | `/api/replays/{id}/publish` | 原子切换 `{expected_snapshot_id, expected_high_water?}` |
| POST | `/api/replays/{id}/interrupt` `/resume` | 中断/续跑 |
| POST | `/api/rollback` | 回滚 `{to_snapshot_id, expected_snapshot_id, reason}` |
| GET  | `/api/records/{rid}/trace` | 单记录全链路追溯 |
| GET  | `/api/snapshots/{id}/rows` | 快照行（分页/过滤） |

## 目录

```
workbench/    db.py(存储) rules.py(规则引擎) core.py(收包/重放/发布/回滚/恢复) server.py(HTTP)
sender/       sender.py(本地发送器)
web/          index.html app.js style.css(单页工作台)
tests/        test_workbench.py(18 个端到端用例)
```
