# NEXUS 分钟级盘中市场监控独立审查与验收记录

**审查日期：** 2026-08-29
**审查范围：** `0025/0026` schema、Provider 采集与调度、数据库读取、API/Web、Compose 和激活手册
**安全结论：** `Intraday Monitor offline_ready / external_data_activation_blocked`

## 1. 发布裁决

本地实现只提供可追溯的盘中显示证据，不形成交易信号，也不进入 Agent、回测、
硬风控、模拟成交或订单域。外部 Provider 请求、真实凭据、Provider 数据库角色授权、
部署、复星写操作和实盘均未执行。

用户声明此前暴露的旧 Key 已吊销；这是用户提供的状态，仓库无法独立验证。后续激活
必须使用全新凭据，并重新完成 entitlement、费用、权限、UTC 窗口和有界 GET 探测审批。

## 2. 独立审查发现与处置

| 编号 | 级别 | 审查发现 | 复核结果与处置 |
| --- | --- | --- | --- |
| R1 | Important | 新 delivery 约束会拒绝历史 Alpaca `snapshot` 行 | 属实。约束只对新 `trade/quote` 执行严格配对，历史 snapshot 可迁移但不进入盘中 loader。 |
| R2 | Important | 直接 `0026 -> 0025` 可能先移除追加保护再发现事实 | 属实。downgrade 在任何 schema 变更前检查盘中事实并 fail closed。 |
| R3 | Important | 当前分钟会随 decision time 推移被误判为完整 | 属实。Provider 与数据库 authority 双重要求 `received_at >= observed_at + 1 minute`。 |
| R4 | Important | 全空 Snapshot 跳过 ingestion，丢失 raw 证据 | 属实。所有有效 HTTP 响应都进入 raw-first ingestion；无规范化事实时只保存 raw。 |
| R5 | Important | verified quote 只校验 artifact source，未绑定 digest | 属实。`0026` preflight/insert trigger 与 loader 同时校验 revision 中的 SHA-256。 |
| R6 | Important | manifest 与 artifact 可能可变 | 不属实。`0008_diagnostics` 已存在无条件 UPDATE/DELETE 防护；未重复创建冲突触发器。 |
| R7 | Important | Provider 进程与通用 worker 共用数据库权限域 | 属实。新增独立 `PROVIDER_DATABASE_URL`、精确表/列权限与实际 RLS 谓词 startup probe，以及强制 RLS 授权模板；模板未执行。 |
| R8 | Important | Provider 与通用 worker 各自创建 daily root，跨队列顺序丢失 | 属实。保留唯一通用 `daily.pipeline` 根，由它显式跨队列物化 Provider 子任务。 |
| R9 | Important | 升级前已排队的 Provider Job 会滞留在 general 队列 | 属实。`0026` 只搬迁 `queued` 任务；terminal 历史保留原执行队列，running 任务拒绝迁移；downgrade 对 Provider terminal/heartbeat 历史 fail closed。 |
| R10 | Important | latest quote 窗口排名扫描整个历史表 | 属实。查询增加 instrument、90 分钟、PIT、合法来源配对、证据状态和 artifact digest 约束。 |
| R11 | Minor | GET 前重验仍有响应期间 TOCTOU | 属实。GET 后再次读取数据库 authority；漂移时只归档 raw，不写 quote/bar。 |
| R12 | Minor | authority 未限制 quality flag 形态与数量 | 属实。数据库拒绝超过 16 个或不符合规范错误码的 flag。 |
| R13 | Self-review | 底层 API 仍允许 Provider 队列创建第二个 daily root，通用 worker 仍注册 Provider 占位 handler | 属实。repository、registry 和 scheduler 固定 daily root 为 general；通用 worker只物化 Provider 子任务，不注册其 handler。 |
| R14 | Self-review | Provider role probe 未覆盖 search path、schema CREATE 和扩展表权限 | 属实。连接固定 `public,pg_catalog`，拒绝可建库/建角色/复制、`BYPASSRLS`、schema CREATE 以及 `TRUNCATE/REFERENCES/TRIGGER/MAINTAIN`。 |
| R15 | Self-review | 既有仓库策略将隔离的 Phase 3A simulation fixture 误判为只读 Gateway 写面 | 属实。只豁免三个精确审查路径，近似路径继续失败；策略测试和全仓扫描均通过。 |
| R16 | Critical | quote/trade PIT 只看 `available_at`，未约束 `received_at` | 属实。数据库 loader 与 Market API 同时约束 observation/artifact 的 `available_at`、`received_at`，1Min bar 也按 `received_at` 重验；新增决策时刻夹在可用与接收之间的回归。 |
| R17 | Important | malformed Snapshot 在解析失败前无法归档 | 属实。Provider transport 返回独立 raw envelope；handler 先创建 raw artifact，再严格解析。解析失败不发布业务事实，并以 `INTRADAY_SNAPSHOT_PAYLOAD_INVALID` 稳定跳过。 |
| R18 | Important | Provider heartbeat 使 downgrade 在不可删除 trigger 处意外失败 | 属实。移除不可执行 DELETE，在任何 schema 变化前以 `P3D01` 明确拒绝 Provider heartbeat。 |
| R19 | Important | 迁移改写 terminal Provider Job 的历史执行队列 | 属实。队列迁移限定 `status='queued'`；terminal 历史不改写，Provider terminal 历史阻止 downgrade。 |
| R20 | Important | Provider startup 只检查 policy 名称，无法识别 `USING (true)` | 属实。现在精确核验 policy role、`ALL`、permissiveness、`qual/with_check`，并校验四个不可由 Provider 修改的 policy 函数定义、波动性、search path 与 invoker 属性。 |
| R21 | Minor | 非数组 `quality_flags` 产生不稳定 PostgreSQL 错误 | 属实。先检查 JSONB 类型，再在受保护的 `CASE` 内检查数量、元素类型和规范码；数据库回归固定为 `23514`。 |
| R22 | Critical | 只核验命名 policy 会遗漏额外 PUBLIC policy 或错配表名的同名 policy | 属实。startup 枚举五张共享运行表的完整 policy 集并要求每表精确两条；授权模板在任何权限变更前按 `(table, name)` 精确拒绝额外 policy。 |
| R23 | Important | policy helper 的 owner/ACL/重载或 Provider 角色继承可绕过应用级隔离 | 属实。startup 拒绝 helper 重载、错误 owner/source/属性、PUBLIC/额外 ACL、异常 grantor、转授权以及 Provider 的任何角色继承；授权模板也在首个 GRANT 前拒绝高权限属性、owner 复用、角色继承和 helper 重载。 |

复核还发现并修复两个迁移实现问题：全新安装没有已被 `0020` 替换的旧 Agent Job
触发器；以及 jobs 队列约束恢复语句误放在 upgrade helper 中。最终迁移只切换实际存在的
触发器，并在 downgrade 才恢复旧队列约束。

## 3. 权限和调度不变量

- 普通 worker 是唯一 daily root owner；`NEXUS_PROVIDER_JOBS_ENABLED` 默认 `false`。
- Provider worker 只注册 `collect.market`、`collect.sec_macro`、
  `collect.intraday_snapshot`，且只领取 `provider` 队列。
- Provider 角色可只读查看 `daily.pipeline` 父任务，但 `WITH CHECK` 只允许写 Provider
  子任务；Job 子表通过强制 RLS 绑定 Provider Job。startup probe 精确核验 policy 谓词和
  policy 函数签名、源码、owner 与精确 ACL，不能用额外 PUBLIC policy、错配表名、同名
  overload 或 `USING (true)` 策略通过门禁。
- Provider 连接固定 `public,pg_catalog` search path；进程拒绝 superuser、
  `BYPASSRLS`、可建库/建角色/复制、任何角色继承、schema CREATE、缺失 schema、任何
  额外表权限、RLS 未强制或 policy 缺失的数据库角色。
- `infra/postgres/provider-runtime-grants.sql` 不创建角色、不设置凭据，且未在本次验收
  中执行；真实授权仍是人工审批门禁。

## 4. 数据与时点不变量

- `received_at` 在 HTTP 返回后产生；future fact、未完成分钟和过期 bucket 均拒绝；
  API/loader 对 observation、artifact 和 bar 同时执行 `available_at/received_at` PIT 门禁。
- transport 在 JSON 解析前返回 raw envelope；不超过 8 MiB 的 malformed/空事实响应先
  内容寻址归档，再以稳定 Job result 跳过，且不产生 quote/trade/bar。
- 原始响应按 payload SHA-256 内容寻址，可跨 delivery bucket 复用；每次 observation
  保留自己的接收和可用时间。
- verified trade/quote 必须绑定同 source、同 digest 的 raw artifact。
- quote/trade 和 1Min authority 均为追加事实；发布后的 bar 集合不可补写。
- API 只读取 decision time 可见的最近 90 分钟事实，整组选择单一
  source/feed/delivery，不跨源拼接。

## 5. 本地验证证据

最终结果已按待提交工作树冻结：

- Python 非集成测试（offline socket guard）：`1102 passed in 53.08s`；
- PostgreSQL 全量集成测试：`842 passed in 2388.81s (0:39:48)`，使用
  `--import-mode=importlib`；
- 复星只读网关测试：`157 passed`；
- Web 单元测试：`480 passed`；Playwright：`85 passed`；vinext build 通过；
- Ruff、Mypy、ESLint、TypeScript 与生成契约一致性检查均通过；
- app/provider/broker Compose 静态解析均通过；
- 仓库策略：`10 passed`，全仓依赖/密钥/镜像/CI 扫描
  `dependency_count=761, finding_count=0`；
- 三轮独立安全复审的最终残余发现为
  `Critical=0, Important=0, Minor=0`；
- `nexus_test_*` 临时数据库残留为 0；任务容器与网络已移除，fixture 环境文件已删除，
  未删除任何 volume，其他 Compose 项目未触碰。

所有验证均在本地 fixture/隔离 PostgreSQL 上运行；未执行 Provider 授权模板、外部请求、
付费激活、复星/券商账户操作或真实订单。

## 6. 未解除门禁

1. Provider 数据库角色创建与授权模板执行；
2. 全新只读凭据的 secret-manager 注入；
3. Alpaca feed entitlement、费用和数据保留预算确认；
4. 有界 UTC 激活窗口中的最多 1 symbol / 2 requests GET 探测；
5. 任何部署、复星账户操作、模拟券商写入或真实订单。

以上任一项都不能由本地测试、历史批准或本记录替代。
