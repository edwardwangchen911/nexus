# 分钟级盘中市场监控激活手册

## 当前裁决

`Intraday Monitor local_read_only / default_disabled`

本地 schema、只读采集链、确定性指标、API 和页面已可用。默认
`NEXUS_INTRADAY_MONITOR_MODE=disabled`；使用已有配置和已有只读权限启动本地 Provider
不需要逐次批准。新增凭据、扩大数据权限、付费升级或对外写入仍须先确认。

用户已声明先前在对话中暴露的旧 Key 已吊销。仓库无法独立证明券商侧状态，因此未来激活仍须使用全新凭据并完成下述 admission；不得恢复、测试或引用旧值。

## 固定能力边界

- 唯一外部操作：`GET https://data.alpaca.markets/v2/stocks/snapshots`。
- 固定查询键：`symbols`、`feed`、`currency=USD`；每批 1–30 个由数据库 authority 解析的美股 ticker。
- 固定调度：15 秒 bucket；超过 bucket 结束 45 秒的任务跳过，不补发。
- Alpaca Snapshot 每个 Job 只发起一次 HTTP 尝试；供应商失败后 60 秒内不创建新 bucket，
  不在传输层或跨 bucket 隐式放大请求。
- 页面只轮询 NEXUS 数据库，浏览器不直连 Alpaca，也不创建采集 Job。
- 采集只由 `nexus-engine-provider-worker` 从独立 `provider` 队列执行；通用 Agent worker 不加载 Provider handler、配置或凭据。
- Provider worker 保留独立进程、队列、凭据变量和 `provider/market` 对象前缀。
  Mac 本地默认复用已有数据库与 MinIO 身份；独立最小权限角色只是可选的部署加固。
- Provider 对 authority 输入保持 `SELECT`-only，且不读回测成员表；输入变更与事实提交由配对 transaction advisory lock 串行。两个最小 trigger-only `SECURITY DEFINER` 分别服务两个 publication `AFTER INSERT` trigger 和一个 frozen-bar `BEFORE INSERT` trigger；Provider/`PUBLIC` 不可直接执行或写发布标记，函数或 trigger topology 漂移即启动失败。
- 全平台只有通用 worker 创建 `daily.pipeline` 根；启用日线 Provider 子任务时，由它显式设置 `NEXUS_PROVIDER_JOBS_ENABLED=true` 后跨队列物化，Provider worker 不创建第二个根。
- `iex` 或 `sip` 必须标记为 `real_time`；`delayed_sip` 必须标记为 `delayed`。
- 页面显示 `实时 IEX（仅 IEX）`、`实时 SIP（全市场）` 或 `延迟 SIP`，不能把 IEX 表示为全市场实时行情。
- 分钟 authority 是 `display_only_intraday_monitor`，不得进入 Agent、回测、策略晋级、硬风控、订单 sizing、模拟成交或券商命令。
- 本手册不激活复星读取、复星模拟盘或实盘。任何真实订单仍须逐笔人工确认。

## 接入前核对

首次接入新账户/feed，或变更套餐、权限、凭据和数据保留范围时，先核对
entitlement、费用、source/feed/delivery 标签、watchlist 上限和回滚方式。日常使用已有
只读配置时复用这些结论，不按请求、分页或启动次数重复批准。

## 有界探测模板

下列是 admission 记录模板，不是可直接执行的命令，也不得填入秘密值后提交：

```yaml
provider: alpaca
operation: get_stock_snapshots
method: GET
origin: https://data.alpaca.markets
path: /v2/stocks/snapshots
feed: iex | sip | delayed_sip
delivery_mode: real_time | delayed
currency: USD
symbols_authority_id: <uuid>
symbol_count_limit: 1
request_limit: 2
utc_window_start: <approved UTC>
utc_window_end: <approved UTC, no more than 10 minutes later>
cost_limit: <approved amount and currency>
credential_reference: <secret-manager reference, never the value>
approver: <user approval evidence>
```

首次真实探测只能使用 1 个已批准标的、最多 2 个请求、最长 10 分钟。先验证响应 schema、来源标签、服务器时间与 raw-first 归档，再另行决定是否扩大到常规 1–30 标的采集。探测不能与 broker、复星 Gateway 或任何订单服务同窗启动。

## 启动前静态检查

这些命令只解析配置或运行离线测试，不会启动 Provider：

```bash
uv run --package nexus-engine pytest engine/tests/unit/ops/test_intraday_compose_policy.py -q
docker compose --env-file infra/env.example -f infra/compose.yaml --profile app config --quiet
docker compose --env-file infra/env.example -f infra/compose.yaml --profile provider config --quiet
docker compose --env-file infra/env.example -f infra/compose.yaml --profile provider-alpaca config --quiet
```

确认解析结果满足：

- `api` 与普通 `worker` 不含 `NEXUS_INTRADAY_*`、`ALPACA_INTRADAY_*` 或 Alpaca secret；
- 复星 `provider-worker` 与 Alpaca `provider-worker-alpaca` 互斥，不得同时领取
  `provider` 队列；复星最多 10 个标的，Alpaca 最多 30 个标的；
- 复星 worker 不接收任何 `ALPACA_*` 配置，Alpaca worker 不接收任何
  `FOSUN_MARKET_*` 网关身份；两者的默认盘中配置均为 `disabled/15`；
- `provider-worker` 优先使用 `PROVIDER_DATABASE_URL`/`PROVIDER_OBJECT_STORE_*`；未设置时
  复用通用本地数据库与对象存储配置。模板 `NEXUS_PROVIDER_JOBS_ENABLED=false`；
- broker、broker-simulation 和 Fosun Gateway 不含盘中配置；
- 当前提交与批准 package 的提交一致，工作树没有未审查差异。

已有受保护配置和只读权限下，启动 provider-worker 和常规 GET 是正常本地操作。
新增凭据、扩大账户/数据权限、付费升级或外部写入仍是停止边界。

## 运行观察与停止条件

激活窗口内只观察脱敏计数、result code、延迟、缺失标的数、对象/authority 哈希和 Job 状态。以下任一情况立即停止 provider-worker，并保持最后证据的原状态：

- entitlement、401/403、feed/delivery 或 source label 与 admission 不一致；
- 429、费用或请求预算达到上限；
- 返回未知 ticker、未知字段、重复 JSON key、float/NaN/Infinity、超过 8 MiB 或时间不为 aware UTC；不超过 transport 上限的 malformed 响应必须先归档 raw 再稳定跳过，超过 8 MiB 则在网络硬边界拒绝；
- bid 高于 ask、负值、无效 OHLC、未来 bar、不完整分钟、来源交叉或 authority/hash 不一致；
- raw-first 归档失败、对象版本缺失、数据库事务部分可见、重复 observation 未幂等；
- 发生 broker/Fosun/Agent/risk/paper 的意外依赖或任何非 GET 网络请求；
- 日志、响应或页面出现凭据、账户数据或未脱敏 Provider 错误。

停止后不自动切回 fixture，不补发过期 bucket，不删除或改写已落库事实，也不扩大 source policy。

供应商传输失败分别落为 `INTRADAY_PROVIDER_UNAVAILABLE`、
`INTRADAY_PROVIDER_STATUS_REJECTED` 或 `INTRADAY_PROVIDER_POLICY_REJECTED`，不再折叠为
`HANDLER_FAILED`。若数据库没有覆盖当天的完整 XNYS 日历 authority，调度器以
`INTRADAY_CALENDAR_AUTHORITY_MISSING` 明确失败；已有完整覆盖但当天休市或尚未开盘时仍正常空操作。

## 回滚与保留影响

1. 停止精确的 `provider-worker`，将外部配置恢复为 `NEXUS_INTRADAY_MONITOR_MODE=disabled`；普通 app 可继续读取已持久化的最后证据并标记 stale/partial。
2. 吊销本次 Key 或移除 secret-manager grant 属于权限变更，须按批准流程执行并记录脱敏结果。
3. 不降级含事实的 `0025_intraday_market_monitor` 或其前向加固 `0026_intraday_review_hardening`。迁移会 fail closed；优先使用新的前向修复。
4. raw 对象、DataArtifact、quote/trade observation 和分钟 authority 是审计事实。停止采集不会删除它们；保留成本需在 admission 前纳入容量与费用预算。
5. 只有经独立确认的数据保留政策才能执行精确清理；不得使用广泛 volume、bucket 或数据库删除。

## 与交易能力的关系

盘中监控的完成不会解锁 Phase 3B、复星模拟写入或实盘。它只提供可追溯的显示证据；交易意图、确定性硬风控、人工审批和订单执行仍是隔离的权限域。任何真实订单继续逐笔确认，Agent 永远不能直接下单或修改硬风控。
