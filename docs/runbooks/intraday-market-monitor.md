# 分钟级盘中市场监控激活手册

## 当前裁决

`Intraday Monitor offline_ready / external_data_activation_blocked`

本状态表示本地 schema、只读采集链、确定性指标、API 和页面可以在 fixture/数据库证据上工作；不表示真实 Alpaca 行情权限已验证，也不授权启动 Provider。默认 `NEXUS_INTRADAY_MONITOR_MODE=disabled`，普通 `app` profile 不接收盘中配置或 Provider 凭据。

用户已声明先前在对话中暴露的旧 Key 已吊销。仓库无法独立证明券商侧状态，因此未来激活仍须使用全新凭据并完成下述 admission；不得恢复、测试或引用旧值。

## 固定能力边界

- 唯一外部操作：`GET https://data.alpaca.markets/v2/stocks/snapshots`。
- 固定查询键：`symbols`、`feed`、`currency=USD`；每批 1–30 个由数据库 authority 解析的美股 ticker。
- 固定调度：15 秒 bucket；超过 bucket 结束 45 秒的任务跳过，不补发。
- 页面只轮询 NEXUS 数据库，浏览器不直连 Alpaca，也不创建采集 Job。
- 采集只由 `nexus-engine-provider-worker` 从独立 `provider` 队列执行；通用 Agent worker 不加载 Provider handler、配置或凭据。
- Provider worker 必须使用独立 `PROVIDER_DATABASE_URL`；启动时拒绝 superuser、`BYPASSRLS`、任何角色继承、过宽表/列权限、缺失强制 RLS，或队列 policy 完整集合、角色、命令、`USING/WITH CHECK` 谓词及受控函数签名、源码、owner、ACL 不精确的角色。
- 全平台只有通用 worker 创建 `daily.pipeline` 根；启用日线 Provider 子任务时，由它显式设置 `NEXUS_PROVIDER_JOBS_ENABLED=true` 后跨队列物化，Provider worker 不创建第二个根。
- `iex` 或 `sip` 必须标记为 `real_time`；`delayed_sip` 必须标记为 `delayed`。
- 页面显示 `实时 IEX（仅 IEX）`、`实时 SIP（全市场）` 或 `延迟 SIP`，不能把 IEX 表示为全市场实时行情。
- 分钟 authority 是 `display_only_intraday_monitor`，不得进入 Agent、回测、策略晋级、硬风控、订单 sizing、模拟成交或券商命令。
- 本手册不激活复星读取、复星模拟盘或实盘。任何真实订单仍须逐笔人工确认。

## 激活前 admission 证据

必须把以下脱敏事实冻结到一次新的 admission package；任一缺失都保持 blocked：

1. Alpaca 账户的数据订阅与目标 feed entitlement，包含确认时间、套餐/成本和适用市场；不保存账户号或凭据。
2. 全新 Key 的创建时间、独立密钥域、只读数据权限和轮换/吊销负责人；Key 值只进入批准的 secret manager。
3. 目标 source/feed/delivery 组合及界面标签审查结果。
4. 数据保留依据：raw payload、内容寻址对象、quote/trade observation 和分钟 authority 的保存期限及容量预算。
5. 精确 watchlist authority 快照，必须为 1–30 个 USD 美股 ticker；不接受临时前端列表。
6. 固定出口、TLS、代理和 DNS 策略验证；实际响应正文与 header 不进入日志。
7. 一个 UTC 激活窗口、调用上限、预算/费用上限、操作人、观察人和立即停止负责人。
8. 通过离线 socket guard、Compose 隔离、全量测试、密钥扫描和恢复演练的当前提交 SHA。
9. 独立 Provider 数据库角色已由授权管理员按
   `infra/postgres/provider-runtime-grants.sql` 审核配置，且 startup probe 证明其不是
   superuser/`BYPASSRLS`，不能读取 Agent、Broker、账户或审批表。

外部数据激活窗口必须逐次批准。批准内容必须精确写明：账户/订阅范围、feed、UTC 起止、最多标的数、最大请求数、费用上限、允许的唯一 GET operation 和回滚负责人。不能用“继续”“试一下”或历史批准替代。

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
```

确认解析结果满足：

- `api` 与普通 `worker` 不含 `NEXUS_INTRADAY_*`、`ALPACA_INTRADAY_*` 或 Alpaca secret；
- 只有 `provider-worker` 接收盘中配置，且模板值仍为 `disabled/15/30`；
- `provider-worker` 只接收 `PROVIDER_DATABASE_URL`，不接收通用 `DATABASE_URL`；模板
  `NEXUS_PROVIDER_JOBS_ENABLED=false`，普通启动不会物化 Provider 日任务；
- broker、broker-simulation 和 Fosun Gateway 不含盘中配置；
- 当前提交与批准 package 的提交一致，工作树没有未审查差异。

真实凭据注入、将 mode 改为 `production`、启动 provider-worker 或发起首次 GET 都属于本手册的停止边界；在获得上述精确批准前不要执行。

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

## 回滚与保留影响

1. 停止精确的 `provider-worker`，将外部配置恢复为 `NEXUS_INTRADAY_MONITOR_MODE=disabled`；普通 app 可继续读取已持久化的最后证据并标记 stale/partial。
2. 吊销本次 Key 或移除 secret-manager grant 属于权限变更，须按批准流程执行并记录脱敏结果。
3. 不降级含事实的 `0025_intraday_market_monitor` 或其前向加固 `0026_intraday_review_hardening`。迁移会 fail closed；优先使用新的前向修复。
4. raw 对象、DataArtifact、quote/trade observation 和分钟 authority 是审计事实。停止采集不会删除它们；保留成本需在 admission 前纳入容量与费用预算。
5. 只有经独立确认的数据保留政策才能执行精确清理；不得使用广泛 volume、bucket 或数据库删除。

## 与交易能力的关系

盘中监控的完成不会解锁 Phase 3B、复星模拟写入或实盘。它只提供可追溯的显示证据；交易意图、确定性硬风控、人工审批和订单执行仍是隔离的权限域。任何真实订单继续逐笔确认，Agent 永远不能直接下单或修改硬风控。
