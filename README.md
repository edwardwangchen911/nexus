# NEXUS

NEXUS 是私人投资研究、回测与模拟交易工作台。它不提供投资建议，也不承诺收益。

## 当前状态

- `phase0_offline_ready`：美股研究、确定性回测、三方案比较、Forward Shadow、硬风控和内部模拟交易已通过离线发布门禁。
- Mac 本地的复星 `local_read_only` 链路已启用：现有 Gateway 经一次性本地刷新器将 1 个账户、
  3 个现金币种、12 条持仓、全部 12 个美股持仓的 Quote 和日 K 持久化到 PostgreSQL/对象存储，
  并通过 API/BFF 在本地组合页和可切换标的的市场页展示复星来源、更新时间与质量状态。组合页
  按原币种确定性展示现金、持仓市值、总资产与配置权重；没有合格 FX 证据时不生成跨币种合计，
  美股持仓可进入对应的已持久化 Quote 与日 K 页面。
- 该状态只是个人 Mac 本地只读使用，不代表生产部署、持续生产 Gateway、复星模拟盘交易或实盘激活。
- 美股真实数据传输与耐久采集链路已实现但默认关闭：Alpaca、SEC、FRED/ALFRED
  使用固定源、冻结 universe、原始证据归档和幂等入库；未配置或启用真实 Provider
  凭据时继续使用无网络 fixture。
- Phase 2 策略进化实验室已完成本地验收；候选策略只能经过确定性回测、Shadow 和治理门禁，Agent 不能自行晋级或交易。
- `Phase 3A offline_ready / Phase 3B external_activation_blocked`：券商模拟执行状态机只在无网络 fixture 中闭环，复星外部模拟写入未激活。
- 分钟级盘中监控保持 `Intraday Monitor offline_ready / external_data_activation_blocked`：只读 Snapshot、确定性波动指标和证据页面已实现；采集使用独立 `provider` 队列、Provider-only worker 和强制 RLS 数据库角色模板，真实 Provider 请求及角色授权仍默认关闭。
- `00656.HK` 继续仅作只读持仓展示；实盘页保持锁定，任何未来真实订单仍须逐笔人工确认。

最终本地证据和外部门禁见 [CI 与离线发布门禁](docs/runbooks/ci-release-gates.md) 与 [Task 21 审查](docs/superpowers/reviews/2026-08-27-task-21-final-operational-gates-review.md)。

## 命令

```bash
npm install
npm run lint
npm run typecheck
npm run test:unit
npm run test:e2e -- --workers=3
npm run build
uv run pytest engine/tests -q --import-mode=importlib
install -d -m 700 "$PWD/.nexus-pytest"
NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" \
  uv run pytest integrations/fosun_gateway/tests -q \
    --basetemp="$PWD/.nexus-pytest/gateway"
```

## 本地运行与运维

### Mac 本地复星只读刷新

在既有安全 launcher 已为 Gateway 与客户端加载同一组本地安全配置、且 Gateway 处于
`local_read_only` 后，宿主机可执行一次手动刷新：

```bash
uv run --package nexus-engine nexus-engine-fosun-local-refresh
```

刷新器从真实持仓确定性提取、去重并排序 `us...` 代码；最多接受 12 个美股标的，使用一次批量
Quote 和每个标的一次日 K 请求。Accounts、Cash、Holdings 合计各一次，单次总 Provider 请求最多
16 次，全部串行、无自动重试。超过范围、合同不匹配或任一读取失败时 fail-closed，数据库事务不会
覆盖上一份可用快照。浏览器只读取已持久化数据，不能触发 Gateway 或 Provider 请求；该入口不包含
自动调度、订单、账户写入或任何交易激活。组合页的“刷新页面数据”仅重新执行已持久化
组合与快照的 BFF GET，不会 POST `/api/v1/broker-sync-runs`。`00656.HK` 仍仅作只读持仓展示，不请求
Quote 或 K 线。

- [本地开发与 Compose](docs/runbooks/local-development.md)：不强制 Docker Desktop，Docker Desktop、OrbStack 或 Colima 均可。
- [备份/恢复演练](docs/runbooks/backup-restore.md)
- [CI 与离线发布门禁](docs/runbooks/ci-release-gates.md)
- [Provider 故障处置](docs/runbooks/provider-outage.md)
- [分钟级盘中市场监控激活门禁](docs/runbooks/intraday-market-monitor.md)
- [密钥与证书轮换](docs/runbooks/key-rotation.md)
- [复星 SDK admission](docs/runbooks/fosun-sdk-admission.md)
- [复星只读激活门禁](docs/runbooks/fosun-read-only-activation.md)
- [PostgreSQL 事务权威 ADR](docs/adr/0001-postgresql-owner.md)
- [复星独立只读网关 ADR](docs/adr/0002-fosun-gateway-boundary.md)

## 安全边界

- 首期仅支持研究、回测和模拟交易；实盘交易默认关闭，未来任何订单均须逐笔人工确认。
- `00656.HK` 仅作只读展示；Agent 只能创建可追溯的研究意图，不能下单或修改风控。
- 数据缺失、过期、未对账或证据不足时，系统应观望，不生成订单。
- 不要把密钥、券商凭据、账户数据或生产配置写入仓库、日志、测试快照或响应内容。
