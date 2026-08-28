# NEXUS

NEXUS 是私人投资研究、回测与模拟交易工作台。它不提供投资建议，也不承诺收益。

## 当前状态

- `phase0_offline_ready`：美股研究、确定性回测、三方案比较、Forward Shadow、硬风控和内部模拟交易已通过离线发布门禁。
- Phase 1 只读代码已就绪，但复星外部门禁仍为 `fixture_with_next_action / COMPLETE_SDK_ADMISSION`；未安装被阻塞的 SDK、未读取历史暴露密钥，也未调用真实账户或 Provider。
- 美股真实数据传输与耐久采集链路已实现但默认关闭：Alpaca、SEC、FRED/ALFRED
  使用固定源、冻结 universe、原始证据归档和幂等入库；未配置或启用真实 Provider
  凭据时继续使用无网络 fixture。
- Phase 2 策略进化实验室已完成本地验收；候选策略只能经过确定性回测、Shadow 和治理门禁，Agent 不能自行晋级或交易。
- `Phase 3A offline_ready / Phase 3B external_activation_blocked`：券商模拟执行状态机只在无网络 fixture 中闭环，复星外部模拟写入未激活。
- 分钟级盘中监控保持 `Intraday Monitor offline_ready / external_data_activation_blocked`：只读 Snapshot、确定性波动指标和证据页面已实现，真实 Provider 请求仍默认关闭。
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
NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" \
  uv run pytest integrations/fosun_gateway/tests -q
```

## 本地运行与运维

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
