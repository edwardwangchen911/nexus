# 自托管 Bark 只读通知

本入口只消费已经提交的 NEXUS Paper outbox 事实并发送加密的可行动通知。通知不能批准或拒绝订单，
不能改变业务状态，不能触发行情采集、模型调用、模拟盘或实盘写入，也不能修改
`execution_locked=true`。页面刷新仍只读取持久化事实。

领取白名单包括以下已闭合事件：

- `paper.order.risk_rejected@paper_event_v1`：风控拒绝；
- `paper.execution.advance.filled@paper_execution_v1`：最终成交；
- `paper.execution.advance.cancelled@paper_execution_v1` 与
  `paper.execution.cancelled@paper_execution_v1`：最终取消；
- `paper.session.summary.ready@paper_session_summary_v1`：交易日日终简报。

Agent 决策持久化、批次评估、订单提交和部分成交不会被 Bark 领取，保持给其余 outbox 消费者。
待处理/部分成交的延迟合并仍未实现。历史或无法形成完整
`notification_snapshot` 的终态事件会以 `notification_delivery=unavailable` 明确跳过，不回查账户或
制造补充通知。当前没有独立的订单失败 outbox event，因此不会制造“失败”通知。Bark 使用订单 ID
更新同一通知；极端的发送成功/数据库确认前崩溃会重投，但不会堆出不同 ID 的通知。该更新语义要求
Bark App 不低于 1.5.2；自托管 server 已固定为 2.3.5。

## 1. 通知内容与隐私

每条即时通知从对应 outbox 的事件时 `notification_snapshot` 渲染：标题显示内部模拟、代码、买卖方向、
股数和结果；正文按结果显示参考价或成交均价、计划/成交金额、费用、拒绝或取消原因、成交后或未变化的
持仓与可用现金，以及可用时的 Agent 理由和失效条件。订单通知 `id` 是该订单 ID，链接（若配置）只会打开只读
`/paper`。

日终简报复用正常绩效发布入口：XNYS 收盘后，收盘绩效快照与专用 outbox 事件在同一事务提交，
且当天至少有一个 Agent Paper 周期才产生。完全未运行时不产生空简报；绩效失败或事务回滚时不产生
摘要。已有快照重放不补写摘要、不查询后来事实重做历史；部署前的历史快照不追补。

同一内部模拟账户、市场、交易日使用稳定 Bark ID，投递重试更新同一条通知，outbox/attempt 保留
各自审计。正文不展示账户 ID。周期按决策交易日统计，完成包括订单全部终态的 `awaiting_market`
周期；不可用包括 `unavailable`/`failed`，未完成单列，合理不交易仅计明确 `NO_ORDER_REQUIRED`。
订单和风控拒绝按当天批次评估时间统计，成交笔数及费用按当天成交事实统计，全部截止收盘。

净值和回撤取收盘绩效；当日盈亏为相邻交易日收盘净值之差，日收益率分母是前一交易日净值。
没有该前日快照时，两项均显示“暂不可用”。`hold_current` 是当前现金起始模型的初始现金持有对照；
SPY 复用已有同窗口、同初始资金比较，不可比或缺合格日线时显示“暂不可用”。累计对照单列起点与
分母，不把累计收益当作日收益。运行状态截至一次数据库发布时间，包含收盘后已完成的终态处理；
SPY 可使用收盘后、摘要冻结前已持久化的合格当日日线。两类时点分别展示，发送时不再读业务事实。

该切片不新增定时器：只有现有日终绩效入口实际成功发布才会产出摘要；运行故障独立提醒仍未实现。
无需数据库迁移。回退应用版本会停止生产和领取该新事件，已提交绩效与 outbox/attempt 均保留。

这些字段足以让用户在不下单的前提下判断是否需要打开页面，因此内容并非完全移除代码、持仓或金额。
在 iPhone 的锁屏通知预览中建议选择“解锁时”或“从不”，并避免在共享设备上显示预览。Bark 和 NEXUS
不会在发送时查询当前价格、持仓、账户或研究结论；后续变化不得回填既有通知。

## 2. 边界和前置条件

- Bark App 与自托管 server 的注册会把 APNs device token 写入 Bark 自己的 `bark-data` 卷。
- 稳态 Compose 不发布 Bark 端口；NEXUS worker 与 Bark Server 共用网络命名空间，只调用
  `http://127.0.0.1:8080/push`。
- 首次注册时 iPhone 必须临时访问 Mac。只在受信任局域网使用 Mac 的明确 LAN 地址，不绑定
  `0.0.0.0`，不做路由器端口转发、Cloudflare Tunnel 或公网反向代理。完成后立即恢复稳态。
- Bark 官方加密设置选择 AES-128-CBC；key 必须与 `infra/.env.local` 中的值一致且恰好 16 个 ASCII 字符。
- device key 与加密 key 只写入受保护且 Git 忽略的 `infra/.env.local` 和 Bark App，不写入命令参数、聊天、日志、数据库或 Git。

官方协议依据：[Bark V2 API](https://github.com/Finb/bark-server/blob/master/docs/API_V2.md)、
[加密说明](https://github.com/Finb/Bark/blob/master/docs/en-us/encryption.md)、
[稳定 ID 语义](https://github.com/Finb/Bark/blob/master/docs/en-us/tutorial.md)和
[注册端点](https://github.com/Finb/bark-server/blob/master/route_register.go)。

## 3. 准备本地配置

用本机受保护编辑器修改 Git 忽略的 `infra/.env.local`。不要把真实值粘贴到聊天或命令参数。
注册前先保持通知 worker 关闭，选择一段 16 位 ASCII 共享密钥；device key 和启用时间暂时留空：

```bash
chmod 0600 infra/.env.local
```

```dotenv
NEXUS_BARK_ENABLED=false
NEXUS_BARK_DEVICE_KEY=
NEXUS_BARK_ENCRYPTION_KEY=<与 Bark App 相同的 16 位 ASCII 共享密钥>
NEXUS_BARK_ACTIVATED_AT=
NEXUS_BARK_READ_ONLY_URL=
```

若 iPhone 不能只读访问 NEXUS `/paper`，保持链接为空。

## 4. 一次性 iPhone 注册

先在 `infra/.env.local` 把 `NEXUS_BARK_REGISTRATION_BIND_ADDRESS` 临时设置为 Mac 当前受信任
局域网地址，例如 `192.168.x.y`；不要使用示例地址原样运行。只启动 Bark Server：

```bash
docker compose -p nexus-fosun-live --env-file infra/.env.local \
  -f infra/compose.yaml -f infra/compose.bark-register.yaml \
  --profile notifications up -d bark-server
```

在 Bark App 添加 `http://<Mac-LAN-IP>:8080`，完成服务器注册，并在 App 内设置与 `.env.local` 相同
的 AES-128-CBC key。把 App 注册后显示的 device key 写入 `NEXUS_BARK_DEVICE_KEY`，将
`NEXUS_BARK_ENABLED` 改为 `true`，并把 `NEXUS_BARK_ACTIVATED_AT` 设置为当前 UTC 时间。该时间是
领取下限：更早的 queued event 不会发送、不会改状态。

确认后立即把 bind address 恢复为 `127.0.0.1`，再用稳态配置强制重建：

```bash
docker compose -p nexus-fosun-live --env-file infra/.env.local \
  -f infra/compose.yaml --profile notifications \
  up -d --force-recreate bark-server
```

稳态 `docker compose ps` 的 Bark Server 不应显示宿主机 published port。不要在聊天中发送 App
显示的 device key 或加密 key。

## 5. 健康检查与启动

先在容器内部检查 Bark，不发送 APNs 通知：

```bash
docker compose -p nexus-fosun-live --env-file infra/.env.local -f infra/compose.yaml \
  --profile notifications exec bark-server wget -qO- http://127.0.0.1:8080/ping
```

预期为 `pong`。随后启动专用 worker：

```bash
docker compose -p nexus-fosun-live --env-file infra/.env.local -f infra/compose.yaml \
  --profile notifications up -d bark-server bark-notifications
```

worker 只领取 `activated_at` 之后、白名单内的 Paper outbox。通知失败会按现有 outbox 最多三次
记录 `PUBLISHER_FAILED` 并最终 dead-letter；它不会重跑业务动作。断流、worker 退出或观察超时
不是投递成功，先按 `outbox_events → outbox_delivery_attempts` 核对状态，不重复触发决策或订单。

真实验收必须使用启用后自然产生的一条已批准内部模拟事实，并同时核对：业务事实已提交、对应
outbox event 为 published、delivery attempt 为 published、iPhone 实际显示解密后的通知内容。
缺少任何一项都不能宣称真实 iPhone/APNs 验收完成。

## 6. 关闭与回滚

先在 `infra/.env.local` 设置 `NEXUS_BARK_ENABLED=false`，再只停止本切片服务：

```bash
docker compose -p nexus-fosun-live --env-file infra/.env.local -f infra/compose.yaml \
  --profile notifications stop bark-notifications bark-server
```

这不会删除 Paper 事实、outbox、attempt 或 Bark 注册数据。重新启用时设置新的 UTC
`NEXUS_BARK_ACTIVATED_AT`，避免发送停用期间的历史队列。`docker compose down -v` 会删除卷，
不属于普通回滚；没有用户另行确认不得执行。

## 明确测试通知

独立通信测试只在用户明确授权后执行。先保存一个新的测试UUID，随后通过正常入口提交一次：

```bash
.venv/bin/python -m nexus.ops.notification_test --repo-root "$PWD" \
  --env-file infra/.env.local enqueue --test-id "$TEST_NOTIFICATION_ID"
.venv/bin/python -m nexus.ops.notification_test --repo-root "$PWD" \
  --env-file infra/.env.local read --test-id "$TEST_NOTIFICATION_ID"
```

`diagnostic.notification.test@diagnostic_notification_v1` 只使用既有受保护Bark配置，
不接受目的地、自由正文、业务时间或重试参数。标题及正文固定标注“NEXUS 测试通知｜非投资预警”。
只登记一条outbox及审计，不生成市场触发、研究或订单；原领取起点及业务通知策略不变。
正常消费者在外部发送前持久化attempt，本事件上限为一次；超时、崩溃及确认丢失均不自动重发。
调用结果不明时只read原UUID，不换键、直接调用provider或重建事件。

`queued` 只代表登记；`provider_accepted` 只代表本地Bark服务接受；发送中或无法确认结果为
`unknown`，即使outbox已经dead_letter也不证明设备未收到。设备实收单独确认。
回滚保留outbox/审计；旧消费者忽略新类型，不能修改旧领取起点或补发历史通知。

### 投递失败的安全诊断

新失败保留原 `PUBLISHER_FAILED`、尝试上限、退避和送达状态，在持有当前有效租约的失败结案事务内，
追加 `outbox.delivery.failure@outbox_delivery_failure_v1` 审计。只保存事件身份、尝试序号、固定阶段
（payload / encryption / transport / response / unknown）及白名单原因，不保存异常文本、响应正文、
地址、凭据或设备标识。连接、读取、写入和连接池超时分别记录；HTTP 拒绝、响应解码失败与服务商拒绝
分别记录。诊断不改变重试资格，也不能证明消息未被远端接受。

正常诊断通知 `read` 回执增加 `failure`。无原始细分审计的历史记录返回 `null`；冲突或不合约的审计
只显示 unknown，不推断或回填旧错误。一次测试通知即使发生超时、确认丢失或租约过期，仍保持未知且
最多一次尝试。审计写入失败时结案同事务回滚，后续原租约按既有规则处理，不生成新的投递许可。

本变更不新增数据库迁移、依赖、通知目的地或事件领取范围。回滚应用可继续读取原 outbox/attempt，
新增审计保留；旧应用忽略新增诊断事实，不应删除审计或重新排队以“恢复”通知。
