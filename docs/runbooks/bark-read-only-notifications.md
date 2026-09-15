# 自托管 Bark 只读通知

本入口只消费已经提交的 NEXUS Paper outbox 事实并发送脱敏通知。通知不能批准或拒绝订单，
不能改变业务状态，不能触发行情采集、模型调用、模拟盘或实盘写入，也不能修改
`execution_locked=true`。页面刷新仍只读取持久化事实。

第一版只接受仓库已有事件：Agent 决策持久化、批次评估、风控拒绝、订单提交、部分成交、成交和
取消。当前没有独立的订单失败 outbox event，因此不会制造“失败”通知。Bark 使用相同 outbox
event ID 更新同一通知；极端的发送成功/数据库确认前崩溃会重投，但不会堆出不同 ID 的通知。
该更新语义要求 Bark App 不低于 1.5.2；自托管 server 已固定为 2.3.5。

## 1. 边界和前置条件

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

## 2. 准备本地配置

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

## 3. 一次性 iPhone 注册

先在 `infra/.env.local` 把 `NEXUS_BARK_REGISTRATION_BIND_ADDRESS` 临时设置为 Mac 当前受信任
局域网地址，例如 `192.168.x.y`；不要使用示例地址原样运行。只启动 Bark Server：

```bash
docker compose --env-file infra/.env.local \
  -f infra/compose.yaml -f infra/compose.bark-register.yaml \
  --profile notifications up -d bark-server
```

在 Bark App 添加 `http://<Mac-LAN-IP>:8080`，完成服务器注册，并在 App 内设置与 `.env.local` 相同
的 AES-128-CBC key。把 App 注册后显示的 device key 写入 `NEXUS_BARK_DEVICE_KEY`，将
`NEXUS_BARK_ENABLED` 改为 `true`，并把 `NEXUS_BARK_ACTIVATED_AT` 设置为当前 UTC 时间。该时间是
领取下限：更早的 queued event 不会发送、不会改状态。

确认后立即把 bind address 恢复为 `127.0.0.1`，再用稳态配置强制重建：

```bash
docker compose --env-file infra/.env.local \
  -f infra/compose.yaml --profile notifications \
  up -d --force-recreate bark-server
```

稳态 `docker compose ps` 的 Bark Server 不应显示宿主机 published port。不要在聊天中发送 App
显示的 device key 或加密 key。

## 4. 健康检查与启动

先在容器内部检查 Bark，不发送 APNs 通知：

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml \
  --profile notifications exec bark-server wget -qO- http://127.0.0.1:8080/ping
```

预期为 `pong`。随后启动专用 worker：

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml \
  --profile notifications up -d bark-server bark-notifications
```

worker 只领取 `activated_at` 之后、白名单内的 Paper outbox。通知失败会按现有 outbox 最多三次
记录 `PUBLISHER_FAILED` 并最终 dead-letter；它不会重跑业务动作。断流、worker 退出或观察超时
不是投递成功，先按 `outbox_events → outbox_delivery_attempts` 核对状态，不重复触发决策或订单。

真实验收必须使用启用后自然产生的一条已批准内部模拟事实，并同时核对：业务事实已提交、对应
outbox event 为 published、delivery attempt 为 published、iPhone 实际显示解密后的脱敏内容。
缺少任何一项都不能宣称真实 iPhone/APNs 验收完成。

## 5. 关闭与回滚

先在 `infra/.env.local` 设置 `NEXUS_BARK_ENABLED=false`，再只停止本切片服务：

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml \
  --profile notifications stop bark-notifications bark-server
```

这不会删除 Paper 事实、outbox、attempt 或 Bark 注册数据。重新启用时设置新的 UTC
`NEXUS_BARK_ACTIVATED_AT`，避免发送停用期间的历史队列。`docker compose down -v` 会删除卷，
不属于普通回滚；没有用户另行确认不得执行。
