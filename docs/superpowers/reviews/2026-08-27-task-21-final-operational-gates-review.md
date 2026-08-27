# Task 21 最终离线发布、运维与复星只读门禁审查

**当前结论：** 代码与安全审查中的 Critical/Important 未解决项为 `0/0`，但 Task 21 最终运行验收尚未关闭。本机 Docker Desktop 能够解析镜像、读取状态和维持已有容器，但任意新容器均停在 `Created`。因此当前不声明 `phase0_offline_ready`；必须在可启动新容器的 runtime 上重跑空 PostgreSQL 全量和加固后备份/恢复成功演练。复星结论仍严格为 `fixture_with_next_action / COMPLETE_SDK_ADMISSION`。

## 1. 审查范围与方法

- 范围：Task 21 规格/实现 `ec147ba^..53edda9` 及本轮纠正差异；主要对象为 E2E/可访问性、Compose/密钥域、readiness、对象存储初始化、备份恢复、CI/供应链、复星 admission/激活和 SDK 运行边界。
- 方法：实现完成后以已提交差异为输入，进行一次单执行者分离式对抗审查；专项查看容器所有权、清理对象、命令有界性、CI 外网与 artifact、SDK 安装/激活绕过、TLS/重定向、base URL 绑定、能力过期、密钥扫描绕过和真实路由。
- 本次未启用委派，因此不是第二人或第二模型背书。每个修正先保留失败反例，再重跑定向与全量受影响门禁。
- 边界：没有读取或调用复星凭据/账户/Provider，没有券商写操作、真实订单、部署、密钥/权限变更、外部消息、付费或 Git 历史改写。

## 2. 已修复发现

| 级别 | 发现 | 修复与反例 |
| --- | --- | --- |
| Important | 备份演练在 `start` 失败后无条件停止随机同名容器；极低概率的命名碰撞可以越过“只清理自己”边界。 | 启动前拒绝既有名称，两容器写入精确 `nexus.drill.run_id` label；cleanup 只在 label 恒等时执行精确 `docker rm --force --volumes <name>`，外来 label 反例确认不会发出 rm。 |
| Important | 所有 Docker/Alembic 子命令无 timeout；本机新容器停在 `Created` 的真实故障会使演练无限挂起，`finally` 无法执行。 | 通用子命令上限 30 秒，迁移 120 秒。在当前 Docker 故障下实际运行稳定退出 1、只输出通用错误，演练容器 `before=0 / after=0`。 |
| Important | `nexus-fosun-activation-gate` 验证 probe，但 composition root 只检查 `FOSUN_SDK_ADMITTED`/manifest；未提供 probe 也可进入 SDK import，且长驻进程或限流排队可能越过能力到期后仍发起读取。 | runtime 必须读取脱敏 artifact，重算并常量时间比较 `FOSUN_ACTIVATION_CONTRACT_HASH`；缺失/哈希错配/过期/未解决 license 均在 SDK import 前拒绝。adapter 在操作入口及限流/排队后的每次 Provider 尝试前重新检查到期。 |
| Important | 固定 SDK 不仅普通请求有 `verify=False`，Session 握手还有第二处模块级 `requests.post(..., verify=False)`；非空 remediation 字符本身不会修正 TLS。 | gateway-owned 适配器精确覆盖两个已审查调用面，强制 `verify=True` 且 `allow_redirects=False`；任一私有结构漂移都在发网前拒绝。policy 同时防止两处修复被删除或 gateway 源码重现 `verify=False`。 |
| Important | 已批准的 manifest/probe 未绑定 runtime API base URL，理论上可把 key 指向其他合法 HTTPS 主机。 | manifest 新增当前为 `null` 的 `admission.provider_base_url`；未来 admitted 必须是无凭据/query/fragment/重定向依赖/歧义端口/尾斜杠的精确 HTTPS URL，runtime 值须逐字节匹配。manifest hash 因而同时绑定该值。 |
| Important | Engine CI 只跑 PostgreSQL 全量，没有运行已实现的 PostgreSQL＋MinIO 备份恢复演练。 | `engine-postgres` 增加 fixture-only drill，job 上限调整为 40 分钟；workflow policy 要求该命令必须存在。未推送，因此 GitHub-hosted 结果尚不存在。 |
| Important | 密钥策略对大于 2 MiB 或非 UTF-8 的 tracked 文件静默跳过。 | 改为读取全部跟踪/未忽略文件的字节并容错解码；2 MiB＋非 UTF-8 中的构造 API key 反例稳定被报告，不输出 key 内容。 |

## 3. 当前验证证据

| 门禁 | 结果 |
| --- | --- |
| Web | ESLint、TypeScript、generated client、production build 退出 0；39 个文件/424 unit 通过。 |
| 浏览器 E2E | Chromium 64/64（1.3 分钟）；覆盖七档视口/横屏、200% 缩放、键盘/焦点/表格、Agent abstention、源冲突、STALE/对账失败、部分成交、回撤锁和两个锁定实盘页。 |
| Gateway offline | loopback-only guard 自测通过，guard 下 145/145；Ruff 和 Mypy 19 个源文件通过。没有 provider 请求。 |
| Engine 无数据库 | unit 704/704（34.28 秒）；Ruff 和 Mypy 234 个源文件通过。 |
| Engine 空 PostgreSQL | 纠正前最近完整证据为 1428/1428（22:52）且容器残留 0；本轮新增 4 个 Engine unit 及容器所有权/timeout 修正后无法在当前 Docker runtime 上重跑，故不视为最终证据。 |
| 备份/恢复 | 纠正前最近成功 fixture 演练：dump 736,456 bytes、1 对象/244 bytes，backup 0.237s、restore 0.428s、verify 0.091s，fixture RPO 0.132s/RTO 0.520s，残留 0。加固后当前故障路径在 30 秒内失败且容器 `0 → 0`；仍需成功路径重跑。 |
| Policy/供应链 | 12/12 policy；761 个第三方依赖/0 finding；uv lock、两 Compose profile、workflow YAML 通过；本轮未改依赖，最近完整 npm audit 与 pip-audit 均为 0。 |
| 复星激活 | 在 offline guard 下当前结果为 `fixture_with_next_action`，`next_action=COMPLETE_SDK_ADMISSION`，contract hash `62223818f1a331308ea882cfc729a7e81baa2461d48d9d4b6cba2cdf9190ae32`。 |
| 清理/密钥 | 本轮演练容器前后 0/0；repository policy 密钥 finding 0；未加载历史暴露 key。其他会话的既有容器未停止、删除或修改。 |

## 4. Docker runtime 阻塞证据

- Docker client/server 均为 29.7.2，`docker info`、镜像 digest、Compose config、已有容器与 `docker inspect` 均正常。
- 精确命名、`--network none`、本地已有的 pinned `python:3.12-slim` 最小容器仍停在 `Created`，`StartedAt` 为零值，入口进程从未执行。
- 本轮精确探针容器和演练容器均已删除；未重启 Docker Desktop，因为重启会停止其他会话正在使用的容器，其 tmpfs 数据可能丢失。

## 5. 关闭条件与回滚

1. 在不影响其他任务的前提下恢复 Docker-compatible runtime；不要删除或复用其他会话容器。
2. 从清洁工作树重跑空库 migration＋Engine 全量，以及加固后 backup/restore 成功路径；要求临时 database/object/container residue 全部为 0。
3. 重跑受本轮纠正影响的 Gateway/Policy/Compose，再更新主计划、progress ledger 和本文档结论；只有届时才声明 `phase0_offline_ready`。

本次只有本地可回滚文件差异。停止本地进程即阻止新任务；备份演练只删除 label/名称/前缀/数据库都精确匹配本次 128-bit run ID 的对象。未来任何复星网络 probe、凭据/权限变更、部署或实盘订单仍须单独确认；真实订单继续逐笔人工确认。
