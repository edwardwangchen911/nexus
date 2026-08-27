# Task 21 最终离线发布、运维与复星只读门禁审查

**当前结论：** 代码与安全审查中的 Critical/Important 未解决项为 `0/0`，Task 21 本地运行验收已关闭，当前可以声明 `phase0_offline_ready`。Phase 1 只读代码在无凭据状态下通过，但真实复星激活仍严格为 `fixture_with_next_action / COMPLETE_SDK_ADMISSION`；这不是 `activated_read_only`，也不授权任何 Provider 或账户调用。

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
| 浏览器 E2E | Chromium 64/64（1.5 分钟）；覆盖七档视口/横屏、200% 缩放、键盘/焦点/表格、Agent abstention、源冲突、STALE/对账失败、部分成交、回撤锁和两个锁定实盘页。 |
| Gateway offline | loopback-only guard 自测通过，guard 下 145/145；Ruff 和 Mypy 19 个源文件通过。没有 provider 请求。 |
| Engine 无数据库 | unit 704/704（34.28 秒）；Ruff 和 Mypy 234 个源文件通过。 |
| Engine 空 PostgreSQL | 从空库顺序迁移 `0001 -> 0021_manual_job_retry_allowance`，全量 1432/1432（23:00），独立动态回环端口/tmpfs 容器残留 0。 |
| 备份/恢复 | 加固后成功 fixture 演练：dump 736,450 bytes、1 对象/245 bytes，backup 0.388s、restore 0.446s、verify 0.093s，fixture RPO 0.177s/RTO 0.539s；migration head、表计数、账本、READY/绩效/审计根、对象哈希全部通过，database/object/container residue 0。 |
| Policy/供应链 | 12/12 policy；761 个第三方依赖/0 finding；uv lock、两 Compose profile、workflow YAML 通过；完整 npm audit 与 pip-audit 均为 0。 |
| 复星激活 | 在 offline guard 下当前结果为 `fixture_with_next_action`，`next_action=COMPLETE_SDK_ADMISSION`，contract hash `62223818f1a331308ea882cfc729a7e81baa2461d48d9d4b6cba2cdf9190ae32`。 |
| 清理/密钥 | 本轮演练容器前后 0/0；repository policy 密钥 finding 0；未加载历史暴露 key。其他会话的既有容器未停止、删除或修改。 |

## 4. Docker runtime 恢复与隔离证据

- Docker client/server 均为 29.7.2，`docker info`、镜像 digest、Compose config、已有容器与 `docker inspect` 均正常。
- 一次早期有界最小探针复现了新容器无法启动；最终门禁前，同名策略的新精确探针在 pinned `python:3.12-slim`、`--network none` 下成功退出 0 并自动无残留，因此无需重启 Docker Desktop。
- 其他任务留下的三个 `Created` 容器经只读 inspect 均显示旧的 5432/9000 端口冲突，不是本轮目标；没有停止、删除、启动或修改它们。三个已有健康容器也保持运行。
- 最终空库容器使用独立名称、任务所有权标签、动态回环端口和 tmpfs；演练容器使用 128-bit run ID、精确标签/名称。两条成功路径结束后的本任务容器残留均为 0。

## 5. 最终本地结论、外部门禁与回滚

1. Phase 0 离线研究、回测、Shadow、硬风控和内部模拟交易达到 `phase0_offline_ready`；这只描述本地可复现代码/运维门禁，不承诺收益、生产 RPO/RTO 或外部部署状态。
2. Phase 1 read-only code complete without credentials；真实激活仍需先关闭 SDK 许可/测试向量/独立安全证据、确认历史 key 已撤销并直接轮换到 gateway secret domain、固定 egress/IP allowlist 和客户端密钥，再对那一次只读 probe 单独取得确认。
3. 任一外部证据失败、artifact 过期/错配、TLS surface 漂移或 base URL 不一致都继续返回 fixture/503，不安装 SDK、不发 Provider 请求、不回退到不受控数据源。

本次只有本地可回滚代码、文档和提交。停止本地进程即阻止新任务；备份演练只删除 label/名称/前缀/数据库都精确匹配本次 128-bit run ID 的对象。未来任何复星网络 probe、凭据/权限变更、部署或实盘订单仍须单独确认；真实订单继续逐笔人工确认。
