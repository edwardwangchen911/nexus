# 本地备份恢复 runbook

## 真实业务备份的隔离恢复

`python -m nexus.ops.local_recovery` 接受已有受保护备份目录，在随机命名、动态回环端口的独立 PostgreSQL 容器中实际执行恢复。它不连接或修改业务数据库，不启动应用、任务或交易；原 fixture 演练入口见下文。

```bash
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.local_recovery \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资 \
  --backup /Users/wangchen/Documents/ChatGPT/投资/outputs/backups/pre-0034-20260902T170730Z
```

前提是本机 Docker、已缓存的固定 PostgreSQL 17 镜像，以及 `--repo-root` 下现有本地对象存储配置。配置仅在内存使用。备份目录必须为 0700，`database.dump` 和 `manifest.json` 为 0600；结果与对象副本也只写入该目录下独占的 `recovery-<run_id>/`，不得作为 fixture、测试输入或提交 Git。

入口按以下顺序执行，任一步失败即停止，不自动重放：

1. 校验 dump 的 SHA-256、长度和备份 manifest；manifest 必须包含 `provider_capabilities`、`portfolio_snapshots`、`price_series_manifests`、`price_bar_records` 四张表的计数/哈希。
2. 拒绝远程 Docker，只通过本机 Unix socket 新建带本轮所有权标签、无持久 volume、数据使用 tmpfs、关闭容器日志的随机容器。目标端口不得与业务数据库相同；恢复前核验目标无已有关系或额外 schema。
3. 再校验传入容器的 dump，执行 `pg_restore --no-owner --no-privileges --exit-on-error --single-transaction`。不运行新迁移；恢复的 schema 和四张表须与备份时点一致。
4. 扫描恢复数据库的 S3 引用，通过同一 URI 关联原始记录的哈希/长度；派生引用可以不重复长度，但其哈希必须一致。仅从现有本机对象存储按精确版本读取，校验版本、SHA-256 和长度，复制为 0600 本地文件，再离线读回校验。单对象上限 64 MiB，总量上限 512 MiB、最多 10000 个，超过时停止而非默默跳过。
5. 不改写历史 URI，私有 `object-bindings.json` 保存原始 URI 与本地副本的对应关系；`result.json` 只保存安全状态、时间和汇总。终端不打印对象 URI、账户内容、密码或异常原文。
6. 成功或失败均仅清理本轮标签恒等的精确容器；清理失败即整体失败，保留主错误和清理错误。原始备份及本轮私有结果保留，不覆盖、不递归删除。

这一步验证数据库内容及数据库引用对象的可恢复性。原 `pre-0034` 备份只有数据库；对象是在恢复核验时从仍可用的本机对象存储取得。未被数据库 URI 引用的原始文件不在本片范围内，不能宣称完整离线灾备、角色/权限恢复或应用切换已验证。备份文件修改时间也不是数据库事务的一致性时刻或 RPO 承诺。

### 2026-09-03 实际核验与故障记录

- 实际 `pg_restore` 成功，备份 schema 为 `0033_cash_flow_business_date`；四表计数与 SHA-256 一致，118 个数据库引用对象通过精确版本、哈希、长度和本地读回校验；本次耗时约 4.0 秒，临时容器残留 0。
- 对应私有记录：`outputs/backups/pre-0034-20260902T170730Z/recovery-29f9e4115ac448238b131aee677be8ab/result.json`。业务 PostgreSQL、MinIO、API 未重启，当前业务 schema 不受恢复影响。
- 首轮数据库恢复成功后，对象绑定因派生研究引用缺少重复长度而停止，残留 0。原因是恢复入口将派生引用误当作原始证据；已改为关联同 URI 的原始长度并拒绝哈希冲突，使用合成数据补回归，不修改旧记录。
- 集中审查发现 manifest 只列一张表也可能得到“表校验通过”；现已要求四表齐全，缺表先拒绝。另覆盖主流程失败后停止对象读取、精确 owner 清理和主错误/清理错误同时保留。
- 中断异常与清理异常组成的 `BaseExceptionGroup` 也须经过 CLI 的固定错误输出；合成回归覆盖该路径，避免中断时意外打印底层异常原文。

## Synthetic fixture 演练的使用边界

`nexus-engine-backup-restore-drill` 是 fixture-only 的本地灾备验证，不接受现有数据库、bucket、前缀或容器名作为参数。它自行生成 32 位随机 run ID，只操作：

- `nexus_drill_source_<run_id>` 和 `nexus_drill_restore_<run_id>`；
- `drills/<run_id>/source|backup|restore/` 三个前缀；
- `nexus-drill-postgres-<run_id>` 和 `nexus-drill-minio-<run_id>` 两个仅使用受限 tmpfs、无持久 volume 的临时容器；
- 该 PostgreSQL 容器内的 `/tmp/nexus_drill_<run_id>.dump`。

`NEXUS_ENV=production` 会在任何容器或数据目标创建前失败。不要为演练将 Docker socket 挂载进 app/API/worker 容器；该命令应在受控的本地工作区通过 `uv` 运行。
两个容器均携带精确 `nexus.drill.run_id` 所有权标签；已有同名容器会在启动前被拒绝，cleanup 只删除标签与本次 run ID 恒等的精确名称。普通子命令上限 30 秒，迁移上限 120 秒。

## 前置检查

```bash
docker version
docker compose version
uv --version
```

不需要本机安装 `pg_dump`/`pg_restore`；命令使用自建 `postgres:17` 容器内的客户端工具。首次运行可能需要拉取已固定的 PostgreSQL/MinIO 镜像，这只是开发依赖下载，不是部署。

## 执行

```bash
NEXUS_ENV=test uv run --package nexus-engine nexus-engine-backup-restore-drill
```

演练顺序固定为：

1. 启动随机命名、动态回环端口、数据目录仅使用 `noexec,nosuid` tmpfs 的 PostgreSQL 17 和 MinIO；
2. 创建 source/restore 精确随机库，仅对 source 从空库迁移到唯一 Alembic head；
3. 使用迁移自带的内部模拟账户和平衡初始账本，追加一个 READY 组合根、一个现金绩效根和一个脱敏审计 manifest；
4. 执行 `pg_dump --format=custom --no-owner --no-privileges`；
5. 将 source 对象逐个复制/校验到 backup，再执行 `pg_restore --exit-on-error --single-transaction` 并复制到 restore；
6. 比较迁移 head、8 张关键权威表计数、ledger 借贷平衡、最新绩效/READY/审计根、对象 SHA-256 与长度，并要求数据库中的审计 manifest URI/长度/哈希精确绑定到恢复对象；
7. 在 `finally` 中删除两个数据库、三个前缀、bucket 和两个临时容器，每类残留必须为 0。

命令只输出一行 JSON，包含 fixture 字节数、对象数、backup/restore/verify 耗时、本次 fixture RPO/RTO 和脱敏布尔验证结果；不输出密码、URL、对象内容或 SQL dump。

## 2026-08-27 本地演练记录

- PostgreSQL custom dump：736,450 bytes；
- 对象：1 个，245 bytes，source/backup/restore SHA-256 一致；
- backup 0.388 秒，restore 0.446 秒，verify 0.093 秒；
- 本次 fixture 实测 RPO 0.177 秒，RTO 0.539 秒，总验证清理前 8.406 秒；
- migration head `0021_manual_job_retry_allowance`，表计数/平衡账本/三类根/对象哈希全部通过；
- database/object/container residue 均为 0。

以上数值只证明本次小型 synthetic fixture 在当前本机的表现，不是生产 RPO/RTO 承诺，也不能代替真实数据量、异地存储、加密备份、保留策略和定期恢复演练。Phase 0–1 的设计目标仍是 RPO 24 小时/RTO 4 小时，但在没有真实运维环境证据前不声称已达到。

## 失败处置

失败时 CLI 只输出通用错误，并仍执行精确 cleanup；若主流程和 cleanup 同时失败，内部异常组会保留两者，避免清理错误覆盖原始原因。先检查是否有残留：

```bash
docker ps -a --format '{{.Names}}' | rg '^nexus-drill-(postgres|minio)-'
```

如果有残留，只根据本次完整 run ID 处理两个精确容器；不使用宽泛 Docker prune、volume prune、递归文件删除或通配数据库删除。若 cleanup 本身失败，将演练视为失败，不记录成功 RPO/RTO。
