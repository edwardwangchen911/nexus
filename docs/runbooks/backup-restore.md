# 备份/恢复演练 runbook

## 使用边界

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
