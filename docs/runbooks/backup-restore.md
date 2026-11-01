# 本地备份恢复 runbook

## 数据库与已引用对象的完整本地备份

`python -m nexus.ops.local_backup` 在一个**尚不存在**的受保护目录中创建数据库及其已引用对象备份。它只读现有本机 PostgreSQL/MinIO，不暂停业务服务，不覆盖旧备份；需要已缓存的固定 PostgreSQL 17 镜像。Mac 上容器内的只读 `pg_dump` 经 `host.docker.internal` 连接已配置的回环源数据库。

```bash
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.local_backup \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资 \
  --destination /Users/wangchen/Documents/ChatGPT/投资/outputs/backups/complete-local-YYYYMMDDTHHMMSSZ
```

备份目录为 0700，文件为 0600，包含 `database.dump`、`objects/`、`object-bindings.json`、`manifest.json` 和最后生成的 `complete.json`，不得提交 Git 或用作测试数据。

1. 在源库的 `REPEATABLE READ READ ONLY` 事务中导出快照；四张关键表摘要、全部 public 表中的对象引用和 `pg_dump --snapshot` 使用同一快照。事务保持到 dump 完成为止，导出期间的新写入不会混入该备份。
2. 每个数据库 S3 引用都必须有一致的 SHA-256/长度绑定。Provider 对象仍按精确版本读取；既有审计清单仅允许同 bucket 下 `audit/manifests/YYYY-MM-DD/<SHA-256>.ndjson.gz` 的规范无版本 URI，核验日期、文件名与数据库哈希、gzip头、长度及完整内容哈希。其他未知路径或缺失版本仍拒绝。缺失或冲突即失败，不能默认为空对象集。备份的原始 URI 不改写；映射文件记录其对应本地副本。本机对象读取显式绕过环境代理，不修改系统代理设置。
3. 校验 dump 在工具容器与本地的哈希/长度，对象写入后离线读回；manifest 固定 `nexus_local_backup_v1` 格式并绑定对象清单哈希和数量。dump 上限 512 MiB，对象上限沿用下文，超出即停止。
4. 只清理本轮随机所有权容器。取消时先等待已发起的命令结束（沿用 120 秒命令上限），再进入所有权清理，避免容器创建尚未完成时漏清理。全部数据验证且清理成功后才写入 `complete.json`，其中绑定 manifest 哈希。中断、缺对象、导出失败或清理失败均不产生完成标记；失败目录保留，下次人工执行必须选择新目录，旧完整备份不受影响。

恢复使用同一个现有入口：

```bash
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.local_recovery \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资 \
  --backup /Users/wangchen/Documents/ChatGPT/投资/outputs/backups/complete-local-YYYYMMDDTHHMMSSZ
```

识别完整格式后，恢复**不读取源凭据、不构造 MinIO 客户端、不访问源数据库或源对象存储**；`--repo-root` 为旧入口兼容参数，这种模式不加载其配置。先校验完成标记及两个清单，再在空的随机 tmpfs PostgreSQL 中恢复。恢复数据库扫描得到的精确 URI/版本/哈希/长度集合必须与备份对象清单完全相等，再从本地副本复制并读回验证。失败不会降级为向源 MinIO 补取。原 URI 和历史数据库行保持不变。

本功能覆盖对应数据库快照及所有必要的已引用对象。未被数据库引用的文件、密钥/配置、数据库角色权限、应用切换和异机恢复不在此验收范围。回滚只需退回旧代码并保留新增备份目录；不需要迁移或删除历史数据。旧代码不能解释新完整格式，应使用本版本恢复它。

包含无版本审计清单的完整备份，恢复时也必须使用支持该内容寻址格式的版本（`ffe8472`及之后）；旧备份中的Provider版本校验保持不变。2026-09-04的`pre-0041-20260904-complete`已覆盖177个引用对象并完成实际离线恢复，未读取源对象存储，临时资源残留0。此前两个失败目录无完成标记，保留用于诊断，不作为有效备份。

### 2026-09-03 完整备份实际验收

- 新备份目录：`outputs/backups/complete-local-20260903T023611Z`，schema `0035_issuer_research_evidence`，146 个已引用对象精确版本及内容校验通过；旧备份未修改。
- 实际恢复使用不存在的源配置目录，完成标记/数据库/四表摘要/146 个本地对象全部校验通过，`source_object_store_required=false`；恢复约 3.701 秒，临时容器残留 0，业务 PostgreSQL/MinIO/API 未重启。
- 私有恢复记录：该备份内 `recovery-da49d533ddd64beda73105cf5d090296/result.json`。它只用于业务恢复验收，未作为单元或集成测试输入。
- 合成跨层测试另行覆盖导出期间的并发写入，以及禁止源配置读取和源 S3 客户端构造后的空库恢复；真实业务服务没有被停止或断网来制造“离线”。

## 真实业务备份的隔离恢复

本节保留旧 DB-only 备份的恢复兼容路径，它仍需源 MinIO；新备份应使用上面的完整格式。

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
3. 再校验传入容器的 dump，默认执行 `pg_restore --no-owner --no-privileges --exit-on-error --single-transaction`；完整 0107 归档使用下述限定兼容路径。不自动运行新迁移；版本标记和逐表摘要须与备份时点一致。
4. 扫描恢复数据库的 S3 引用，通过同一 URI 关联原始记录的哈希/长度；派生引用可以不重复长度，但其哈希必须一致。仅从现有本机对象存储按精确版本读取，校验版本、SHA-256 和长度，复制为 0600 本地文件，再离线读回校验。单对象上限 64 MiB，总量上限 512 MiB、最多 10000 个，超过时停止而非默默跳过。
5. 不改写历史 URI，私有 `object-bindings.json` 保存原始 URI 与本地副本的对应关系；`result.json` 只保存安全状态、时间和汇总。终端不打印对象 URI、账户内容、密码或异常原文。
6. 成功或失败均仅清理本轮标签恒等的精确容器；清理失败即整体失败，保留主错误和清理错误。原始备份及本轮私有结果保留，不覆盖、不递归删除。

这一步验证数据库内容及数据库引用对象的可恢复性。原 `pre-0034` 备份只有数据库；对象是在恢复核验时从仍可用的本机对象存储取得。未被数据库 URI 引用的原始文件不在本片范围内，不能宣称完整离线灾备、角色/权限恢复或应用切换已验证。备份文件修改时间也不是数据库事务的一致性时刻或 RPO 承诺。

### 0107 完整归档的函数查找兼容与 0108 回滚

0107 的 Platform manifest 包装函数调用未限定 schema 的 v1 函数。在恢复工具的安全空 `search_path` 下，非空 manifest 的 COPY 会因函数解析失败而中止。0108 仅将两处调用限定为 `public.`；原 v1 正文、校验策略、表结构与业务行均不变，不改写历史迁移。

正常恢复入口只对通过原摘要和完整格式校验的 0107 归档启用兼容分支：从原 dump 生成 pre-data 及 data/post-data SQL，在同一个 `psql -X --set=ON_ERROR_STOP=1 --single-transaction` 会话内依次执行 pre-data、限定修正、data/post-data。修正前严格验证签名、属性和原函数正文指纹；未知定义立即拒绝。空搜索路径、CHECK、触发器及 post-data 均保留，不先失败后重试。原 dump、业务源库、manifest 行和恢复副本的 `alembic_version` 不变；恢复报告的 `schema_compatibility` 明确记录 `0107_platform_manifest_qualified_lookup`，不能把目标副本的兼容修正称为逐字相同 schema 或自动升级。

其他版本继续原 `pg_restore` 路径。新 0108 备份无需该兼容分支。应用回滚应使用支持当前 schema、旧身份及可选 HTTP 诊断字段的兼容镜像；显式将 0108 版本标记降回 0107 时保留函数限定修正，不重新引入恢复缺陷。0107 对已存在 v2 事实的更早版本降级保护不变。恢复通过仅证明该备份的数据和对象恢复，不证明服务切换或研究闭环。

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
