# API 资格轮登记与只读核对

适用于 schema 0111 的稳定用途 `platform-qualification-v1`。同一受支持配置下，
后续轮次只登记新的实验 UUID 和冻结预算，不增加用途枚举、迁移或发布版本。
旧用途和旧结果保持原身份、原唯一约束及只读核对能力。

## 登记前

先保存本轮 UUID 和私有输出目录，核对是否已有对应登记或任务。正常运行根目录中的
`.secrets/nexus-platform-local/qualification-rounds/<UUID>/` 是权威冻结包及登记尝试的位置，
随既有本机凭据和私有状态保留；部署、备份和回滚不得丢弃它。`--output` 只是导出目录，
换导出目录不会获得新的冻结或数据库提交资格。目录创建逐级同步父目录项，失败在数据库尝试之前拒绝；已有标记不删除。保留一次已核验的任务政策
原件及其 SHA-256；当前任务无需逐轮请求预算批准。`continuation-evidence` 留存上一轮的
诊断与继续依据，不把失败或未知当成可重试。原未知费用、预留和有效租约继续参与准入。

源文件是正常入口已归档的冻结资料；模板仅复用既有评价协议。实现 SHA 必须来自独立核验
的已部署版本，传入参数本身不是版本核验。配置、价格有效期及模型容量仍须合格。
凭据只经正常本机入口读取，不在命令、聊天或普通日志中输入 Key。

下面变量由本轮已保存的身份和已核验文件提供，不在循环中重新生成 UUID：

```bash
.venv/bin/nexus-engine-research-comparison register-round \
  --repo-root "$NEXUS_REPO_ROOT" \
  --experiment-id "$ROUND_ID" \
  --source "$FROZEN_SOURCE" --source-sha256 "$SOURCE_SHA256" \
  --template "$EVALUATION_TEMPLATE" \
  --task-policy "$TASK_POLICY" --task-policy-sha256 "$POLICY_SHA256" \
  --continuation-evidence "$CONTINUATION_EVIDENCE" \
  --implementation-sha "$VERIFIED_DEPLOYED_SHA" \
  --output "$ROUND_DIRECTORY"
```

登记不调用模型、不预留原生位置预算、不启动宿主。回执
`bound_frozen_positions_unreserved` 仅表示整轮上界已冻结；实际预留、领取和发送仍逐位置走
原 `admit-position`、`run-position` 路径，消费共享账本和全部门禁。登记成功不等于允许发送，
更不等于质量合格或正式决策许可；质量达标前业务默认仍为 SDK。

## 中断和不明结果

本地文件交付中断时只恢复原 UUID 和原参数；冻结时间、预算与输入不得重生成。
持久化登记尝试后，重复登记只读回原身份，不再次插入。提交结果不明时返回非零退出，
保持 `unknown` 和 `replay_allowed=false`；不得删除权威尝试标记或换 UUID 自动重试。
同 UUID 换导出目录只复制原冻结内容并读回原数据库事实。

文件发布在硬链接后发生进程退出时，恢复仅在排他发布锁中移除可精确证明归属的临时别名：
目标名和内容摘要匹配、同 inode、仅两个链接、私有目录及文件权限。恢复保持原 inode 和字节，
不产生新的登记尝试。未知别名、多余链接、内容或权限变化继续拒绝并保留文件；正常读取仍
要求 `nlink==1`。维护镜像拒绝新登记，历史查询继续使用 `inspect-round`。

```bash
.venv/bin/nexus-engine-research-comparison inspect-round \
  --repo-root "$NEXUS_REPO_ROOT" --experiment-id "$ROUND_ID"
```

该入口只读数据库，不依赖原政策、Key 或冻结文件仍在，也不因当前价格过期失去历史读取
能力。未观察到登记保持未知，不能据此判定原事务没有提交。核对已登记身份、实际位置预算、
用量、终结和清理证明后才判断后继轮准入；只读回执不授予派发权限。

## 兼容与回滚

0111 仅放开稳定用途的多身份登记，保留四种历史用途唯一性，不改历史记录。出现稳定用途
数据后拒绝降回不支持它的 schema；回滚必须使用已验证读取 0111 的兼容镜像，保留原预算、
阶段事实及审计。新价格或模型配置仍须独立验证为受支持配置，不能借新轮登记放宽校验。
