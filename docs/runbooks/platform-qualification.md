# API 资格轮登记与只读核对

适用于 schema 0111 及以上的稳定用途 `platform-qualification-v1`。同一受支持配置下，
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

### 显式选择 medium 诊断配置（0112）

默认仍为原 low 配置。已评审的 medium 单变量诊断须在正常
`register-profile` / `inspect-profile` 上同时指定
`--purpose platform-qualification-v1` 和
`--profile-version platform-terra-medium-implicit30m-20261101-v1`；
`register-round` 也必须显式传入同一个 `--profile-version`。独立 `prepare` 支持相同选择，
但已登记的轮次继续消费原冻结包，不重新 prepare。其他历史 purpose 拒绝该选择。

配置选择固化进权威包；同一 UUID 换 effort、版本或导出目录都不能重新冻结或登记。
使用该配置返回的 model_config_id 准入，不套用旧 low 模型记录。官方支持 medium 不代表
本地质量已合格；三阶段累计 output 仍为 8192 且包含 reasoning，用完即停，不增大上限。
价格截止仍 `2026-11-02T00:00:00Z`，现有旧未知费用与共享预算保护不变。

### 共享额度与本机配置

`AGENT_DAILY_BUDGET_USD` 是正常 `infra/.env.local` 的可配置共享日限额；默认 3 美元
不是不可变的产品政策。按当前任务的费用授权，核对既有实际消耗、所有未知预留、
完整待执行包络、正式未来义务及合理余量后设置有界值；不靠清空旧账或等待日切来解决
已知不足。不修改未获授权的业务预算绑定，不充值或解除交易门禁。
正常 CLI 每次读取该配置；常驻进程和容器须在正常重载/部署后另核对生效，不能仅以文件
更新宣称所有运行消费者已变化。修改前保留私有原件及指纹，回退只恢复本次键值。

Platform 每次 `admit-position` 在既有共享会计/UTC 日锁下检查所有**尚未准入位置**的
冻结最大值。已有位置的预留或实际结算由共享账本计入，不再叠加一次。正式未来义务、
其他消费者和历史未结会计门禁仍生效；成功时只给当前位置建立原生预留，不为未来位置
新建占用，也不保证它们稍后一定可用。并发占用变化后，下一位置必须重新检查。
原费用和会计日期不移动，跨日不补充实验自身冻结总额；未知仍保留在原账本及资金核验中。

整轮容量不足返回 `COMPARISON_ROUND_BUDGET_EXHAUSTED`。原生准入事务确认回滚后，
正常入口在既有审计链追加拒绝回执；`inspect-round.last_admission_refusal` 保留历史原因。
这与已发送失败、已消费位置或永久停止的 scope 不同，不伪造 Run/Job/终结证明。
审计写入或提交回执不明返回 `COMPARISON_REFUSAL_AUDIT_UNCONFIRMED`，只读核对，不自动重试。
确无原生身份/发送、scope 未停且有明确继续授权时，可由正常入口准入尚未消费的位置；
原拒绝回执、已成功结果及冻结 manifest 都保留。已经消费的位置继续拒绝，不能换键重放。

## 阶段合成与历史展示

Platform 价格版本与 SDK 版本分别登记。只有精确模型配置、Run 的模型/价格身份和原生
实验绑定均验证通过，新的最终合成才消费既有三阶段结构化复核；不借用 SDK 版本或
改变费用身份。最终论点、逐项复核及不确定性来自已保存的组合阶段，证据审计非支持时
仍否决“充分”；缺合格价格仍为 partial，watch 不产生执行许可。

历史最终意图保持原样，不补写复核或重算质量。详情页通过相同实验身份与既有内容摘要、
冻结证据、提示及引用检查，只读展示原始三阶段记录；身份或内容不能验证时显示校验失败，
不展示未经验证的论点。原始模型自评与系统最终研究状态分别展示，均不代表独立质量验收
通过，也不解除实验隔离或正式业务门禁。

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

0112 新增 medium 精确配置支持，不改旧行。未产生 medium 配置或阶段事实时可退回 0111；
一旦存在则拒绝数据库降级，保留 0112，使用验证过的兼容读取镜像。部署前必须验证该回滚路径，
不能把仅支持 0111 的历史镜像当作 medium 事实的可行回滚。

## 原首版准备退出的限定历史读取

`28e5ff2fb4770ea415a599c0fa3d3763f1a899bf` 的首版 Platform 入口曾在充分验证准备之前
安装阶段成员，最终可能留下 `skipped / abstained / evidence_authority_missing`、空且未知的
阶段 terminal 与原预算 released 的矛盾组合。公共 terminal reader、旧位置 inspect、同轮后继
位置和重放仍拒绝；不改旧行，不把零字段或 released 当作结算证明。

跨实验互斥另按 `historical_platform_preparation_isolated` 核对完整冻结身份、原生绑定、
唯一 attempt/lease/fence、停止 scope/chain、事件/输出/terminal 哈希及时间关系。限定用途和
已审计实现提交之外不适用，任何授权、观测、验证或清理资源事实存在都拒绝。它仅回答本次
旧执行未进入外发且永久不能重领或授权；没有进入 provider 生命周期意味着清理不适用，
不是 `cleanup_confirmed=true`，也不授予模型质量、费用结算或未知接受状态。

无派发的因果依据来自该提交中的实际入口：`comparison_execution.py` 固定选择已绑定
Platform 的 responses 路由；`research_stages/executor.py:276` 先 await 授权，失败或提交不明
直接抛出；`protocol.py:343` 插入精确 run/job/attempt/lease/fence 授权，362 行提交、369 行才
返回 ticket；HTTP client 在 executor 的 368 行才构造，生命周期任务随后才启动。
0099 的 authorization immutable trigger 禁止正常 UPDATE/DELETE，授权 SQL 同时要求原 Job
running、预留有效及 scope 未停止。结合空的完整授权集合，才能推导未进入外发，不能只凭
错误原因、零 token 或当前无活动租约推断。实现 SHA 参数本身也不是运行版本证明：实际案例
须复核原入口/部署及执行记录；此次原正常调用包装在启动前核对了冻结实现提交，并保存退出
回执，私有原件留在本机。该固定兼容边界不用于登记后续新轮的版本白名单。

隔离回归重现旧捕获和零用量退出路径，在 transport 边缘核对零发送及零 client 创建，保持
公共拒绝断言，验证错误版本/哈希/停止/租约、迟到授权拒绝及事实变化后重新阻断，再通过正常
稳定登记和根预留接续。原未知、费用、预算与其他实验仍逐项受原门禁约束。
