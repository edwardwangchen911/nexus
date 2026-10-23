# 会计复核材料与收件入口

此入口整理精确事实材料并保存不可变的 **received_unverified** 收件记录。
收件不是采纳，不产生 ResearchEvidenceRef，也不解除估值、时效、价格或风控门禁。
正式指定与采纳是下文的独立操作；收件成功不代表已完成采纳或真实研究验收。

## 本地使用

使用项目虚拟环境，在项目根目录运行：

```sh
PYTHONPATH=.:engine/src .venv/bin/python -m nexus.ops.accounting_review_local export \
  --material /absolute/private/material-manifest.json \
  --sources /absolute/private/source-directory \
  --output /absolute/private/material.json

PYTHONPATH=.:engine/src .venv/bin/python -m nexus.ops.accounting_review_local check \
  --material /absolute/private/material.json \
  --sources /absolute/private/source-directory \
  --submission /absolute/private/review.json

PYTHONPATH=.:engine/src .venv/bin/python -m nexus.ops.accounting_review_local check-bindings \
  --repo-root /absolute/repository \
  --material /absolute/private/material.json \
  --sources /absolute/private/source-directory

PYTHONPATH=.:engine/src .venv/bin/python -m nexus.ops.accounting_review_local receive \
  --repo-root /absolute/repository \
  --material /absolute/private/material.json \
  --sources /absolute/private/source-directory \
  --submission /absolute/private/review.json

PYTHONPATH=.:engine/src .venv/bin/python -m nexus.ops.accounting_review_local inspect \
  --repo-root /absolute/repository --submission-id UUID
```

`export/check` 不加载业务配置，不联网；导出拒绝覆盖已有文件并使用0600权限。
`check-bindings/inspect` 使用正常 `infra/.env.local` 配置并开启只读数据库事务。
`receive` 仅追加独立来源 `nexus_accounting_review_receipt` 的 DataSnapshot；同ID同内容返回
原记录，同ID不同内容拒绝。并发接收在首次插入前即按同一提交ID串行化。
单独收件不需要迁移、新依赖或凭据；正式采纳及新冻结引用需0086，见下文回退边界。

## 材料与意见的含义

`accounting_material_v1` 绑定 instrument/issuer、精确事实与完整字段、原件哈希、
修订集合、XBRL节点及未决问题。材料原件须为固定SEC来源且在指定目录内，拒绝路径穿越、
符号链接、字节或哈希不匹配。单源最多8MiB，总计32MiB。

`check` 输出的 `xbrl_contexts_verified` 只表示实际重新解析匹配的事实数量；没有节点证明
时为0，不将声明的context哈希当作验证通过。`check-bindings` 另外逐字段核对真实数据库
事实及修订身份。这两类核对仍不验证来源回执的授权可信度或实质会计结论。

`accounting_submission_v1` 包含声明的审阅身份/能力、真实审阅时点以及完整fact×修订意见。
每项带来源定位、原文引文、理由与时间冲突说明。结论枚举严格区分：

- `as_disclosed_not_replaced`：指定披露没有替换或重述该已披露事实；不是排除全部会计影响。
- `requires_reconciliation`：需要对账或更正，不可用另一份较新意见覆盖。
- `unknown`：材料或判断仍不足。

声明身份、引文存在、数值相等、未来授予日期或收件成功均不构成专业资格或采纳权限。
私人内部研究允许用户明确指定的有限AI分析；不得捏造CPA资格，也不额外要求用户亲自审计。
正式采纳仍须独立绑定真实授权来源、精确报告来源/hash、范围和可用时间。

## 消费约束

正式采纳必须重新核验来源回执、DB/XBRL语义、授权和报告来源，再追加独立采纳事实。
新Run冻结精确引用；旧Run不追读新结论。消费者只能按原规则先选分母，再核验所需事实的
全部修订覆盖，不挑已审事实拼分母。窄义披露意见不能直接替代更广的会计对账保证。
任何范围外事实、新修订、相反意见、缺失证明或真实更正仍保留限制；不回填旧快照。

## 正式指定、采纳和未来冻结

`designate` 是受保护的本地 operator 操作，不向 Agent 或 HTTP 研究工具开放。
operator 核对已经存在的人类指定与报告来源后，保存 `OwnerDesignation` JSON：
`designation_id`、`material_sha256`、`submission_sha256`、`reviewer_name`、
`owner_message_source/text`、`report_source/text`、
`purpose=owner_designated_ai_as_disclosed_eps`，以及 `source_receipts` 中逐份原获取回执的原文。
这些内容提供可审计来源绑定，不宣称密码学认证、CPA资格或独立会计鉴证。
不要求用户重复批准已经明确指定的同一判断范围。

```sh
PYTHONPATH=.:engine/src .venv/bin/python -m nexus.ops.accounting_review_local designate \
  --repo-root /absolute/repository --submission-id UUID \
  --designation /absolute/private/designation.json --operator-confirmed

PYTHONPATH=.:engine/src .venv/bin/python -m nexus.ops.accounting_review_local adopt \
  --repo-root /absolute/repository --submission-id UUID --designation-id UUID \
  --sources /absolute/private/source-directory

PYTHONPATH=.:engine/src .venv/bin/python -m nexus.ops.accounting_review_local inspect-adoption \
  --repo-root /absolute/repository --submission-id UUID --designation-id UUID
```

`adopt` 重新核验回执、DB/XBRL、已归档SEC原件及指定/报告链，再追加采纳证据。
支持的历史回执为：精确四字段原件回执（URL、接收时间、hash、字节数）、增加HTTP200的
完整提交回执，以及无URL但具有ARCHIVED/submission_get/HTTP200/非空documents/正整数requests
的完整归档回执。四字段历史格式没有HTTP状态，不补造；显式失败、缺失字段或未知格式拒绝。
完整提交的header、主文档名、类型和原件须匹配既有SEC归档。两个归档表示若仅差XBRL包裹、
边界换行和body结尾单个空外链script，则逐字节核对其余正文；保留双方原hash，不声称原始字节相同。

正式登记时间取自完整验证结束后的数据库时钟，不回填到操作起点。相同身份重复采纳保留
原证据及时间；不同内容冲突拒绝。收件仍为 `received_unverified`。研究读回校验冻结的
收件—指定—采纳hash、对象和时间链以及数据库范围，不声称每次读回都重读外部原件。
只有新快照能冻结新采纳；早于采纳登记的cutoff、旧Run不会追读它。所有相关可见意见均保留，
覆盖不足为unverified，相反意见为conflict，容量不足拒绝而不移除原限制。

消费者仅在冻结采纳有效时使用 `as_disclosed_eps_v1`；先按原规则选择最新三组件，再核验
精确fact×所有冻结修订。成功结果带 `historical_as_disclosed` 及范围限制，表示冻结报价与
历史披露EPS的描述性比率，不等于历史PIT成立、全部会计正确、未来EPS保证或执行许可。
正式决策仍重验时效、价格、预算与风控，`execution_locked=true` 保持不变。

## 迁移与回退

采纳使用0086迁移扩展现有冻结证据归属守卫，不新增表列、不修改旧事实。运行服务要求
精确schema head，旧0085服务不得在0086数据库上继续接收任务。部署前保留备份和上一版本，
在旧服务退出后迁移并切换新版本，完成身份和健康核验后才允许正常入口写入。
没有任何新采纳冻结引用时可降回0085并恢复已验证旧镜像；迁移会拒绝在已有新冻结引用时降级。
出现新冻结引用后，必须保留0086及其读取支持，通过兼容0086的修复镜像前向回滚，不能删除
新证据或恢复旧数据库来掩盖已发生事实。部署验收与真实研究验收分别记录。
