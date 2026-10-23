# 会计复核材料与收件入口

此入口整理精确事实材料并保存不可变的 **received_unverified** 收件记录。
收件不是采纳，不产生 ResearchEvidenceRef，也不解除估值、时效、价格或风控门禁。
当前尚未交付独立授权采纳、新快照冻结和正式消费者闭环；不能以本入口成功声称业务验收通过。

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
不需要迁移、新依赖或凭据；回退代码时保留已有收件记录，旧研究选择器不消费该来源。

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

## 尚待接通的交付条件

正式采纳必须重新核验来源回执、DB/XBRL语义、授权和报告来源，再追加独立采纳事实。
新Run冻结精确引用；旧Run不追读新结论。消费者只能按原规则先选分母，再核验所需事实的
全部修订覆盖，不挑已审事实拼分母。窄义披露意见不能直接替代更广的会计对账保证。
任何范围外事实、新修订、相反意见、缺失证明或真实更正仍保留限制；不回填旧快照。
