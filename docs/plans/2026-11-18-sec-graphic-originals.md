# SEC GRAPHIC 原件采集实施与验收

> 按既有长期 SEC 合同实施；不改变研究材料资格。指定独立评审：`NEXUS-SEC-GRAPHIC-ORIGINALS-20261118-R1`。

目标：真实 NVDA 8-K `0001045810-26-000088` 的 GRAPHIC 声明不再阻断其他官方原件采集，同时明确保留非文本未解释状态。该披露受理时间为 2026-11-17T21:20:51Z，位于已授权市场监控时段之外；归档和离线读回不创建正式命中、通知或研究。

## 实现边界

- 原研究 `document_inventory` 仍仅接受原文本类型。新归档入口单独支持固定扩展名 GRAPHIC，保留全部原清单；其他未知类型继续拒绝。
- 正常 collector 下载原清单、全部声明对象及 aggregate submission。沿用固定发行人/accession 目录、HTTP 200、无自动重试、60 秒和原请求次数上限；总原件另限制为 8 MiB。
- 文本逐件执行原精确 submission→HTTP 字节证明。GRAPHIC 仅证明官方 HTTP 原件收件、大小、格式头及 hash；aggregate 对应关系仍未验证，内容仍未解释。
- 新 `sec_uninterpreted_filing_v1` 使用独立 source 保存不可变索引、清单、原件及 receipt 绑定。`research_qualified` 固定为 false；不创建 ResearchEvidenceRef，旧研究 reader 不接受该来源。
- 正常只读 inspect 重验索引、全部 DataArtifact、原始字节、receipt 和声明。不能把采集完成解释成图像内容合格或研究链完成。

## 验收与交付顺序

- [x] 用代表性 GRAPHIC 清单复现旧 collector 提前拒绝。
- [x] 定向测试正常采集、未知类型、错件、缺件、大小、路径、receipt、文本变化、重复序号、错误页以及纯文本兼容。
- [x] 隔离 PostgreSQL 验证正常 collector 持久化及强读、重复导入、原件篡改拒绝、旧研究 reader 拒绝，研究与通知零副作用。
- [ ] 正常真实原件采集及 inspect；区分候选入口验证与部署后运行事实。
- [ ] 提交、独立复核、安全集成；不等待远端 CI。
- [ ] 原环境备份/正常恢复、占用及兼容回滚核验后部署，真实页面、持续心跳和原编号读回。

无需迁移或依赖变化。旧版本回滚保留新来源和原件，旧 reader 忽略这一不合格来源；不得删除新事实。另已确认的普通日预算 H 保护遗漏单独修复，不能把本切片或 $13 总额控制当作其已闭环的证明。旧 once、未知费用及历史分片矛盾不变。
