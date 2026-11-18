# SEC GRAPHIC 精确关系证明

**目标：** 用已保存的官方 aggregate 与独立 HTTP 图片补齐字节关系，不改变历史覆盖或研究资格。

**事实与设计：** 真实 NVDA 原件含 UU 编码，存在末尾零编码空格省略及非零末组填充。按声明长度解码后，13441 字节与 HTTP 图片完全一致。新增有界 decoder，仅支持明确的成员、身份、序号和 framing；全部实际字节必须相同。旧 v1 归档及 unverified/uninterpreted 字段不修改。正常入口追加独立关系证明，固定 research_qualified=false；内容必要性审查另行完成，不在采集器内猜测。

**约束：** 无新增依赖或迁移，无网络、模型、研究或通知调用；13 USD、未知预留、旧 once 不变。未知格式拒绝。回滚忽略新证明，旧事实保留。

## 实施计划

- [x] transport 单测先复现拒绝；补精确 UU 关系验证，覆盖错误身份、序号、字节、framing、编码、长度和重复成员。实际缺一零编码字符与非零末组 padding 分开记录，decoder/profile、aggregate/member/HTTP hash、恢复行号均可强读重算。
- [x] 独立 proof producer/reader 绑定旧 snapshot/revision/全部原件，正常 CLI 追加与强读，旧 reader 仍不消费。
- [x] 正常隔离库入口验证幂等、篡改、错误账户和零副作用；真实已保存原件通过产品 decoder，不重新 GET。正式旧快照尚未登记追加证明，等待部署。
- [x] Ruff、5 个源文件 Mypy、76 单测、10 PostgreSQL 用例通过；只读独立代码审查无未关闭 P1/P2。
- [ ] 常规集成及部署。部署沿现有门；新增依赖风险决定仍待用户，不绕过。

**内容资格边界：** 已直接看图为 NVIDIA 标志，EX99.2 标题前一处引用。指定 Chat R2 已完成独立在线看图和页面检查，支持该上下文中的品牌装饰判断；在线观察尚未绑定本地精确 hash。研究资格仍须可信审查事实入口，普通 CLI 不能自填 reviewer 或自签 JSON 授予资格。此修复不实现或绕过该权限边界，不把当前观察区间或历史索引宣称完整。

## 实际发现与验证

最终回归定位到真实收件时钟问题：Mac 收件时间可能领先随后 PostgreSQL 校验时间，旧代码报 `SEC_INDEX_ARCHIVE_INVALID`。确定的客户端快 60 秒用例真实 RED；正常 collector 改为完整响应取得并私有保存之后，通过配置的数据库时钟记录新 receipt，原 60 秒边界不变。时钟失败保留 raw、无 receipt、不重试。旧收据与未来时间保护不改。

本地证据 `.secrets/sec-graphic-relations-20261118T0216/`：`receipt-clock-red.log`、`receipt-clock-green.log`（8 passed）、`final-clock-native-pg.log`（10 passed / 54.20s）、`real-frozen-profile-proof.json`。真实 NVDA 图片 13441 bytes 完全一致；旧 v1 `unverified/uninterpreted` 原封保留。未产生正式新事件、通知或付费研究；历史未知费用沿原身份保留。
