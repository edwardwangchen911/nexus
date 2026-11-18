# SEC 精确内容审查对象

目标是为当前真实 NVDA 公告准备可独立复核的完整对象，不能把图片原件关系证明变成研究资格。指定 Chat R2 已允许普通工具准备审查对象；R3 正在核对剩余的用户指定用途与可信审查边界。

正常 `prepare-content-review` 入口读取既有 scope、原 snapshot/hash、全部原件和收据及独立 UU 关系，导出私有原样文件、材料 hash 和所有 HTML 的资源引用观察。解析结果不是浏览器执行或完整覆盖证明。没有 reviewer 输入、资格签发、ResearchEvidenceRef、模型、事件、通知或预算写入。

只读兼容 schema134/135；不放宽现有研究/监控入口的 schema 门。输入仍由正常 repo/env 配置解析。旧收据及归档不修改，输出目录只能新建于用户所有的私有父目录，完成标记最后写入。

## 实施与验收

- [x] 先写材料边界、依赖观察和不授资格测试，确认 RED；重复 src 的观察丢失也已 RED→GREEN，保留所有原始属性并标歧义。
- [x] 实施正常只读导出与 scope/as-of/原件强读，固定 unverified/false。
- [x] 正常 foundation 入口验证私有输出、完整性、错误账户、错误来源、重复目录和 SQL/业务零副作用；写文件失败保留原始片段、没有完成标记。83 单测、10 PostgreSQL 用例通过，新增写失败 native 用例另通过一次；Ruff/格式及 Mypy 3 源文件通过。独立代码审查无未关闭 P1/P2。
- [x] 真实 snapshot 已通过候选正常 CLI 在原 schema134 READ ONLY 导出，7 对原件及收据、4 HTML 的全部输出 hash 已复核。材料 hash `1ee097fa875484f5a51b8df1740c4c3aa83a521b46fd3bfcd84be9bfc91d32c3`，输出仍 false/unverified/not_designated。来源 `.secrets/filing-context-20261118T0248/nvda-review-material/`；没有模型/SEC调用、Run、事件或通知。
- [ ] 常规集成。实际部署仍沿原风险决定与安全门，不能以材料准备代替整链验收。

已有会计采纳的 purpose 仅限原 EPS 范围；BFF 服务签名也不是独立审查身份。本切片不复用这些权限，不新增可信签发人。后续如需新的用户指定用途，须先完成独立评审和用户明确决定。

R3 独立复核已从官方图像元素正常导出 JPG，实际重算原字节 hash 并查看同一文件：与本地 13441 bytes 原件完全相同，支持可见内容为品牌装饰的判断。该报告不宣称全部 HTML/DataFiles 都已语义审查，也不授予新用途。剩余 operator 边界经实际检查：投资 Agent 只有固定只读工具，HTTP 没有采纳入口；本机 operator 与 API 的数据库角色相同，不能声称有数据库身份隔离。资格采纳仍未实现或登记。
