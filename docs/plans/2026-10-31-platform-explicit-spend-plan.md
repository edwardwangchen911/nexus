### 2026-11-01 稳定登记实现与验证

同一受支持配置的新轮使用稳定purpose和独立UUID，通过正常register-round/inspect-round完成登记与只读核对；一次0111迁移保留四种历史purpose唯一性。整轮上界由任务政策、输入、配置、Key绑定和有效价格冻结，实际预留仍逐位置消费原账本，不再逐轮请批。

终审反例已落实修复：维护镜像在文件/DB登记前拒绝；权威bundle/attempt固定在正常运行根的UUID目录，output仅导出；提交未知、换目录、并发和保留attempt但丢bundle均不重冻/重插。preclaim先核验精确failed/零attempt/未启动形态及正式审计证明，再走stage reader；正常旧关闭后新stable根实际预留通过，缺审计和skipped旧坏历史保持拒绝。link后真实子进程硬退出只恢复精确内容/inode/私有权限/两链接的已知临时别名，原字节与登记尝试保留，未知别名不删除。

集中验证：120项隔离数据库矩阵通过，最后attempt绑定补丁7项定向复验通过；96项聚焦单测及最终8项真实进程/文件发布回归通过，覆盖有重叠不合并计数。Mypy 716源码、Ruff和完整依赖审计通过；原minimatch范围与2026-11-02 08:00北京时间截止不变。正常CLI同版本两轮完整三股三阶段使用HTTP替身，不能当作真实API质量。

定向终审nexus-stable-round-fixes-20261101已关闭原四项及attempt补充。末项目录持久化P2也已按条件补齐：逐级同步父目录项，在冻结和DB尝试前完成；6项正常CLI顺序/故障及相关回归通过，定向代码复核无新增阻塞，证据已回传原评审。候选及全部CI、0111兼容非空F→R→F、正常部署和真实三股验收尚待完成；没有真实业务模型调用、服务切换、未知接受或预算释放。操作方法见运行手册platform-qualification.md，精确日志在私有stable-platform-qualification及相邻历史命名的platform-fourth-qualification目录。

## 2026-11-01 现行实施：稳定配置与独立运行身份

本节替代刚开始的第四轮硬编码草稿；草稿及所有未提交差异已保存在私有stable-platform-qualification/superseded-fourth-round/，没有部署或真实调用。用户已取消本任务逐轮预算审批；规范见nexus-direct-responses-evaluation-v0.1.md顶部。原生Goal仍active，当前工具不支持编辑objective；不得假标complete来重建。

- [x] 原生核验原始用户消息；停止按轮扩枚举/迁移，保留已有成果。
- [x] 查明purpose全局UNIQUE、按purpose注册查询、有限CLI choice和资金version映射四处耦合。
- [x] 指定Chat一次定向合同评审：nexus-stable-platform-round-contract-20261101。
- [x] 一次0111兼容迁移、稳定purpose/config与experiment身份分离；旧身份读取及唯一约束保留。
- [x] 正常register-round冻结输入/配置/预算与private凭据绑定；同id幂等与冲突拒绝，未知提交只读查原id。
- [x] 同版本两轮原生链、并发预留、未知占用和费用累计、发送前后失败、清理结算/迟到写入/恢复回归。
- [ ] 终审/全部CI/最终及兼容回滚/正常部署后直接继续真实三股验收，不再逐轮请批。

旧purpose、旧单轮批准和失败仍历史只读；未变HTTP、schema、容量及阶段协议矩阵复用。保留execution_locked、普通Worker停用和所有unknown预算。结构化schema历史夹具已显式固定0110出生环境，保留原断言；0111兼容由新增回归单独验证。

---

# 单轮 Platform 显式费用授权

> 执行使用 `superpowers:executing-plans`，复用现有资格轮和阶段协议，不扩建执行框架。

**目标：** 用户已明确允许本轮 API 费用后，正常入口可以消费精确实验绑定的授权；不再要求证明仅使用赠额，也不伪造余额。

**架构：** 保留旧赠额证明及其序列化，增加私有授权文件变体。仍冻结现有 manifest 的两个绑定哈希，复用 producer、共享预算、阶段提交和最终同步发送检查。仅 Key 作用域不填写未经核验的组织/项目 ID。授权不等于账户余额证明。

**技术：** Python/Pydantic、现有 CLI、PostgreSQL 隔离回归；不迁移 schema，不改变业务默认 SDK。

- [x] 定向 Chat 评审费用合同差异；依据有效意见收口。
- [x] `test_platform_explicit_spend.py` 先证实旧路径拒绝新合法授权；覆盖身份、金额、窗口及原件变化。
- [x] `platform_credentials.py` 增加严格授权种类、Key 作用域和只读检查，保留赠额路径。
- [x] `platform_funding.py` 绑定唯一 experiment/input，保留共享账本检查；`research_stages/executor.py` 使用共同期限判断，最终发送前重读。
- [x] 隔离 PG 正常 producer/Worker 路径覆盖新授权、未知占用不变、最终发送失效拒绝；复用已有生命周期与并发矩阵。
- [ ] Ruff/Mypy/定向单测和 PG 回归；提交实际差异作定向终审。
- [ ] 全部候选/主线 CI、最终与维护回滚镜像通过后正常部署；沿唯一正常身份执行真实资格轮，不重放。

范围固定为 AAPL/AVGO/NVDA 各 research/critique/synthesis，总授权上限 6.42 美元，实际采用完整冻结最坏用量，不因结算补充额度。价格与既有风险期限仍为 2026-11-01T00:00Z；不充值、不自动充值、不新授权、不发测试通知。普通 Worker 停用、execution_locked 和历史 unknown/预算不变。旧赠额文件、旧 manifest/hash、旧回执及兼容读取不修改；失败/未知按原路径停止和保留。
