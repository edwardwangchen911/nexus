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
