# 独立研究与监控及明确测试通知

用户已授权通过正常入口执行一轮新的完整三股研究，生成合格监控计划并验收可执行的通知链；允许向既有目的地发送一条明确标记的测试通知。不重放旧周期，不把独立研究冒充自然日调度，不变更晚间调度、账户权限、执行锁或未知费用/预算保护。

独立Chat评审 `nexus-independent-monitor-research-20261027-01` 有条件通过设计；不代表代码、运行或真实调用已通过。原文保留于writer私有证据目录。用户授权已核实，无需再询问研究资格或测试发送许可。

## 已确认的入口差异

`research_only_v2` 允许盘外历史研究，但禁止正式配置与计划消费；其prompt和snapshot用途不能重标。正式 `research_and_paper` 的盘内、价格、风控要求原样保留。监控来源现在要求原生正式cycle或原生增量绑定；只修改prompt白名单或安装函数不能接通后续扫描与增量。当前没有可合法安装的旧proposal，不通过新请求补旧槽。

## A：新研究用途（待实施）

- [ ] 正常CLI/API新增明确的 `independent_monitor_research_v1` 合同，复用独立请求、唯一宿主、三阶段、全批预算和正常收尾；根据schema最小迁移，不沿用v2标签，不造PaperCycle。
- [ ] 冻结完整候选manifest、真实共同cutoff、证券发行人绑定及范围、原件/快照/hash、实际观测/接收/可用时点、历史行情交易日/币种/调整口径和SEC基线。必需底座不足拒绝；可选某类材料不足只限制对应覆盖。
- [ ] 新prompt/schema在portfolio角色完成单股综合和条件采纳；不额外执行三股组合配置。原模型预算/绝对截止/审计否决和watch只能defer不变。
- [ ] 专用context引用每kind前驱ID/generation或已证明不存在；来源资格→两kind安装→source_current/scan→单股父绑定→审计接替→BFF/页面同一用途解释。保留已知结算、原生唯一Job/Attempt/lease、原件、两审/精确采纳和CAS。
- [ ] 新用途及其增量后代拒绝进入正式配置/目标/订单。新baseline不消费旧daily/mixed pending；原episode、事件归属和去重保持。
- [ ] 历史行情不能冒充当前L1；权威时段外not_due，时段内缺失/陈旧unknown。不能续期过期计划或伪造触发。
- [ ] 隔离完整链及正式拒绝回归、终审/全部CI/部署后，保存唯一新请求身份再提交一次；未知仅跟踪同身份，不换键。真实输出不保证合格proposal，逐股报告coverage gap。今晚日槽和受保护预算不变。

## B：一条明确测试通知（当前实施切片）

复用正常outbox领取、加密Bark publisher及既有配置。新增独立固定schema，不产生MonitorTrigger/Job/Run/订单。用户输入只有测试UUID；目的地、自由正文、业务时点和retry参数均不可提交。

- [x] 固定标题/正文“NEXUS 测试通知｜非投资预警”；严格字段/UTC/身份校验，独立测试group和稳定ID，无业务导航。
- [x] 正常enqueue/read入口；DB时钟、同身份事务锁、同事务outbox和审计。精确身份重复调用只读原回执。
- [x] `max_attempts=1`；既有dispatcher在HTTP前提交claim/attempt，失败、确认丢失及租约到期不产生第二次尝试。原业务通知重投策略不变；默认HTTP transport无自动重试。
- [x] 已登记queued；发送中/失败或确认丢失unknown；只有持久化接受回执为provider_accepted，设备实收一直单列unverified。读回不发送。
- [ ] 独立复核、完整类型及聚焦PG/unit、最终head全部CI；正常部署需更新Bark消费者代码并保留原activated_at与实例单一性，不补历史消息。
- [ ] 部署后仅提交本次获准的一条测试，记录原身份/attempt和provider结果，设备实收由用户事实确认；不能据此宣称真实投资预警已发生。

无需依赖或schema变更的B可独立交付，A仍是完整目标的一部分。B回滚时旧消费者不领取新schema，已落盘outbox/audit保留；任何已进入发送边界的测试不得重发。未部署的候选不能访问业务队列或发送测试。运行服务及其他用户文件保持。
