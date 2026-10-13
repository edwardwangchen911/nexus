# 研究超时与只读负载诊断

用于判断正常阶段请求的负载与本地超时来源；不是自然业务验收或性能承诺。
以下 CLI 从 `--repo-root` 指定的主仓库受保护配置读取真实业务库，明确传入
`--database-url-file` 时仅读指定诊断库。进入只读事务，不调用模型或改变业务事实。

```bash
uv run --project engine nexus-engine-research-workload-inspection \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资 --run-id RUN_UUID
```

依赖现有版本的冻结证据、提示及原始输出，缺失时返回固定安全状态。
只保存安全元数据；不要将账户内容、原始研究文本或认证配置粘贴到聊天中。
新增 CLI 注册无需依赖、schema 或配置迁移。旧回执读取兼容；回滚代码不会删除新回执，
旧代码未暴露的新诊断字段不可作为旧运行已采集证据。

`--financial-table-candidate` 仅预览未启用的请求装配候选：财务观察保留所有原值、期间、
来源绑定和记录顺序，以可还原表格呈现；只发送当前阶段适用的指令，提示正文保持完整，
提示版本/hash 继续保存在冻结权威与输出审计中。综合阶段指令、全部前序 claims、
反证和否决票不裁剪。默认请求与历史 hash 不变，正常研究入口不使用此开关。
候选始终 `activated=false`、`future_claims_capacity_proven=false`；预览通过不授权启用、
部署或重放。三阶段须分别核对完整 wire；前序文本长度的合成压力样例只证明装配形状，
不证明模型内容合格、实际 SDK 输出预算可容纳或自然周期成功。

候选输出的 `capacity_envelope` 按当前冻结输入及 SDK schema，覆盖前序角色最多两条、
每条最多400字符和三项引用的保守字节形状；阶段上下文列化只缩小请求，报告以还原原行
后的完整请求作为包络。Unicode JSON 转义按每字符最多六字节计量，不假定极值形状
能同时满足8192实际输出预算，也不输出合成结论正文。此包络不覆盖未来披露。
单次 token 预检与 wire 分开：样例 allowance 不是所有字符串的 token 上界；
保守 token 包络采用字节后备界，无法证明低于门禁时标 `not_proven_within_limit`。
累计使用量条件的阈值为 null 表示该保守包络不能证明存在可继续额度，不表示零使用量
即可继续。`aggregate_sdk_input_completion_proven=false` 保留；实际阶段仍按 SDK 用量结算，
不将三个本地估算求和替换正式门禁，不据诊断注册、启用或重放周期。

### v9 本地资格合同（未注册或启用）

用户批准的 `codex-sdk-chatgpt-v9-full-input-capacity` 使用固定100479字节边界，
保留完整冻结输入和全部前序claims；96k累计SDK输入、8192累计输出、单次64000
预检及三调用上限不变，阶段时限沿用v8。旧v8及默认入口仍保留原字节边界。
该资格版本不在正常模型准入/自动选择集合中；定义版本不等于登记、启用或部署。

本地 `preflight_capacity_pool` 必须接收完整冻结池权威和每个成员的正常只读输入构造器，
不得由调用方筛选更易通过的子集。全部成员的包络和首请求预检通过后，才一次绑定
共享manifest、各自request/evidence及精确research请求hash；失败不先放行其他成员。
v9 `analyze` 缺全池绑定，或请求、证据、首请求内容漂移时在SDK前拒绝。
可从已保存周期调用正常只读资格入口，无须手填候选或Run列表：

```bash
uv run --project engine nexus-engine-research-workload-inspection \
  --repo-root /Users/wangchen/Documents/ChatGPT/投资 --capacity-cycle-id CYCLE_UUID
```

该选项与 `--run-id` 互斥。入口严格核对完整清单/事件hash、研究成员和时刻、模型冻结hash，
通过现有周期上下文reader核对补入上下文后的研究快照，不将它误当清单的原快照。
使用旧Run的原证据编译新的假设profile，标 `readonly_hypothetical_profile`，不改旧版本/hash。
非法或超限只输出固定安全码。正式周期/Worker接线与完整隔离整链仍须验证；
只读资格入口通过不表示可执行、已启用或新自然周期成功。
未来阶段继续按实际SDK用量检查，不以本地预检保证全部阶段预算、质量或自然周期成功。

容量包络按正常无损投影后的完整 wire 计数，包含格式解码说明；不把列格式展开成
不会发送的对象行后再应用字节门禁。当前 prior 生产者固定字段顺序及 role/index，
每角色最多两项、每项文本最多400字符及三个引用。六字节转义字符、最大引用位数和
完整前序行集合逐项支配实际合法值；原行/列行及整份 wire 的最小长度选择保持单调，
故最大行投影覆盖较少行导致的原格式回退。该推导依赖现有生产者和投影合同；修改
字段、顺序、次数、引用或跨 prior 的字符串投影时必须重验。此处不改变固定100479
上限、冻结输入、历史身份、token额度或SDK实际请求检查。
回退删除/停止选用资格候选即可；未注册阶段不迁移或修改历史Run、输出、周期和订单。

隔离 Worker 构造器仅在显式 `full_input_capacity=True`、实际数据库名为
`nexus_test_*` / `nexus_diag_*` 且 v9 模型配置/hash、Run 版本/hash 完全一致时构造。
该参数不暴露为业务 Worker 或模型注册入口；默认 Worker 仍拒绝 v9。
构造不等于全池放行，缺少预检绑定仍在首次 SDK 前拒绝。v9 配置hash覆盖固定字节
上限、无损财务投影、当前阶段提示投影和全池门禁；v8 的配置/hash不修改。
阶段只读 reader 可验证 v9 输出的完整 assessment、引用与前序审查覆盖，损坏时拒绝。

2026-10-13 本地验证已覆盖显式隔离工厂、错误库/版本/hash/token预算拒绝、工厂到
三阶段 SDK 替身，以及三成员全池预检后分别产生八角色输出；未调用真实模型。
这是构造与协议层验证，不是 Job 领取、PostgreSQL 落盘、组合决策、订单/账本及页面
整链验收。完整隔离数据库链路和正式周期接线仍未完成，不据此申请或宣称启用。

隔离研究准入同样必须显式 `full_input_capacity=True`，核对实际 test/diag 数据库和
新模型配置/hash/有效时刻；仅允许独立 Paper Research、单次 Job，不允许继承版本、
经济评价或 fixture 行情许可。普通显式选择与继承请求拒绝v9，自动选择排除v9；
拒绝不得留下幂等claim、Run、Job或预算预留。幂等与已有Job回读须在异步等待后
重读实际模型，并核对Run↔model的ID、版本、hash、额度及有效时刻，不能只比较常量。
测试只在临时PostgreSQL库插入全新合成资格模型；没有产品注册入口或主库登记。
本段准入验证仅证明新研究任务的身份与幂等边界，尚不证明Job执行、研究输出落盘、
组合决策、订单、账本或页面。旧版本事实及其读取合同保持不变。

隔离Job资格守卫在领取锁内核对实际数据库、精确模型、Run及Job输入hash、范围和
时点。v9从通用领取及过期恢复中排除；显式资格仍限单Job、一次尝试、195秒handler
与210秒lease。领取本身不绑定完整候选池，模型入口仍要求完整池预检。
2026-10-13 单成员合成资格测试已通过原生 `run_job_worker_once`：隔离工厂、正常
数据库只读工具、全池门禁、三阶段SDK替身、八角色与完整结构化assessment落盘，
终态Job/Run成功、用量和预算结算、阶段reader verified。v9合成沿用既有结构化
审查与否决语义；默认actionable consumer仍拒绝v9，显式隔离消费见下，研究完成不自动授权执行。
测试使用正常数据合同下的合成来源，不是上游真实数据或正式多候选周期证据；
正式服务尚未启用v9；该阶段的组合和页面验证见下方后续记录。

隔离产品 Worker 现在可显式调用 `run_agent_worker_once(full_input_capacity=True,
job_ids=(JOB_UUID,))`，不接受自选成员列表或工具/模型客户端注入。
它先从所选Run的冻结Paper context定位周期，验证周期清单/hash/事件链和精确v9模型，
导出全部1–5成员；每个成员原冻结context证据均须重验为VALID。全部成员的正常
数据库输入、wire包络和首请求预检通过后才允许领取，并在handler内用原deadline
重检全池；preclaim的观察期限不用于模型调用，不重新开启执行期限。
候选周期仍通过显式隔离candidate模式和精确模型ID选择独立v9身份，不修改v7配置。
普通Worker/注册/服务入口不启用v9；后续隔离消费必须另行验证资格和确定性风控。

2026-10-13 真实PostgreSQL隔离测试覆盖3和5候选的正常周期创建及产品Worker研究闭环，
另覆盖尾成员wire超限和现有context revalidator判为INVALID时领取前拒绝，0attempt/0SDK。
后者为受控验证故障，不改写不可变证据。相关28项PG、43项单位回归通过，静态检查
通过；使用合成来源和SDK替身，仅证明完整池研究阶段，不证明真实模型预算/质量、
组合决策、内部模拟成交/账本、页面整链、正式自然周期或样本外策略有效。

2026-10-13 隔离消费后续验证：完成的v9研究必须有精确模型/hash、唯一成功原生Job
与尝试、原冻结输出/引用/内容资格，才可经已有显式candidate模式进入组合合成；
普通consumer和v8 formal模式不因此接受v9。组合请求仍64000字节，预算/调用上限不变。
新增0076只扩展既有SQL隔离分支的版本/hash，保留原数据库名称、周期、事件、Run、
模型、时间、锁、预算和输出绑定校验；不改旧migration或旧事实，不为资格登记/启用模型。
空库可0076→0075→0076往返；已有v9绑定事实时禁止降级，须保留事实并停止选择新资格版本。
主库仍0075，本候选尚未合入或部署。

同一正常产品路径验证3/5候选的买入与不交易：完整池研究→组合选择→确定性风险→
目标/计划→订单或NO_ORDER_REQUIRED→内部成交→账本/现金/持仓→API/BFF/页面。
4个v9场景、4个v7兼容场景、迁移往返及73项相关单测通过；v9三候选买入另启用真实
本地Web/BFF与浏览器断言，保存Workspace、Decisions列表/详情、Paper、Portfolio截图。
页面读回不增加业务事实。截图中的数据过期/循环离线是固定历史合成样本和一次性
诊断心跳的呈现，不是实时运行健康证明；不能把截图、合成成交或green测试当正式验收。
真实来源、正式服务版本/心跳、自然周期、真实模型质量/用量与OOS仍未由本测试证明。

2026-10-13 正式请求准备接线的隔离验证：显式 formal admission 可冻结独立v9
模型/hash/预算身份并幂等读回；权限等待后再次读取原请求指定的精确模型，核对ID、
配置、额度和定价时点后才冻结新Run。fresh/replay模型漂移均拒绝且不新增Job/Run。
正式、旧版本及隔离准入相关22项PG测试通过，Ruff/Mypy通过，定向独立复核无剩余
P1/P2。这里只准备身份，原正式Worker资格与consumer仍拒绝v9；未登记主库、
启用版本、合入部署或启动真实周期。正式全池领取与执行接线仍待后续隔离验证。

正式委员会工厂的后续接线支持显式、精确绑定的v9，使用冻结模型版本选择100479
字节上限，v8仍64000；输入96k/输出8192、最多三次调用及原期限均保持。工厂不
授予领取资格，v9在完整池绑定前仍拒绝调用；隔离和正式模式不可混用。工厂使用
原handler起点减15秒处理宽限，委员会继续减5秒终态预留，不重开期限。受控SDK
替身验证八角色/三阶段，默认入口和正式领取/消费守卫仍未启用v9。
相关37项单位回归、Ruff/Mypy及差异检查通过，定向独立复核无P1/P2；本轮无主库
登记、真实模型调用、服务更新或自然周期，不能据此宣称正式可用。

正式完整池领取接线已在候选代码补齐：正常正式周期创建冻结独立v9身份，Worker
从所选Run派生预算profile，领取前从正式周期事件和原清单重建全部1–5成员，验证
每个成员的版本/hash、冻结context和容量。完整池通过后才领取；handler使用原期限
再验证并绑定全池。预检观察期限不用于执行，不重开调用期限，不重放终态任务。
正式与隔离模式必须匹配原周期事件，本机配置与实际数据库须匹配；旧v8容量不变。
3/5候选的正式和隔离研究路径、尾成员wire超限/context失效共8项PG验证通过；
缺完整周期的v9任务、旧v8周期/领取/准入共25项PG回归及62项相关单测通过，Ruff/
Mypy和定向独立复核通过。SDK使用受控替身，不是实际服务调用。正式v9研究输出
到组合消费和SQL绑定仍关闭，未登记主库、更新服务、启动周期或宣称自然验收通过。
另外2项PG模式误用反例确认正式/隔离不能交叉消费，均0attempt/0SDK，原任务保持queued。

正式v9输出消费与绑定接线：研究输出仍须精确模型/hash、唯一成功Job/尝试、完整
结构化审查及原引用/内容资格；v9还须从原冻结context和周期事件证明完整池及原模式，
不凭版本号交叉选择正式或隔离消费者。组合请求仍64000字节，风险、资金、仓位、
预算、调用次数和执行锁不变。正式proposal/前序target/页面读取按各自冻结身份验证。
新增0077只扩展原formal SQL分支的精确v9版本/hash，保留其他绑定约束，不改0076或
历史migration。原新SQL片段与隔离分支相同造成空库降级不唯一的问题已由PG反例
复现并修正为正式分支独有的等价括号表达式。空库0077→0076→0077以及0077→0075→0077
原定义往返通过；已有正式v9绑定时禁止直接降级，保留事实并停止选择新版本。
正式合同路径的3/5候选买入/不交易4项整链、旧v7/隔离v9的3候选买入/不交易4项
兼容整链、7项迁移/旧版本输出元数据、48项相关单测通过；覆盖跨模式消费拒绝、
reader后模型hash损坏由SQL拒绝、回退限制、幂等回读、成交/账本/资金/持仓及API。
来源、研究和报价使用受控替身，未证明实际SDK用量/质量或自然验收；本轮没有主库
登记/迁移、服务更新、真实周期、外部交易、推送或经济评价激活。
正式v9三候选买入另通过实际Web/BFF浏览器验收，保存Workspace、Decisions列表/详情、
Paper和Portfolio五张截图；默认正式读回不借用candidate读取开关，页面读回不增加
业务事实。私有回执 `.secrets/v9-formal-complete-chain-20261013/verification.json` 记录
候选源码hash和验证范围。截图仍是历史合成来源/诊断心跳，数据过期和循环离线提示
属于该样本真实呈现，不代表当前主服务在线、输入新鲜或正式自然周期通过。

v9正常登记/运行绑定接线：正常setup只有同时显式选择formal profile、full input
capacity和固定100479字节才登记新身份；不从数字推断版本，不创建研究或周期，默认
版本选择及旧v8保持。获准登记时使用正常本地入口，完整参数为：

```sh
uv run --project engine nexus-openai-local setup --backend codex_sdk \
  --formal-stage-capacity --full-input-capacity \
  --stage-time-budget --allow-stage-time-budget-registration \
  --structured-synthesis --aggregate-input-tokens 96000 --max-wire-bytes 100479 \
  --inline-citations --actionable-paper --skip-key
```

以上命令仅说明入口，本轮未对主库执行。登记前拒绝URL中的连接覆盖参数，进入数据库
后核对实际DB，防止声明localhost而实际连接其他目标；独立审查发现的该旧入口缺口
已用六项无网络负例复现并修复。受保护配置应用和宿主加载保留原冻结v9模型ID；
随后修改配置只产生pending apply，不能静默替换已有绑定。正常三候选trigger和宿主
只读预检已在隔离数据库验证，原预算/时限及零attempt保持。
未调用v9手动任务通过原生fenced Job终结取消：采用冻结版本的预算profile，无SDK/
额度消耗，释放原预约；已调用、输出、事件损坏及未知结果仍拒绝，故障不留下部分
取消事实。旧v8兼容、v9模式与3/5候选及故障场景共21项PG、登记6项PG、相关46项
单测及Ruff/Mypy通过，定向独立复核无剩余P1/P2。本轮无新migration，无主库登记、
版本启用、配置修改、服务更新或真实周期；整套候选源码/镜像及集成仍待交付。

## 只读研究负载核查

`nexus-engine-research-workload-inspection --repo-root REPO --run-id RUN_UUID` 通过正常受保护本地配置，在只读事务调用既有数据库工具和正常冻结输入构造器。它不调用 analyze、SDK、Worker，不创建Job、周期、订单或历史替代事实。专用诊断库可显式传 `--database-url-file PRIVATE_0600_FILE`，沿用原诊断URL安全校验。

输出包含冻结输入JSON字节数/hash、Paper上下文是否存在、版本及安全状态，不输出输入正文或账户资料。非法/缺失输入保持unknown或固定安全错误，不把损坏上下文当不存在。`frozen_inputs_bytes`仅计原始输入，不包含阶段提示或前序角色。

`critique` 额外复用正常 Worker 的委员会版本选择与正常阶段请求构造器，以五个已持久化研究角色的原始论点构造完整请求。先核对角色顺序、提示hash、冻结输入hash和引用，再计算提交wire字节数、canonical JSON schema字节数、正常准入使用的input_allowance_units以及请求hash。只有与落盘critique请求hash完全一致时才标为 `request_hash_verified`；缺原hash或不同则仅为 `current_code_readback_only`，前序研究缺失或不一致为 `PRIOR_RESEARCH_UNVERIFIED`，不补造前序输出。schema编码是本地canonical JSON，不是SDK网络封包大小；input_allowance_units不是服务端实际token，`server_token_count=null`。完整SDK调用及动态harness没有重构，顶层 `original_invocation_reconstructed=false` 保持；请求指纹匹配不证明调用耗时、稳定性或超时原因。

隔离成功诊断的Paper上下文若不存在，不能代替包含正式上下文的自然负载验收。原超时已没有精确阶段计时的，不回填、不重放；保留新合法周期的分配时限/阶段/SDK固定阶段计时以判断根因。该CLI的读回成功不代表模型调用成功、业务合格或自然验收通过。


### 含 Paper 上下文的全阶段受控回归

`test_paper_budget_context.py` 的v6成功分支复用正常双候选周期、当前决策报价/前日日线/组合投影合同及正常Worker，只替换外部SDK返回；完整走研究→critique→synthesis及结构校验/原生预算/数据库终态/API读回。上下文在三阶段保持一致，8角色共用冻结输入hash，三阶段计时落盘，API stage_trace为verified/三个complete；已终态任务再次进入不调用模型替身，研究不新增订单，另一候选保持prepared。

这是合成双候选fixture中一个Run的受控研究链，非两个候选研究全部完成，非组合综合/成交整链、真实SDK时延、自然验收或经济样本。模型返回和usage均为测试值，不把elapsed_ms当真实性能结果。原两个失败/不重放分支保留。

2026-10-09在干净main@322c98d9的git archive只叠加本测试复核，3 passed / 13.88s；生产persistence/codex_staged/Worker原件与该revision逐字节一致，未带入经济预算WIP。实际PG55448由fixture创建/清理独立数据库；临时源码克隆随runner结束清理。私有回执位于实现工作区 `.secrets/paper-context-controlled-20261009/verification.json`。不复制个人账户或把正式Paper上下文直接注入“只复制公开证据”的诊断库；现有context→cycle→Run→event校验及诊断无业务状态守卫保持。


### 超时来源诊断

新增可选 `timeout_source`：阶段自身截止为 `stage_deadline`，旧独立调用截止为 `invocation_deadline`，委员会自身截止为 `committee_deadline`；上游 TimeoutError 未能确认来源时为 `unknown`。只在本地计时器实际 expired 或显式单调截止检查满足时确认来源。旧回执缺字段保留原样，不回填；SDK idle 或 Worker 外层超时不能由本字段倒推。未修改 120s/135s、阶段配额、SDK 重试或清理期限。诊断能定位计时器不证明加时会成功，也不代表自然运行验收。
