# v6 研究时间预算：显式注册与正式启用

此入口只登记配置，不启动研究、调用模型、创建周期或订单，也不更新服务。
默认 setup 保留 v5；不带显式允许参数的 v6 注册只接受 nexus_test_/nexus_diag_。
隔离诊断执行入口始终仅接受实际匹配的 nexus_diag_，不受下面的开关影响。

## 正式注册前的边界

- 本地代码、宿主、API、Worker 必须核对同一 release revision；服务更新须先获用户确认。
- 确认受保护配置中的本机数据库目标。注册在同一事务首次写入前读取实际库名，
  即使显式允许也要求与配置完全一致；不因开关绕过其他合法性和配置哈希检查。
- 注册会使 v6 进入未来新研究的候选范围。没有 PASS 版本选择覆盖时，正常准入选择
  最新合格模型；已有 PASS 选择仍按既有规则优先，不自动撤销或伪造评估。
- 现有 Run 的配置和哈希已冻结，不能换版本或重放错过、失败、未知槽位。

## 获准后的正常入口

从主仓库运行，配置解析沿用正常产品入口；不在命令或输出中展开连接串或凭据：

```sh
uv run --project engine nexus-openai-local setup \
  --backend codex_sdk --structured-synthesis --aggregate-input-tokens 96000 \
  --max-wire-bytes 64000 --inline-citations --actionable-paper \
  --stage-time-budget --allow-stage-time-budget-registration --skip-key
```

重复执行只复用同一 v6 配置，不新增研究。省略 stage-time-budget 则仍登记 v5；
省略显式允许开关不能向业务库登记 v6。注册参数不进入运行预算哈希，也不改变
135 秒 Worker、原起点 115 秒 Committee、三阶段调用上限或输出/使用量校验。

## 验收和回退

注册后只读核对 v6 身份、未来新 Run 的实际冻结版本及同版本服务；正常完整研究、
组合决策、确定性风险与内部模拟执行、页面读回仍须独立验收。单次公开诊断成功或
合成 SDK 的接线回归不能代替自然周期、自然成交或样本外验证。

回退只影响未来选择；保留 v6 配置、已冻结 Run 和全部使用量/输出事实，不删除或
修改历史。单纯重新运行默认 v5 setup 不保证恢复选择：既有 v5 不会因幂等注册变新。
若当前没有合法的 v5 PASS 选择，不能凭空制造通过记录；先停止后续自动研究并诊断，
再按已有版本选择机制和用户授权处理。禁止用删除 v6 或篡改 created_at 来回退。

## v7 候选的隔离命令链

`codex-sdk-chatgpt-v7-stage-capacity-candidate` 仅用于资格验证，尚未进入正式默认选型或
执行准入集合。下列命令使用当前包含候选代码的仓库；不更新主线服务。需要已迁移的
本机 `nexus_diag_` 诊断库，且其中没有真实账户、券商绑定、Paper 账户、周期或订单。
URL 只存于属主为当前用户、0600 的普通文件，不在命令或输出中展开。代码在首次写入前
核对实际数据库与配置名称一致；`--allow-stage-time-budget-registration` 与候选模式互斥。

```sh
uv run --project engine nexus-openai-local setup --backend codex_sdk \
  --structured-synthesis --aggregate-input-tokens 96000 --max-wire-bytes 64000 \
  --inline-citations --actionable-paper --stage-time-budget --skip-key \
  --candidate-stage-capacity --database-url-file PRIVATE_0600_FILE

uv run --project engine nexus-openai-local admit-research \
  --research-request PRIVATE_REQUEST_JSON \
  --candidate-stage-capacity --database-url-file PRIVATE_0600_FILE

uv run --project engine nexus-engine-research-budget-diagnostic \
  --repo-root REPO --database-url-file PRIVATE_0600_FILE --run-id RUN_UUID \
  --output-dir NEW_PRIVATE_LEAF --candidate-stage-capacity
```

准入请求沿用 `LocalResearchAdmission`：`request` 包含诊断库中已有的 instrument、snapshot
及 decision_time，`model_config_id` 明确选择该库规范候选模型，`idempotency_key` 必须以
`budget-diagnostic:` 开头。注册和准入不调用模型；诊断执行会调用现有套餐的正常 SDK。
诊断库只允许一个 prepared Run 和一个尚未尝试的 Paper 研究 Job。输出父目录须为当前
用户所有的0700目录；叶子目录必须全新，先持久化 started 回执再进入正常 Worker。
失败、未知或观察超时后只读回执和数据库状态，不通过改目录或换 Job 重放。

候选使用180秒研究预算、195秒 handler 和210秒 lease，保留原始绝对起点；claim 与
执行前均重新核对冻结身份。成功只证明该隔离研究运行，`formal_acceptance=false`；
合成 SDK 响应不证明实际模型时延，缺失行情的 partial 不成为合格组合决策。完整多股票
组合链、真实调用和合法自然周期仍须单独验收。回退代码保留候选配置、Job/Run 和回执，
默认 Worker 不领取或回收候选任务；不得把候选身份改写成正式版本。

若需从已有正式 Run 创建公开证据负载，使用正常
`nexus-engine-research-budget-case --repo-root REPO --database-url-file PRIVATE_0600_FILE
--source-run-id SOURCE_RUN_UUID --candidate-stage-capacity`。目标须为全新的、已迁移且清理过
迁移种子数据的专用诊断库；复制前验证实际目标完全匹配。只复制白名单公开来源及绑定，
不复制账户、Paper 周期或正式上下文，严格读回后冻结新的规范候选 Run。该入口本身
不调用模型；随后只对该新 Run 调用一次 diagnostic。`--synthetic-paper-load` 仅模拟
Paper 输入负载，不能当作正式上下文、报价或执行权威。

隔离组合验证的研究读取边界为
`load_actionable_research_output(..., candidate_stage_capacity=True)`。该参数仅供内部显式
资格入口使用，不是正式 API 的请求开关；默认仍拒绝候选版本。先只读核对实际隔离库、
规范模型和冻结哈希，以及唯一成功 Paper Job/attempt，再执行原有研究内容、证据、
行情、上下文和有效期检查。原正式版本集合不变；Job 成功不使 partial 自动成为可执行
结论。

内部组合消费者 `admit_portfolio_synthesis`、`persist_portfolio_proposal` 和
`load_portfolio_proposal` 也接受显式 `candidate_stage_capacity=True`；在任何业务查询、
账户锁或写入前验证严格布尔参数和实际隔离库，再将资格传到原研究读取器。综合缓存
重放按原 `frozen.admitted_at` 逐项重新核对冻结来源及当前消费模式，不能从候选模式
切换回默认路径绕过版本门。提案的缓存、绑定和递归已有目标基线沿用同一参数。
默认正式消费者仍使用 `False`，不开放 API 请求参数或新增正式版本。

综合 Worker 的内部正常入口
`run_portfolio_synthesis_worker_once(settings, job_ids=(EXACT_JOB_UUID,), candidate_stage_capacity=True)`
先核对实际隔离库与配置数据库一致；直接 dispatch 入口同样要求实际隔离库、严格布尔
参数和精确一个 UUID。候选 transport 只接受规范候选模型。领取在精确 Job 锁内重新
核对唯一 Job、冻结 synthesis Run/model/当前定价及各只候选研究原 reader 的内容与证据；
checkpoint 在模型调用前再次复验。沿用综合单次调用与120秒 lease，不复用研究的
210秒 lease。候选综合不自动回收或 orphan reconcile；通用 claim/reclaim/reconcile
均排除由候选版本绑定的综合 Job。已终态不创建第二次调用。

目标激活 `activate_portfolio_target(..., candidate_stage_capacity=True)` 在任何激活缓存、
历史 claim 或已有目标返回前重新读取并校验提案，候选资格不会由缓存返回替代。
参数继续传至私有目标调整批次及其缓存/读回；三个入口先验证严格布尔参数和实际
隔离库，新批次只允许目标激活路径。公共批次创建仍不开放候选参数，确定性风控、
有效期、基线、资金、审批和执行锁条件保持。资格拒绝不得新增目标、订单或幂等记录。

周期内部入口 `start_schwab_agent_paper_cycle`、`advance_agent_paper_cycle` 接受显式
`candidate_stage_capacity=True` 与 `candidate_model_config_id=EXACT_MODEL_UUID`；
`resume_schwab_agent_paper_cycle` 只接受模式，从首条不可变 `CYCLE_PREPARED` 事件读回
模型 ID 与规范哈希。候选必须绑定候选批次，不能与版本评价通道混用；实际隔离库、
模型创建时点及定价有效期均须通过。推进先取账户锁，再读槽位/校验模式，最后声明
幂等身份；恢复在声明前拒绝模式切换，终态缓存也不能绕过身份检查。
研究准入使用明确候选模型而非“最新配置”，仍创建原生 Paper Job、沿用原周期/标的
幂等键；研究读取、组合综合、提案及目标激活继续显式传递同一模式。默认正式路径与
正式版本集合不变，没有新增 CLI/API 或调度激活开关。

后续宿主/执行调用及3/5股票完整成交、账本和页面仍待统一验证。
以上消费者/Worker/周期接线及拒绝/旧正式路径兼容回归不是候选成功整链、实际成交
或自然周期验收。回滚本轮入口代码会使已冻结候选周期不可推进，不删除或改写其事实；
默认 Worker 保持隔离，不能把候选周期重标为正式周期。

隔离候选的数据库绑定门禁由 `0067_agent_output_versions` 与当前应用迁移头统一：
原先数据库只接收 v4，而应用正式合同已包含 v5/v6。迁移保留原数量、重复标的、
输出终态及锁检查，正式版本限定为冻结的 v4/v5/v6；v7 额外要求实际隔离库、周期
首事件的显式模式、规范模型 ID/hash/参数/定价和研究 Run 的冻结 hash、actor、
周期/标的 key、时点及终态。不是正式 v7 激活。迁移不修改历史行或回填旧失败周期；
降级恢复原 v4 门禁，已有 v5/v6/候选事实保留但新绑定将再次被拒绝。

`test_candidate_multistock_path.py` 新建日期一致的3/5股票隔离场景，以正常候选
Worker/reader、组合综合 Worker、初始基线确认、周期恢复和目标激活入口验证
受控买入/不交易、风险/订单明细与终态重放不重复。SDK回复由合成输入替换；
隔离模板使用既有测试时钟，不能将这些日期或结果当作真实来源或历史运行事实。
买入场景继续通过正常 `collect_agent_paper_window` 和 `paper_once.run_once`
消费每笔订单的精确五分钟窗口，验证两笔内部成交、费用现金扣减、预留释放、
持仓增量、账本借贷平衡和 Paper 订单读服务的成交明细。同窗口不同观察时刻
只读回原结果，provider 调用、窗口、成交及成交账本事务均不重复。
行情 provider 也使用受控回复；订单准入沿用已有 fixture 的价格权限 stub。
Paper 读服务和提案/目标读取链显式传递隔离候选模式，先校验实际数据库隔离。
默认读取仍拒绝候选订单；HTTP 请求参数不能开启模式，也不自动识别或启用。
隔离页面资格入口可显式设置 `NEXUS_CANDIDATE_STAGE_CAPACITY_READS=true`：
要求 `NEXUS_ENV=test`、显式本机 asyncpg 的 `nexus_test_`/`nexus_diag_` 库、
禁用 Provider 与 Agent Paper 调度，禁止连接重定向参数；Paper 路由再次核对
实际数据库且仅允许 GET/HEAD，保留 HMAC。该开关不激活研究 Worker、正式模型
或交易能力。关闭开关即可恢复默认候选拒绝，不删除已有隔离事实。

候选整链的日期场景在各正常写入前设置隔离库新行的合成默认时钟，避免共享模板
的 Jan1 默认导致 BFF 判定研究早于候选；不修改已落盘记录或放宽 BFF 时序检查。
开启 `NEXUS_CONTROLLED_BROWSER=1` 的矩阵沿正常 API/BFF/浏览器核对3/5候选的
Workspace、Decisions 列表/详情、Paper、Portfolio，并比较浏览前后业务事实。
截图在私有目录按本地提交版本归档；只有提交后复跑才作为该版本页面证据。
Paper 页面把收盘绩效缺失与当前周期/订单事实分开呈现：未发布绩效时仍展示已有
运行记录，不伪造收益或持仓绩效，取消操作的原权限门保持不变。
这些仍是受控 SDK/行情的隔离整链，不是实际来源/真实 SDK 或自然运行验收。
当前候选分支只有 `0067_agent_output_versions` 一个迁移头，可以独立从 `0066`
验证升级与回退。另一工作区尚未合入的 `0067_formal_budget_binding` 不阻塞本分支
单头验证；以后集成经济分支时须串行整理迁移图，不得直接部署两个 sibling heads。
若迁移已应用，不能改写其历史身份；未应用的后续迁移可按最终集成顺序调整。
代码集成、隔离迁移通过与业务库迁移/服务更新是不同事实；业务迁移和更新仍须确认。

### 待激活的正式容量身份 v8

`codex-sdk-chatgpt-v8-stage-capacity` 是独立于冻结 v6 和隔离候选 v7 的待激活身份。
规范配置哈希包含180秒研究、195秒 handler、210秒 lease 及逐阶段预算；注册不变更
旧版本或历史 Run。以下正常入口仅注册模型和既有提示词，不创建研究 Job/Run，
也不调用 SDK、行情或交易接口。业务库注册仍须用户确认；本轮仅验证隔离测试库。

```sh
uv run --project engine nexus-openai-local setup --backend codex_sdk \
  --structured-synthesis --aggregate-input-tokens 96000 --max-wire-bytes 64000 \
  --inline-citations --actionable-paper --stage-time-budget --formal-stage-capacity \
  --allow-stage-time-budget-registration --skip-key
```

入口在写入前核对实际数据库名称与配置一致；待激活模式不能与候选模式混用。
重复注册核对规范哈希和全部持久化模型字段，冲突拒绝，不覆盖旧行。默认研究选型
排除待激活 v8；显式指定 v8 在幂等声明之前返回
`CODEX_FORMAL_STAGE_ACTIVATION_REQUIRED`。此版本未加入 SDK/正式 Paper 版本集合，
没有调度或执行启用开关。注册成功不代表预算已经在正式进程生效。

后续须独立完成正常周期准入、正式消费者及数据库绑定接线和验收后才能设计激活。
回滚代码时保留注册记录，不删除或重标历史；保持当前选型与执行锁。


待激活 v8 的内部研究执行接线使用
`run_agent_worker_once(settings, job_ids=(EXACT_JOB_UUID,), formal_stage_capacity=True)`。
仅允许本机 SDK、精确一个 Paper Job，拒绝连接 URL 的 host/hostaddr/port/dsn/database/dbname
重定向参数，并在领取前核对实际数据库名称。模式不能与 v7 候选混用。
这不是正常周期激活入口：默认准入仍拒绝 v8，CLI 不开放本参数。

该内部入口固定195秒 handler / 210秒 lease；在 Job 锁内和执行前重新核对规范模型、
当前定价、Run/model 冻结版本及哈希、Job输入哈希、标的/决策时刻和首尝试状态。
默认 claim/reclaim 排除待激活 v8，不能借默认135秒 Worker消费，也不自动回收其尝试。
研究从原 handler 截止时间扣除15秒取得180秒绝对预算，再保留5秒终态及每次7秒 SDK
清理预算，阶段上限为60/45/45秒。数据库准备及前序阶段耗时不重置后序阶段起点。
正式身份使用独立版本；v6/v7身份、配置哈希和旧预算行为保持。

隔离验证包含正常 Worker 入口缺少冻结输入证据时终态拒绝、SDK调用为零且不重放；
规范合成 Job 的锁内领取及拒绝不产生 attempt/lease；受控研究三阶段预算反例覆盖
准备耗时0/10秒。测试不创建真实研究证据，不调用模型/券商，也不注册业务库或部署。
后续仍需正常正式周期准入、消费者和数据库输出绑定接线及自然验收。
