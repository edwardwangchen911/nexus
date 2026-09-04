# 手动快照诊断与单次研究

`admit` / `advance` 把一个明确的已持久化快照串为“日根 → 对账校验 → 正式诊断”，不调用模型。`research-admit` 在诊断成功后登记原生研究，只有显式 `research-run` 才调用一次已有研究 worker。`proposal-build` 将已保存研究绑定到不可执行草稿。所有命令均不采集券商、不创建 paper 或实盘订单，不启动调度。`completed` 只表示对应流程完成，不等于研究内容/投资决策合格。

`formal-proposal-admit` / `formal-proposal-run` 是独立的正式来源绑定分析入口，复用上述本地配置，不需要重新研究或把非日根研究补写成日根依赖。“正式绑定”只证明身份/时间关联，不表示经济比较或执行就绪。

```sh
python -m nexus.ops.daily_once formal-proposal-admit --request-file /绝对路径/formal-request.json
python -m nexus.ops.daily_once formal-proposal-run --job-id <上一步返回的Job UUID>
```

请求文件为UTF-8 JSON对象（最多1MiB），不含`action`或密钥。字段为：

- `portfolio_snapshot_id`、`diagnostic_report_id`：同一明确、已持久化组合快照与诊断；不自动选最新。
- `decision_cutoff_at`：真实UTC证据截止，不替换组合原估值时间。
- `research_outputs`：`[{"run_id":"…","output_id":"…"}]`，覆盖既有baseline选择的标的，所有研究原始组合身份相同；不使用失败/部分研究替代合格研究。
- `strategy_references`：现有 `FrozenStrategyReferences`，含 `market`（decision/execution session、decision_time、source_policy_id、calendar_authority_id、universe_snapshot_id、quality_report_id）以及 `raw_bar_set_ids`、`adjusted_bar_set_ids`、`sector_exposure_ids`。填写已存在且用途、时点合格的记录，不从NAV/未核单位行情伪造复权历史。
- `idempotency_key`：同一请求重复使用同一稳定键，结果未知时先核已有Job，不换键重放。

入队只从正常确定性选择器生成绑定和Job，调用者不能指定目标仓位或就绪旗标。`formal-proposal-run` 仅消费所给正式绑定Job最多一次，其他任务不受影响；运行需要显式配置的现有对象存储读权限。数据库必须已应用与代码一致的迁移，不能只复制脚本绕过版本检查。未知费用、RESEARCH_REQUIRED诊断和不完整比较保持可见、不可排名与不可执行；新正式绑定方案不会开放Shadow。实际来源/研究不齐时明确拒绝，合成测试通过不代表真实数据就绪。

## 配置和输入

使用项目 Python 虚拟环境，`PYTHONPATH=engine/src`。命令通过 `EngineSettings` 读取当前进程已注入的环境；不会自动读取 `infra.env`，也不创建/打印密钥。数据库必须是已有本地 `postgresql+asyncpg` 地址，主机为 `localhost`、`127.0.0.1` 或 `::1`，不能用查询参数重定向到另一数据库。

`advance` 还需要现有五项 `OBJECT_STORE_*` 显式非空配置（endpoint、region、bucket、access key、secret key），复用 `S3ProviderEvidenceStore.read_normalized` 只读冻结归档。局部 `DailyOnceSettings` 在全局开发默认值填充前检查输入；缺失时在创建 reader/worker 前报 `DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE`。不接受默补的 fixture 配置，不要用虚构空读取器代替真实配置。读取不做自动重试。

从现有受保护的本地数据库/API 确认 snapshot ID、XNYS calendar authority ID 与已收盘的 session。不要把账户事实或真实 ID 复制到公开日志/聊天。指定实际 UTC 手动截止，不回填到旧纽约 18:00 计划时间。快照与日历必须在这个截止已可用，快照自身 `as_of` 不因手动执行改变。

## 命令

以下尖括号是占位符，必须替换为已有本地事实：

```text
python -m nexus.ops.daily_once admit --snapshot-id <已有快照UUID> --calendar-authority-id <已有XNYS日历UUID> --session <YYYY-MM-DD> --cutoff-at <真实UTC截止>
python -m nexus.ops.daily_once advance --root-id <admit返回的root_id>
```

`admit` 只幂等登记完整冻结输入；相同输入返回同一根，不执行诊断。改变截止代表新的显式请求，不用于伪装旧研究重试。

`advance` 最多执行三个原有 worker tick，仅领取/回收该根及经归属、payload、依赖验证的两个子节点；不触碰其他根的高优先级或过期作业。前置未就绪或可用时间未到立即返回，不 sleep、轮询或自动调度。错误不触发券商/模型重放。

输出只含 root ID、阶段类型、状态与安全结果码，不含账户金额/持仓或原始异常：

- `completed`：三个阶段成功；报告仍可能是 `RESEARCH_REQUIRED`，须继续补证据。
- `blocked`：已有终态不满足成功，例如未对账快照、`DIAGNOSTIC_BLOCKED`。不据此创建下游研究/订单。
- `pending`：仍排队或运行；不要把本次观察结束当作请求失败。
- `unknown / DAILY_ONCE_RESULT_UNKNOWN`：通信或提交确认不充分，不宣称回滚。先查看原 root/冻结请求，不能自动重复采集或模型调用。

## 兼容、验证与回滚

### 诊断后的研究

诊断三阶段成功后，使用已登记的显式 model config 与实际新的 UTC 研究冻结时刻。该时刻不能早于诊断作业完成或报告创建；它不改变旧日根 cutoff 或账户快照 as_of。研究始终绑定原日根快照，不自动替换为最新快照。

```text
python -m nexus.ops.daily_once research-admit --root-id <已有日根UUID> --instrument-id <原快照内证券UUID> --research-as-of <实际研究UTC时刻> --model-config-id <已登记模型配置UUID>
python -m nexus.ops.daily_once research-run --root-id <已有日根UUID> --run-id <准入返回的run_id>
```

两条研究命令均要求五项对象库配置显式非空及 `AGENT_RUNTIME_MODE=codex_sdk` 或 `responses`，不接受隐式 fixture。Codex 配置须匹配现有 v1/v2/v3 研究模型，v3 由已有 setup 的 `--structured-synthesis` 显式登记，不自动升级旧配置；Responses 须具备原有 API key 与模型配置且所选模型一致。注册配置复用现有本地研究 setup，不在此处新建凭据或调用外部模型探测。预算沿用 `AGENT_DAILY_BUDGET_USD`，订阅配置中的零 API 计价不意味着订阅使用没有额度限制。

`research-admit` 只在父阶段成功时原子登记快照、原生 run/Job、预算预留及诊断依赖；相同冻结请求返回同一 run，旧 run 若缺少依赖不会被补写。`research-run` 先核验 root/run、冻结上下文、模型与依赖，只领取或回收这一原生 Job，最多一次 worker tick；其他排队作业及过期租约不受影响。不等待未来 available_at、不循环重试；失败先查原 run 的保护状态和日志，不改 cutoff 重放。

研究输出仅含 root/run ID、作业与研究状态、处理数及固定结果码。`prepared` 是已准入而未执行；`pending` 是原作业排队/运行；`blocked` 是失败或不满足完成条件。`completed` 也可能是 partial 研究，必须继续依据现有质量/未知项判断，不生成订单或解锁执行。通信/提交确认不清仍为 `unknown`，不要当作没有产生研究。

### 保存带研究依据的只读草稿

```text
python -m nexus.ops.daily_once proposal-build --root-id <已有日根UUID> --run-id <该根的终态研究UUID> --context-as-of <结果可用后的实际UTC截止> --idempotency-key <本次稳定请求键>
```

不调用模型，也不创建或推进 Job。仍须显式五项对象库配置和就绪数据库；直接复用原草稿构造器、冻结源材料读取和事务。通过准确 root/run 自动取得原组合快照与诊断；不选择最新研究或最新账户快照。输出只含草稿/根/运行 ID、`draft_created`、`context_only` 与 `execution_locked=true`，不含持仓、资金或研究原文。

引用截止和请求键须保持稳定：相同请求返回原草稿；不要用新截止冒充旧请求重试。缺少草稿所需流动性等材料或研究绑定无效时返回 `DAILY_ONCE_DRAFT_INPUT_UNAVAILABLE`；通信/提交确认不足仍返回 unknown，先查原请求不盲目重放。失败或弃权研究只能保存明确的不可用原因，不能被当作合格结论。

新草稿输入使用 `optimization_draft_inputs_v2` 保存准确输出 ID、内容摘要、证据及独立时间。原估值时间不变，研究不参与目标仓位计算；页面“冻结研究依据”只展示这一保存值，刷新不调用采集/模型。旧 v1 草稿不补写 context，旧正式优化入口拒绝把 v2 context 当准入权威。这不等于正式研究约束目标或完整经济比较已完成。

### 保存所选日流程阶段的审计记录

```text
python -m nexus.ops.daily_once audit --root-id <已有日根UUID>
python -m nexus.ops.daily_once audit --root-id <已有日根UUID> --run-id <明确绑定该根的研究UUID>
```

这是一次显式本地审计动作，不是业务阶段执行：读取日根、精确质量/诊断子阶段，以及可选的指定研究；记录真实状态后，尝试用既有审计清单封存其所在链段。不采集、不调用模型、不执行 worker，不创建方案或订单。失败或排队中的阶段也保留实际状态，不能将本命令成功解释为研究、决策或整个日流程完成。

未提供 `--run-id` 时不自动选择最新研究，也不表示该根没有研究。提供时核验既有日根依赖和冻结输入，不为旧研究补写绑定。本片范围不包含正式方案、paper 或绩效；这些阶段必须依据各自证据核验，不能由审计状态推导就绪。

需要显式配置的既有本地数据库与五项对象存储配置；对象存储须为本地地址。本命令会向本地业务库追加审计事件，并按既有 create-only 方式保存脱敏清单对象，不是“零写入”操作。完整阶段引用保存在受保护业务库，不复制到聊天或公开日志。

观察使用独立、一致的只读快照，不领取或锁住业务任务；正在执行的研究只显示最近已提交事实，不等待模型结束。`observed_at` 是该次保存的观察时间，不是旧日根的证据截止，也不承诺返回时仍为最新状态。

相同所选阶段事实重复观察复用原事件；阶段事实改变时追加新事件，原冻结截止和历史事件不改变。`status=recorded` 表示观察记录已确认保存，`manifest_status=sealed` 才表示清单真正覆盖该事件序号。当天清单已经封存而尚未覆盖新事件时，后者保持 `pending`，不覆盖旧清单，不改用未来日期。两段分开提交；对象存储或提交确认异常保持相应的 `unknown`，不能把已保存事件称为已回滚。先核对已有记录，不重跑研究或其他业务来“重试审计”。

### 保留旧行为

不修改旧 `daily.pipeline` schema，不启用旧同名研究/优化阶段，无新增依赖。正式绑定入口要求迁移 `0040_formal_decision_cutoff`：新增可空证据截止与校验，旧记录保持 NULL、不回填。停止使用新命令即可回滚使用方式，不删除历史 Job/诊断；数据库只有不存在正式绑定记录时才允许按迁移流程降级，有记录时明确拒绝降级，不能通过删除证据强行回退。现有长驻入口不是本命令，不能用“启动全部 worker”代替这次显式推进。

测试使用隔离 PostgreSQL 与合成快照：入口→原 worker→真实诊断/原生研究状态持久化；验证未对账/未来事实/越界子节点、并行根隔离、未来可用时间立即返回、缺依赖旧研究不补写。实际研究证据驱动方案与真实案例全链路仍待独立验收，不能以本片测试通过宣称整个 L1 完成。
