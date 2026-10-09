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

组合周期、综合 Worker 和目标激活尚需统一传递该隔离合同；以上消费者接线及拒绝/
兼容回归不是候选 3/5 股票成功整链、实际成交或自然周期验收。
