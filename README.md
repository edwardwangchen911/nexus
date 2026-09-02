# NEXUS

NEXUS 是私人投资研究、回测与模拟交易工作台。它不提供投资建议，也不承诺收益。

## 当前状态

- `phase0_offline_ready`：美股研究、确定性回测、三方案比较、Forward Shadow、硬风控和内部模拟交易已通过离线发布门禁。
- Mac 本地的复星 `local_read_only` 链路已启用：现有 Gateway 经一次性本地刷新器将 1 个账户、
  3 个现金币种、12 条持仓、全部 12 个美股持仓的 Quote 和日 K 持久化到 PostgreSQL/对象存储，
  并通过 API/BFF 在本地组合页和可切换标的的市场页展示复星来源、更新时间与质量状态。组合页
  按原币种确定性展示现金、持仓市值、总资产与配置权重；没有合格 FX 证据时不生成跨币种合计，
  美股持仓可进入对应的已持久化 Quote 与日 K 页面。
- 2026-09-02 11:33（北京时间）本地真实刷新已完成现金、持仓、未终结订单双读及现金流
  日期窗口、分页、稳定 ID 与重复/冲突检查：12 条 CashFlows 已入库，`cash_flow_quality=PASS`，
  `display_readiness=DISPLAY_READY`、`decision_readiness=DECISION_READY`、`execution_locked=true`。
- 第一份正式真实组合诊断已持久化，并通过现有 API/BFF 和组合页展示。报告含 6 个诊断项，
  结论为 `RESEARCH_REQUIRED`：现金占比和集中度可用，但历史市场证据仍为
  `MARKET_EVIDENCE_UNAVAILABLE`，不能据此宣称风险低或生成订单。
- 2026-09-02 本机研究输入已覆盖 10 个真实持仓：10 份 SEC 财报、304 条财务事实，
  每个研究页均绑定原币种持仓权重、现金占比和 6 个既有诊断项。另 2 个持仓尚不满足
  当前公司 10-K/10-Q 入口，不伪造覆盖；真实 Agent 运行和正式方案组仍为 0。
  模型 Key 不影响这些资料的读取，历史价格证据缺口与执行锁保持不变。
- CashFlows 使用 Provider 的 `tradeDate` 作为业务日期；按官方 OpenAPI 的
  `direction=1/2` 规范流入/流出金额，保留流水类型原代码，不猜测“入金/分红”等类别。
  `createdAt` 无时区时精确时刻保持空值，不擅自指定时区。未来对账不完整的快照仍只提供
  显式标注的初步诊断，不能成为正式优化基线。
- 该状态只是个人 Mac 本地只读使用。当前只区分无网络 fixture 与本地真实只读，不设“生产模式”，也不代表复星模拟盘交易或实盘激活。
- 美股真实数据传输与耐久采集链路已实现但默认关闭：Alpaca、SEC、FRED/ALFRED
  使用固定源、冻结 universe、原始证据归档和幂等入库；未配置或启用真实 Provider
  凭据时继续使用无网络 fixture。
- Phase 2 策略进化实验室已完成本地验收；候选策略只能经过确定性回测、Shadow 和治理门禁，Agent 不能自行晋级或交易。
- `Phase 3A offline_ready / Phase 3B external_activation_blocked`：券商模拟执行状态机只在无网络 fixture 中闭环，复星外部模拟写入未激活。
- 分钟级盘中监控保持 `Intraday Monitor offline_ready / external_data_activation_blocked`：只读 Snapshot、确定性波动指标和证据页面已实现；采集使用独立 `provider` 队列、Provider-only worker 和强制 RLS 数据库角色模板，真实 Provider 请求及角色授权仍默认关闭。
- `00656.HK` 继续仅作只读持仓展示；实盘页保持锁定，任何未来真实订单仍须逐笔人工确认。

最终本地证据和外部门禁见 [CI 与离线发布门禁](docs/runbooks/ci-release-gates.md) 与 [Task 21 审查](docs/superpowers/reviews/2026-08-27-task-21-final-operational-gates-review.md)。

## 命令

### 本机 OpenAI Agent 接入

当前本机使用官方 Python [Codex SDK](https://learn.chatgpt.com/docs/codex-sdk)
（`openai-codex==0.147.0`，自带对应运行时），不再维护自写 CLI 输出解析器。
复用本机已有 ChatGPT 登录，无需模型 API Key，也不读取、复制或展示认证文件。

```bash
uv run --package nexus-engine nexus-openai-local setup --backend codex_sdk
uv run --package nexus-engine nexus-openai-local status --backend codex_sdk
uv run --package nexus-engine nexus-openai-local run-once --backend codex_sdk
```

`setup` 仅登记模型和既有提示词、检查登录，不做模型推理。`run-once` 处理一个已登记的研究任务：
主程序从数据库读取冻结的财报及脱敏组合背景，交给一个临时、只读、无工具的 SDK 会话，
一次生成既有 8 个角色的结果，沿用原有证据校验、数据库、BFF 和研究页面。
关闭模型请求/流重试和任务自动重试；35 秒超时后关闭 SDK，不启动后台轮询。
工具、插件、MCP、hooks 和账户写入均不开放，不向 SDK 继承数据库或券商环境变量。
财报期间与来源保留，模型只能引用冻结证据；缺合格价格依据仍为 `partial / watch`。
页面区分 ChatGPT 套餐额度与 API 美元费用，并展示真实 token 用量；零 API 美元不代表免费或无限额度。
64,000 输入 / 8,192 输出为本地验收阈值，不承诺能中途硬截断套餐计费；SDK 会话也有上下文开销。
新增依赖仅位于 Python 引擎，无新服务、数据库迁移或自动调度。
停用此入口即可停止调用；如需回到下面的 API 方式，显式使用 `--backend responses`，保留审计历史。

组合页的「Agent 研究」列直接读取已有冻结资料和研究结果，按持仓展示状态、报告截止时间、
摘要及证据缺口；打开报告或刷新页面不会登记任务、调用模型或访问券商。
研究状态有独立读取时间，不改写账户快照，也不改变 `decision_readiness` 或交易锁。
此接线仅新增可选响应字段，无依赖或数据库迁移；回滚应用代码即可撤回展示，已有研究不受影响。

#### 可选：独立 API Key 的 Responses 接入

复用已有 Responses/委员会和预算逻辑，不新建 Agent 框架。默认固定
`gpt-5.4-mini-2026-03-17`，模型与 8 个已有提示词可幂等登记到本地数据库。
单次研究共享 64,000 输入 / 8,192 输出 token 上限，按 2026-09-02 核实的
[官方标准价格](https://developers.openai.com/api/docs/models/gpt-5.4-mini)最多约 $0.085；
沿用现有每日预算检查（默认 $3）。不启用自动调度，不开放交易。

在仓库根目录执行：

```bash
uv run --package nexus-engine nexus-openai-local setup --backend responses
```

以下 API 方式的命令均须显式加 `--backend responses`。`setup` 登记本地配置，在本机终端隐藏输入 API Key，保存到被 Git 忽略的
`.secrets/nexus-openai-local/api-key`（0600）；也接受进程中的 `OPENAI_API_KEY`。
不要在聊天、命令参数或受版本控制的文件中填写密钥；此 API 方式不复用 Codex 登录凭据。
`setup --skip-key` 可先完成无网络配置；`status` 只返回脱敏状态。
录入后 `setup` 自动执行一次 `verify`：只向官方 API 发出一次不含账户数据的连通性请求，输出上限 128 tokens、
超时 30 秒、无 SDK 自动重试；不能把连通性通过当作真实研究完成。

已有证据准入和任务登记完成后，`nexus-openai-local run-once` 只处理一个已有研究任务。
缺少研究快照、合格价格证据或模型密钥时，不生成假研究；当前历史风险证据缺口仍须补齐。
密钥暂不可用时，可先运行 `nexus-openai-local prepare-research --instrument-id <标的 UUID>`：
只从本地已入库的 SEC 财报冻结研究输入，并返回现有研究页路径；不请求 SEC、复星或模型，
不登记研究任务，不消耗模型预算。相同标的、时点和证据重复执行不会重复创建快照。
本入口不会把展示用 K 线提升为研究证据；真实财报尚未入库时返回
`RESEARCH_EVIDENCE_UNAVAILABLE`，不创建空快照。资料齐备后仍沿用原有研究 API/Worker；
价格依据缺失时委员会结果保持不完整、无仓位动作。停止使用该命令即可停用，无 schema 迁移。

真实财报使用同一个入口：`nexus-openai-local refresh-research --instrument-id <标的 UUID>`。
只需在本机已忽略的 `infra/.env.local` 中配置 `SEC_CONTACT` 联系邮箱（随请求标识发送给 SEC，
不是登录账号或密钥），不需要 OpenAI、FRED 或券商凭据。单次仅处理该美股标的的最新
10-K/10-Q：最多 3 个公开 GET（标的目录、Company Facts、当前 submissions），60 秒总窗口，
禁止跳转、无自动重试、首错即停。复用已有 SEC 归档与财报/财务事实入库逻辑，
核对 ticker/交易所/CIK 后生成研究输入及研究页路径，不启动模型或后台任务。
仅有外国发行人 6-K 文本时，使用 `nexus-openai-local refresh-disclosure --instrument-id <标的 UUID>`。
同样最多 3 个公开 GET（目录、submissions、最新 6-K 原文），无重试；不请求 Company Facts。
原文归档并校验哈希后进入既有数据库、冻结研究、Codex SDK 与研究页；正文按需展开，
明确区分披露与结构化财务指标，不从文字生成财务数值，也不提升行情或执行权限。
无新依赖或数据库迁移；停用此命令即可停止采集，既有财报入口不变。
ETF 使用 `nexus-openai-local refresh-fund --instrument-id <标的 UUID> --accession <497K 申报编号>`。
最多 4 个公开 GET（SEC 基金目录、submissions、申报索引、原文），无重试、首错即停。
目录与索引交叉核对 CIK、基金系列、份额类别和 ticker；仅冻结指定的单基金摘要招募书，
不自动声称它是最新版本，也不把招募书当作当前基金持仓或公司财报；不改写持仓主表。
实际上市交易所由 SEC 目录与 submissions 交叉核对；旧复星主表的 `XNYS` 市场占位不作
上市交易所依据。返回值分别标明本地主表与 SEC 交易所，不改写持仓、行情或主表。
研究页直接展示快照绑定的 SEC 财报与财务原始值，含统计期间、接受/入库时间、版本和哈希；
无需模型 Key，不将资料展示视为 AI 结论。现有准入 API 新增可选只读 `inputs`，无数据库迁移。
已持有标的同时冻结 `inputs.portfolio_context`，仅含该标的在原币种资产内的权重、现金比例、
快照时点与脱敏诊断，不含账户号、现金金额、持仓数量或订单标识。不跨币种估算资产。
组合背景以规范 JSON 内联保存在既有 DataSnapshot；研究登记时单独绑定其哈希证据，
不冒充财报或行情准入。`get_portfolio_exposure` 和 `get_portfolio_diagnostics` 读取该冻结内容，
拒绝其他快照/标的/诊断和内容漂移；`get_proposal_comparison` 仍明确不可用。
旧快照的 READY 不会替代现金流对账，缺价时不计算比例，未对账时不携带正式诊断。
市场页研究入口使用本次服务端读取时点，保留报价原时间，避免旧报价时间隐藏新入库的财报。
SEC 晚间接受与次工作日申报日期分别保留，不因日期先后误拒财报；财务事实选择仍要求
接受时间、申报日期和证据可用时间均不晚于决策时点。规则依据见
[SEC 申报状态说明](https://www.sec.gov/submit-filings/filer-support-resources/how-do-i-guides/determine-status-my-filing)。
仅写本机数据库及本机对象存储；重复财报不重复入库。停用此命令即可停止采集，无新服务或迁移。
Responses 使用 `store=false`，工具续调用显式携带上下文；预算、证据校验和执行锁不变。
本次不改变数据库 schema；停用此本机入口即可停止模型调用，保留配置与审计历史。

### 常用开发命令

```bash
npm install
npm run lint
npm run typecheck
npm run test:unit
npm run test:e2e -- --workers=3
npm run build
uv run pytest engine/tests -q --import-mode=importlib
install -d -m 700 "$PWD/.nexus-pytest"
NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" \
  uv run pytest integrations/fosun_gateway/tests -q \
    --basetemp="$PWD/.nexus-pytest/gateway"
```

## 本地运行与运维

### Mac 本地复星只读刷新

统一使用 `nexus-fosun-local` 管理本地只读链路。先做无网络、无数据库的配置检查，再只读查看已持久化状态：

```bash
uv run --package nexus-engine nexus-fosun-local init
uv run --package nexus-engine nexus-fosun-local doctor
uv run --package nexus-engine nexus-fosun-local status
```

首次使用执行一次 `init`：它会在 Git 忽略的 `.secrets/nexus-fosun-local/hmac.env` 中生成
broker/market 两套独立的本地 HMAC 身份，文件权限为 `0600`。重复执行只校验并返回
`exists`，不覆盖、不轮换、不打印任何身份值。

CLI 会自动读取已忽略、不入库的 `infra/.env.local`，并映射 `.secrets/nexus-fosun-local/`
中现有的 broker/market TLS 文件和回环 Gateway 端口。显式环境变量始终优先，无需为日常检查手工
`export` 数据库、对象存储和 TLS 路径。

`doctor` 只输出缺失的环境变量名和 TLS 文件是否存在，不打印配置值、不连数据库、不连复星。
`status` 只读本地 PostgreSQL 中最新快照的脱敏汇总；本地数据库未运行时返回稳定的
`LOCAL_DATABASE_UNAVAILABLE` / `database`，不暴露连接串或底层异常。只有在既有安全 launcher
已加载本地配置、且 Gateway 为 `local_read_only` 时，才显式执行：

```bash
uv run --package nexus-engine nexus-fosun-local refresh
```

刷新器从真实持仓确定性提取、去重并排序 `us...` 代码；最多接受 12 个美股标的，使用一次批量
Quote 和每个标的一次日 K 请求。账户发现和持仓范围发现各一次，随后按
`Cash → Holdings → OrderList → Holdings → OrderList → Cash` 完成一次无重试双读；12 个美股标的时
随后读取一次 CashFlows，首次覆盖 7 个自然日，之后从上次水位回溯 3 天补齐至当前日。单页 CashFlows、12 个美股标的时单次总
Provider 请求最多 22 次。超过范围、合同不匹配、订单读取失败或双读不一致时 fail-closed，
数据库事务不会覆盖上一份可用快照；现金流读取或分页不完整时保留可展示快照，但决策状态保持待对账。浏览器只读取已持久化数据，不能触发 Gateway 或 Provider 请求；
该入口不包含自动调度、订单写入、账户写入或任何交易激活。组合页的“刷新页面数据”仅重新执行已持久化
组合与快照的 BFF GET，不会 POST `/api/v1/broker-sync-runs`。`00656.HK` 仍仅作只读持仓展示，不请求
Quote 或 K 线。

组合快照同时展示两类就绪度：“展示状态”只回答现金/持仓事实是否可用；“决策状态”回答
是否完成对账、时效和风控门禁。因此 `PARTIAL` 快照仍可正常展示真实资产，但不会被误用为优化或交易基线。

现金流日期精度迁移 `0033_cash_flow_business_date` 只允许精确时刻为空，旧记录不变，
日期精度记录必须具有稳定 Provider ID。空表可迁移往返；已存在此类记录时降级会拒绝，
不得通过删除记录或编造时间回滚。此时保留兼容 schema 并以前向修复恢复代码。

- [本地开发与 Compose](docs/runbooks/local-development.md)：不强制 Docker Desktop，Docker Desktop、OrbStack 或 Colima 均可。
- [备份/恢复演练](docs/runbooks/backup-restore.md)
- [CI 与离线发布门禁](docs/runbooks/ci-release-gates.md)
- [Provider 故障处置](docs/runbooks/provider-outage.md)
- [分钟级盘中市场监控激活门禁](docs/runbooks/intraday-market-monitor.md)
- [密钥与证书轮换](docs/runbooks/key-rotation.md)
- [复星 SDK admission](docs/runbooks/fosun-sdk-admission.md)
- [复星只读激活门禁](docs/runbooks/fosun-read-only-activation.md)
- [PostgreSQL 事务权威 ADR](docs/adr/0001-postgresql-owner.md)
- [复星独立只读网关 ADR](docs/adr/0002-fosun-gateway-boundary.md)

## 安全边界

- 首期仅支持研究、回测和模拟交易；实盘交易默认关闭，未来任何订单均须逐笔人工确认。
- `00656.HK` 仅作只读展示；Agent 只能创建可追溯的研究意图，不能下单或修改风控。
- 数据缺失、过期、未对账或证据不足时，系统应观望，不生成订单。
- 不要把密钥、券商凭据、账户数据或生产配置写入仓库、日志、测试快照或响应内容。
