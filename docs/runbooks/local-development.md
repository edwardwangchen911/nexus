# 本地开发与 Compose runbook

## 边界

本流程只启动 synthetic fixture 环境，用于研究、回测、Shadow、内部模拟盘和运维验证。它不安装复星 SDK，不读取真实账户，不启动任何交易写入路由。`app` profile 不包含 `fosun-gateway` 或 `broker-worker`；不要为了本地开发启用 `broker` profile。

Docker Desktop 不是必需品。任何提供 Docker API 与 Compose v2 的兼容运行时都可以，包括 Docker Desktop、OrbStack 或 Colima。

## 预检

```bash
docker version
docker compose version
uv --version
node --version
npm --version
```

前两条必须同时显示可用的 client 和 server/Compose。仅安装 `docker` CLI 而没有可达 daemon 不足以运行本项目。

## 真实本地 fixture E2E（不使用 Compose）

synthetic browser E2E 可在浏览器层替换 API 响应，以覆盖页面契约；真实本地 fixture E2E
不替换网络响应，启动 loopback Engine 与 Vinext 同源 BFF，并将状态写入调用者提供的专用空
PostgreSQL 数据库。它只验证本地软件链路，不读取或构造真实行情、账户、订单或 Provider 证据。

调用者必须先提供一个专用、空的 PostgreSQL 数据库，名称以 `nexus_e2e_` 开头。例如，使用
loopback 地址的 URL（占位符必须替换为仅用于该测试数据库的 synthetic 凭据）：

```bash
export NEXUS_REAL_E2E_DATABASE_URL='postgresql+asyncpg://<synthetic-user>:<synthetic-password>@127.0.0.1:<port>/nexus_e2e_local'
export NEXUS_E2E_BROWSER_CHANNEL=chromium
npm run test:e2e:real
```

运行器只接受无附加参数的固定命令；项目根目录存在 `.env*` 或 `.dev.vars*` 时会在连接数据库前
失败。它会先确认目标库没有非系统 schema 或用户对象，并在迁移、浏览器旅程和结果验证期间
持有 PostgreSQL advisory lock。运行器不会创建、删除、清空或重置数据库；若目标库非空、已被
另一门禁占用或最终结果不是恰好一个固定旅程通过，测试会失败而不会清理。
默认不运行 Provider、交易 Gateway、任何 worker 或 MinIO；不要把本命令视为复星 SDK admission、
复星激活、真实行情/财报、账户同步或订单可用性的证据。

## 本地环境文件

```bash
cp infra/env.example infra/.env.local
```

`infra/.env.local` 被 `.gitignore` 覆盖，仍只能填本地 fixture 值，不得填真实 Provider 密钥、账户资料或生产证书。必填域：

- 基础设施：`POSTGRES_*`、`MINIO_ROOT_*`；
- Engine：`NEXUS_ENV=development`、`DATABASE_URL`、全部 `OBJECT_STORE_*`、Engine HMAC current key ID/secret；
- Agent：`AGENT_RUNTIME_MODE=fixture`；默认不需要任何 `OPENAI_*`；
- 行情/财报：`NEXUS_PROVIDER_RUNTIME_MODE=fixture`；普通 `app` profile 不接收
  复星/Alpaca/FRED 密钥、任一网关客户端身份或 `NEXUS_INTRADAY_*` 配置，也不会发起外部数据请求；
- 端口：默认 5432/9000/9001/8000，被占用时用 `NEXUS_*_HOST_PORT` 改为其他本地端口。

`DATABASE_URL` 与 `OBJECT_STORE_ENDPOINT` 供容器使用，因此 host 分别应指向 Compose service 名 `postgres` 与 `minio`，而不是 `127.0.0.1`。

Strategy R&D 独立 worker 支持显式 `fixture`、`responses`，以及必须带 `--once` 的
`codex_sdk`。Codex 长驻模式在领取队列前拒绝；不会回退为 fixture。先通过现有本地配置入口
登记专用提示词和模型版本，不重标通用研究 v1/v2：

```bash
uv run --package nexus-engine nexus-openai-local setup --backend codex_sdk --strategy-rnd --skip-key
```

结构化研究综合 v3 使用单独的显式登记入口：

```bash
uv run --package nexus-engine nexus-openai-local setup --backend codex_sdk --structured-synthesis --skip-key
```

此入口同时不可变登记 `local-portfolio-synthesis-v2` 最终角色提示；旧 `local-openai-v1` 内容和已冻结研究不变。现有准入按角色选择最新提示，因此登记后未来非三阶段研究也会选择新版 portfolio 提示；提示内仅在 `stage=synthesis` 综合跨来源证据，其他调用仍承担原组合上下文职责。回读旧研究仍按原 manifest，不追溯换提示。来源索引有效仅代表引用绑定成功，不代表论点内容已经独立核验。

此入口登记 `codex-sdk-chatgpt-v3`，不创建研究或 Job、不调用模型；与 `--staged`、
`--strategy-rnd` 互斥。原 `--staged` 仍为 v2，历史配置与研究不升级。研究准入须显式选择
v3 model config ID，并由 Codex SDK worker 消费；不会自动成为默认研究配置。
三阶段最终结果保存综合正文及逐条反证审查；缺少完整确定性准入时仍为观察、无执行动作、
证据不完整。回退时为新任务显式选择原版本配置，保留已有 v3 历史；无需数据库迁移。

同模型、同套餐的较大累计输入预算使用显式新版本：

```bash
uv run --package nexus-engine nexus-openai-local setup --backend codex_sdk --structured-synthesis --aggregate-input-tokens 96000 --skip-key
```

此命令只登记不可变 `codex-sdk-chatgpt-v3-input96k`，不修改原 v1/v2/v3 的 64k
配置或创建研究。正常 `admit-research` 请求须显式选择这个配置 ID；worker 校验冻结
版本/哈希和 96k 累计输入上限，预检及最终用量按同一配置判断。每次 wire 仍限
55,808 字节，累计输出 8,192、最多三阶段、时限与单次 Job 尝试均不变；研究语义
仍为 partial/watch/none，不授予方案或执行资格。输入增加后仍须先检查完整请求大小。
停止选择新版本即可停止新增该预算研究；保留新配置及历史记录，无迁移或新增依赖。

完整正文加季度投影超过旧单请求字节边界时，使用单独变体：

```bash
uv run --package nexus-engine nexus-openai-local setup --backend codex_sdk --structured-synthesis --aggregate-input-tokens 96000 --max-wire-bytes 64000 --skip-key
```

此入口登记 `codex-sdk-chatgpt-v3-input96k-wire64k`，配置 hash 包含 64,000 字节边界。
worker 按冻结版本将边界传到 staged 与实际 SDK invocation；旧 input96k 仍为 55,808
字节，旧配置不变。单请求 token **预留**仍不超过 64,000，实测按累计 96,000 输入/
8,192 输出检查并保留全部收到用量；不宣称单次实测消耗能硬截断。其余研究语义、
三阶段、时限、失败不重试和执行限制均不变。预检须完整保留原件和投影，不截断正文。

需要给研究工具内容附上直接引用序号时，在上述 setup 参数中添加
`--inline-citations`，登记独立版本 `codex-sdk-chatgpt-v3-input96k-wire64k-cite1`。
只在 SDK structured、96k 输入和 64k 字节组合下支持；此选择不增加额度或调用次数。
新配置 hash 包含 `inline_citations=true`，worker 仅为该冻结版本启用引用输入视图。
省略参数仍登记原版本，不修改旧配置或旧研究。停止选择新版本即可回退后续调用，
保留已产生的不可变记录；没有迁移、新依赖或执行资格变化。验收需同时覆盖正常
setup→worker 的新版本接线和旧版本输入重放，不能仅验证视图辅助函数。

研究输入的本地计量使用锁定的 `tiktoken==0.14.0` 和已验哈希的官方公开词表。
依赖通过既有 `uv sync --locked` 流程同步后，在仓库根目录显式准备一次：

```bash
uv run --package nexus-engine nexus-openai-local prepare-tokenizer --backend codex_sdk
```

该动作只准备约3.6 MB的公开词表到 `~/.cache/nexus/tokenizers/o200k_base.tiktoken`，
不读取账户配置/凭据，不访问数据库或调用模型。已有有效文件不联网；缺失时仅一次固定来源
的限时、有大小上限请求，拒绝重定向和错误哈希后原子保存。它不是 `setup --skip-key`
的一部分；不要用“只做setup”暗中触发下载。

研究过程中不会下载词表。固定模型/包版本或缓存不满足条件时，保留字节预检；不能以
缺少缓存为由重试研究。文本token计数不等于整个SDK请求的精确计数：预检还计入实际
严格输出schema、显式系统/开发者指令的字节量和原8192运行器余量。原wire字节上限、
原版本累计实际64000/8192阈值（显式 input96k 版本仅累计输入为96000）、三次调用和
超时仍保持；预检通过不保证研究结论合格。
回退代码与依赖锁即可恢复旧预检，无DB迁移；公共缓存可保留，已有研究及证据不改写。

`--strategy-rnd` 入口只幂等登记配置，不创建研究或 Job、不调用模型；与 `--staged` 互斥。
已显式注入本地数据库等运行配置的进程可使用
`AGENT_RUNTIME_MODE=codex_sdk uv run --package nexus-engine nexus-engine-strategy-rnd-worker --once`
处理最多一个已有策略研发 Job；worker 本身不读取 `infra/.env.local`，不要把数据库凭据复制到
命令或日志。调用前须完成 `0036_strategy_rnd_usage_unknown` 迁移与合格策略/价格 authority 准备。
普通 setup 和单次 worker 都不补造这些输入，也不自动排队、晋级或交易。

模型最多一次无工具调用，30 秒局部截止并给 45 秒 worker 总截止至少留出 5 秒结算余量。
64,000/8,192 token 是本地验收阈值，不是 SDK 服务端消耗上限；已知用量原样保存，未知三项计数
和订阅美元结算费用保持 null。进程异常后的重领在第二次模型调用前终止，不自动重放未知结果。
0036 不改旧行；存在未知或不兼容旧约束的用量事实时拒绝降级，不填零、不删历史。
合成接线通过仍不等于真实家族完成；当前调整价格、长期 OOS、人工选择与执行锁门禁不变。

## 启动

先校验解析后的 Compose，不打印完整配置：

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app config --quiet
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile broker config --quiet
```

第二条只做静态组合校验，不启动 broker 服务。然后按依赖顺序启动 app：

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app up -d postgres minio
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app run --rm object-store-init
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app run --rm --no-deps api alembic -c engine/alembic.ini upgrade head
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app up -d api worker
```

`object-store-init` 只服务普通 `app` profile 并操作其配置的单一 bucket；development/test
缺失时创建，随后启用并复核对象版本控制，production 则要求 bucket 与版本控制均已预置。
重复执行应返回 `existing`。Provider profile 不单独运行这个初始化器；普通 Mac 本地模式
复用 app bucket，但仍固定写入 `provider/market` 前缀。只有启用可选部署加固时，才需在
单独的权限变更流程中预置 Provider bucket/principal/policy。

## 已取得的发行商分红原件导入

`nexus.ops.issuer_distribution_local` 用于同一证券的当前研究事实，复用本机数据库与
对象存储，不发起下载、模型调用或券商请求。输入为 SSGA 官方历史分红 XLSX、官方基金
身份 HTML 及各自收件记录（`url`、`sha256`、实际 `received_at`）。从受保护本地上下文
传入证券 ID；不要把账户身份或完整命令参数写到日志。目标输出目录须为 `0700`，输出文件
必须尚不存在。入口接受 `--repo-root`、`--instrument-id`、`--workbook`、
`--workbook-receipt`、`--identity`、`--identity-receipt`、`--output`。

它复核既有 SEC 497K 份额证据与发行商 ticker/CUSIP，保存并验读原件的版本 URI，选择截至
实际收件日已到支付日的最新分红。除息、登记、支付日分别保留；金额为每份 USD，空资本利得
保持未知。新建研究快照经现有 `get_security_snapshot` 提供 `issuer_distribution`；
旧冻结输入不变。原件缺少历史公告时间与事件生命周期状态，因此明确
`historical_backtest_eligible=false`，不生成可执行公司行动、复权价格或费用适用性。

前置迁移 `0035_issuer_research_evidence` 只为既有证据所有权函数增加精确来源分支，
不新增表，不回填旧数据，也不放宽价格或执行条件。应用前保存受保护的一致性数据库备份。
没有冻结发行商证据时可降至 `0034_capability_versions`，测试验证函数原样恢复；一旦已冻结
则禁止删除该支持分支，应保留迁移并回退应用或前向修复，避免破坏历史引用。


同一入口增加 `--fund-observation` 后读取官方当前持仓 XLSX 和基金行业分配 HTML，
其余参数、SEC 份额身份复核、原件归档与冻结验读沿用。前置迁移为
`0038_issuer_fund_observations`；不要在旧数据库或不兼容消费者上执行此模式。
`get_security_snapshot` 通过精确冻结引用返回 `issuer_fund_observation`，不新增正式
行业风险记录。保留 industry 原标签、百分数 Decimal、零与负权重、缺分类、资料日期
及两份实际收件时间；发布时间、分母、有效期和差额分类保持未知，不把差额转成现金。
解析器只识别现有单sheet及明确A:I合并页尾结构，数据行缺权重或显示百分比与原值
不一致时拒绝；版式变化先核对原件，不能静默丢行。内容相同的原件可复用首次收件的
DataArtifact，新观察仍记录本次收件；旧归档不覆盖。0038仅扩展精确所有权分支，
无新表；没有新类型冻结引用时可降回0037，存在引用时拒绝降级并保留历史支持。


同一入口的 `--nav-history` 与 `--fund-observation` 互斥，用于官方 NAV History XLSX
及身份 HTML。新前置迁移 `0039_issuer_nav_history` 只追加两份原件的精确归属分支，
无新增表或历史数据回填；未冻结 NAV 时可降回0038，已有冻结引用则拒绝删除支持。
观察值为每份净值，USD依据发行商页面的 Base Currency，不声称工作簿直接标有币种。
保存完整原始观察（包括非交易日）、原份额数/净资产及空值、两原件hash和实际receipt；
`get_security_snapshot.issuer_nav_history` 返回最近32条并附总数、完整覆盖期和截断标记。
历史发布时间、修订/拆分调整与再投资方法未知，不能把资料日当当时已知，不能将NAV
写入市场PriceBar或视为每日总收益。旧SEC/分红/行业持仓冻结资料继续独立读取。

排障记录：官方目录接口省略页面实际使用的 `ui=fund-finder` 会返回 400；另一分红接口
曾返回 HTTP 200 HTML，不能按 JSON 成功处理，也不自动重试。基金页面“Sector”可能是
细分行业分类，未核实统一口径时不得灌入 11 行业指标。SEC 孤立快照、内容 hash 不一致、
原件 URI 或 schema 冲突均已由导入入口拒绝并有隔离 PG 回归，不能只看存在性或文件长度。
代码侧 `CURRENT_MIGRATION_HEAD` 须同步迁移图；就绪单测现直接比对两者，防止升级后误报
schema 不兼容。实际就绪检查仍分别判断数据库、迁移、对象存储与 worker。

## 可选的真实美股只读数据

真实美股行情、SEC 财报与 FRED/ALFRED 宏观数据属于独立的 Provider
profile，不属于默认本地启动流程。首选市场源固定为复星：
`NEXUS_MARKET_PROVIDER=fosun`、`NEXUS_ALPACA_MARKET_MODE=disabled`。首次配置或轮换时，在
受保护密钥域注入 market-only 的复星网关 mTLS/HMAC 身份、`FRED_API_KEY` 与带联系方式的
`SEC_CONTACT`；网关自身是唯一接收 `FSOPENAPI_*` 的进程。不要把任何值写入仓库、
命令历史或日志。Alpaca 只作为已核验的备选/交叉验证源；没有完成其 entitlement、
费用、留存和数据合同核验时，不得改为 `NEXUS_MARKET_PROVIDER=alpaca` 或启用
`NEXUS_ALPACA_MARKET_MODE=enabled`。

Compose 中 `provider` 是固定的复星密钥域，`provider-alpaca` 是固定的
Alpaca 密钥域；两者不得同时启动。切源时必须先停止旧 Provider Worker，
确认旧 lease 已释放，再让通用 worker 与新 Provider Worker 使用同一个
`NEXUS_MARKET_PROVIDER`。当前队列没有 source-bound claim，因此本拆分不声称支持并发
双源交叉验证；该能力需要未来独立的 Job/queue 协议。

普通 Mac 本地启动默认复用已有 `DATABASE_URL` 和 `OBJECT_STORE_*`，Compose 会将它们
映射到 Provider 进程需要的变量名。Provider 仍使用独立进程、队列、供应商凭据域和
`provider/market` 前缀，不因共享本地存储身份而获得 Broker/Agent 能力。

`infra/postgres/provider-runtime-grants.sql` 和 `PROVIDER_*` 覆盖项是可选部署加固：需要多用户隔离时
再创建最小权限数据库/MinIO 身份并运行对应审计。这些不是个人本地读链路的启动前置。

生产数据模式仍只保留一个由通用 worker 管理的 `daily.pipeline` 根。激活时，通用
worker 使用原 `DATABASE_URL` 并显式设置 `NEXUS_PROVIDER_JOBS_ENABLED=true`，只负责将
两个 Provider 子任务写入 `provider` 队列；Provider-only worker 优先使用
`PROVIDER_DATABASE_URL`，未设置时复用本地 `DATABASE_URL` 领取这些子任务。不能停止通用
worker，也不能让 Provider worker 创建第二个日任务根。以下是显式启用形态，不是本地默认命令：

```bash
NEXUS_MARKET_PROVIDER=fosun NEXUS_PROVIDER_JOBS_ENABLED=true \
  docker compose --env-file infra/.env.local \
  -f infra/compose.yaml --profile app --profile provider up -d worker fosun-gateway provider-worker
```

Alpaca 替代形态不启动复星 Gateway，且只能在先停止
`provider-worker` 后使用：

```bash
NEXUS_MARKET_PROVIDER=alpaca NEXUS_PROVIDER_JOBS_ENABLED=true \
  docker compose --env-file infra/.env.local \
  -f infra/compose.yaml --profile app --profile provider-alpaca up -d \
  worker provider-worker-alpaca
```

`provider-worker` 强制 `NEXUS_PROVIDER_RUNTIME_MODE=production`。在默认配置中，美股日线和
盘中证据只通过复星 market-only 网关身份读取；SEC company facts/submissions 与
FRED/ALFRED 的 8 个白名单序列继续使用各自固定 HTTPS origin。Provider Worker 不接收
broker 网关身份、账户 ID 加密密钥或交易写能力，也不会构造或读取 Alpaca client；复星
Gateway 同时属于显式 `provider`/`broker` profile，但由不同的 HMAC principal 派生固定操作
权限。生产 Gateway 使用外部命名卷 `nexus-fosun-gateway-replay-v1` 保存 nonce journal
并持有 singleton lease；不得在普通 `down -v` 或清理脚本中删除该卷。启用可选部署加固时，
Provider 数据库角色只能读写
`provider` 队列及只读查看其 `daily.pipeline` 父任务；在 market 数据域，它仅能 INSERT
批准的 evidence、manifest、bar/quote、authority、filing/fact、corporate-action 与宏观事实表，
不能 UPDATE/DELETE authority 输入或直接写 publication mark。进程启动时会复核精确权限、
强制 RLS、策略实际谓词和受控函数定义。每日 Job 只读取调度时冻结并绑定哈希的
美股 universe（上限 100 个）；不存在、哈希不匹配、数据冲突或证据不完整时均跳过，
不会临时扩展股票池。

分钟级 Snapshot 是这个独立 profile 中另一项默认关闭的能力。跟踪模板固定为
`NEXUS_INTRADAY_MONITOR_MODE=disabled`、15 秒 bucket 和复星最多 10 个标的。页面把复星
delivery `unknown` 明确显示为“实时性未获证明”，缺失 bid/ask 时显示“盘口未提供”，不会
合成价差或交易信号。已有受保护配置和只读权限下可按正常本地入口启用；首次接入新账户、
新增凭据、扩大权限或付费升级时，分别按[复星只读激活手册](fosun-read-only-activation.md)和
[分钟级盘中市场监控激活手册](intraday-market-monitor.md)完成接入核对。Alpaca 不会被自动
fallback 触发。

## 验证

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app ps -a
curl --fail --silent http://127.0.0.1:8000/health/live
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app run --rm object-store-init
```

如果在 `infra/.env.local` 改写了 `NEXUS_API_HOST_PORT`，上面 `curl` 也应使用同一个显式端口；`--env-file` 不会把变量注入当前 shell。

`/health/live` 只证明 API 进程存活。真正的 `/health/ready` 受 HMAC + nonce 保护，同时检查 PostgreSQL、唯一 Alembic head、bucket 和 30 秒内的通用 worker heartbeat。通过同源 Web BFF 的 `/api/v1/readiness` 检查，不要在命令行手工粘贴签名 secret。当前组件状态也可在本地“风险与审计”页面查看。

如果 readiness 不是 `ready`，按 `database -> migration -> object_store -> worker` 的顺序查询脱敏组件状态；不要通过关闭检查或伪造 heartbeat 绕过。

## 单次 Shadow 价格查询（不启动或推进 Shadow）

入口只为已有 Shadow 根采集一个已结束纽约自然日的 Alpaca 原始日价格，证券来自冻结方案。
使用已就绪的本地 PostgreSQL/对象存储和现有本地配置入口中的
`ALPACA_API_KEY_ID`、`ALPACA_API_SECRET_KEY`，不把密钥放入参数、聊天或仓库。
不需要切换 `NEXUS_MARKET_PROVIDER`、启动 Provider Worker 或打开调度。接口未取得访问时，
只运行隔离 mock 测试；此命令没有向业务库写入 mock 数据的开关。

```sh
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.shadow_price_query \
  --repo-root . \
  --shadow-group-id '<已有 Shadow 根 ID>' \
  --session-date '<已结束且属于该根的交易日 YYYY-MM-DD>' \
  --idempotency-key '<本次明确查询的唯一键>' \
  --feed sip \
  --delivery-mode delayed
```

feed/delivery-mode 应与实际来源配置一致，不因传入参数就视为已验证实时权限。
命令保存精确原件、逐页请求凭证、归一化价格及返回集合证明；空响应只表示该窗口返回零条。
成功仍为 `price_use_authorized=false`、`execution_locked=true`，不代表价格用途已获准，
不会调用 Shadow start/advance、模型或交易接口。历史实际可用时间不回填。

成功同 key 重放先重验既有证明，零外部请求；同 key 已收包但没有完整证明时拒绝重新采集。
错误退出使用固定 `SHADOW_PRICE_RESULT_UNKNOWN`，可能已经保存部分原件/凭证或提交完成，
不能据此声称已回滚或自动换 key 重试。先检查该 key 的已存记录和实际命令状态。
停用只需停止调用模块；不删除或改写原件与既有证据。历史 v1 读取不变，本片无迁移。

## FRED 接入与 Python wrapper 选择

本地宏观原件采集复用已有 `httpx` 固定 origin GET 边界，不新增通用 SDK 兼容层。
[FRED 官方契约](https://fred.stlouisfed.org/docs/api/fred/overview.html)是 REST/XML/JSON；
社区库不是 Fed 维护的官方 SDK。本轮核对的
[`fredapi`](https://github.com/mortada/fredapi/blob/master/fredapi/fred.py)将观测转成
float/NaN，公开 metadata 接口不接受 vintage，也不返回逐页原始 bytes；
[`FRB`](https://github.com/avelkoski/FRB/blob/master/fred/helpers/__init__.py)当前源码默认
磁盘缓存和等待重试，底层未设置 timeout；
[`fred`](https://github.com/zachwill/fred/blob/master/fred/core.py)也没有有界 timeout/响应尺寸。
因此暂不新增这些依赖，避免为满足原件、实际收件时间与首错停止而覆写其传输内部。
新采集只使用官方 [series metadata](https://fred.stlouisfed.org/docs/api/fred/series.html)
及 [observations](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)两端点，
显式指定 vintage 和小观察窗，原件先私有保存；`units=lin` 是变换码，实际单位取 metadata。
分页按 `count/offset` 核验，不把短页或 SDK 成功视为完整证据；历史 vintage 不回填今天才收件的可用时间。

单次采集与导入分别运行，目录必须是尚不存在的私有路径（父目录应已存在）：

```bash
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.fred_local --repo-root "$PWD" collect \
  --series DGS10 --start 2026-08-24 --end 2026-08-28 --vintage 2026-08-28 \
  --output-dir /absolute/private/new-fred-capture
PYTHONPATH=engine/src .venv/bin/python -m nexus.ops.fred_local --repo-root "$PWD" import \
  --collection-dir /absolute/private/new-fred-capture
```

`collect` 只需已配置的 `FRED_API_KEY`，固定官方 origin、单请求不重试、全轮 60 秒，
最多 metadata 加 4 页；每个已收原件及不含密钥的请求 receipt 均先保存，首次错误立即停止。
失败目录不复用，不能据此自动重采。`import` 不外呼 FRED，要求显式本地数据库和五项
对象存储配置，重核原件/分页/metadata/calendar 后才在一个事务内创建日期独立的宏观事实与引用。
正常进程环境变量覆盖本地配置文件，不改变调用进程的环境。
相同归档导入幂等；本地内容修订不改旧行。`.` 保留缺值而不生成零值；超出数据库精度的值拒绝导入，
不静默四舍五入。当前研究可见时间还受各层实际创建时点约束，不能按旧收件时间读到后来才落库的事实。
命令只返回安全状态、数量及 ID；不运行模型、创建订单或改旧研究快照。`FRED_RESULT_UNKNOWN`
不代表已回滚，原件或数据库提交可能已存在，应先核对原目录及命令状态，不自动重试。

## 更新所属本地 API

确认实际 Compose project、源码提交和服务归属后，先为当前可寻址的镜像保留独立回滚
tag，再构建新版本；不要先覆盖 `latest` 再尝试用运行容器的旧 image ID 打标签。
Docker 可能已不再保留该 ID 的可寻址镜像，即使旧容器仍在运行。若发生此情况，先从
已核验的旧提交重建回滚镜像并验证源码，再切换服务，不能把更早版本冒充本次回滚点。

构建上下文使用目标提交的 `git archive`，避免将被 Git 忽略的私人验收材料送入镜像。
仅更新所属 API，沿用其配置与端口，使用 `--no-deps --no-build` 限定切换范围；核对运行
镜像的实际 image ID、容器内源码、配置未变，以及 PG/MinIO 容器 ID 和启动时间未变。
这些比较结果应在等待存活/就绪前先落受保护记录；若需分次核验，先保存不含明文配置的
前后摘要，避免观察窗口超时退出后丢失比较依据。短等待超时先核对启动日志和实际进程，
不要据此再次重启。
先检查 `/health/live` 再检查 BFF readiness；启动期间一次连接错误不应触发业务请求重放。
本地只读模式未启动通用 worker 时，应逐项报告 readiness，不能把 worker unavailable
误报为数据库、对象存储或页面读取故障。回滚使用保留的旧镜像和同一配置，仅重建该 API。

## 停止与精确清理

```bash
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app stop api worker
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app down
```

`down` 默认保留 volume。只有确认该 Compose project 中的 fixture 数据可删除时，才可针对该精确 project 另行执行 volume 清理；不得清理其他 NEXUS 容器、volume 或工作树。

## 常见失败

- 端口冲突：只改 `NEXUS_*_HOST_PORT`，容器内 service URL 不变。
- init 失败：检查六个必填值和 MinIO health；CLI 故意不打印 Provider 错误详情。
- migration `incompatible`：在当前代码镜像重跑 `upgrade head`，不手工改 `alembic_version`。
- worker `stale`：确认 worker 正在运行且使用同一数据库/迁移 head；不直接写 heartbeat 表。
- Provider Job `FROZEN_UNIVERSE_*`：先发布完整 universe，再等待下一次调度；不要手工改 Job payload。
- Provider evidence `*_CONFLICT`：保留现有版本与对象，停止自动重试并核对上游修订；不要覆盖同摘要对象。

### 缺少美股身份来源

本地刷新返回 `LOCAL_BROKER_INSTRUMENT_SOURCE_REQUIRED`，表示新出现的美股代码尚无
可复用身份，不是缺配置或授权。`us` 前缀只表示市场域，不能生成具体上市所、
证券类型和最小价格单位。先使用有真实来源的现有 `instrument_identity_local` 建档
入口（见 baseline 运行手册）；该入口当前只覆盖固定定义的普通股，其他类型或
定义外标的保持明确缺口，不伪造身份。已有合法身份正常复用；已有字段与来源冲突
时仍拒绝，不覆盖旧行或为了完成刷新更换UUID。订单中首次出现的新美股也适用。
