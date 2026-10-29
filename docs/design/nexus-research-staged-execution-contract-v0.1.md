# 单股研究分阶段执行合同 v0.1

最新正式迁入见§二十四：同一核心、统一证明、0099迁移及版本路由模块已落地并通过16项回归。既定安全等价门槛保持关闭；下一步统一接正常生产者、Worker/SDK和公共消费者，不再扩建原型。当前尚未启用生产路由、部署或业务宿主。

## 当前续办入口：统一正常、失败与恢复（2026-10-28）

本轮先关闭终审已复现的授权提交未知、孤儿错误结算和原异常丢失；三个原断言保持。授权提交未知走研究专用边界，零发送、原claim与预留保持，不进入旧通用失败处理。新增不可变核验事实，绑定不可变授权与观察的同一Run/阶段；核验使用SDK原wire摘要、原响应和实测用量，不能从待核验的数据库观察反推可信性。缺少核验、明确不匹配、非合格观察或未知tokens均不能结算。

正常终结、孤儿终结和下一阶段准入使用同一持久化投影与known规则；阶段内容合格、用量已知、SDK已退出仍是三个不同结论。终结证据采用带版本的canonical JSON摘要，并冻结观察、核验与清理的精确成员；同刻或之后迟到事实不能加入已闭合证明。原异常/取消优先于停止记录失败；先置本地停止，持久化失败只加固定安全说明，不能重新分类或继续派发。

最终发布须在同一scope锁下重新读取持久化和本地停止：停止先提交时，不发布可消费最终Output，即使三阶段用量可信且cleanup齐备；可信费用仍一次结算。最终先提交时，后到停止不追改原结果。研究专用失败边界覆盖整个最终事务，包括COMMIT前的原生验证、账本和证明SQL；不能只保护COMMIT而把较早SQL错误交给不理解阶段证据的旧通用失败处理。失败保留原cause/取消、原claim与已提交阶段事实，只沿原身份进行合格读回或结案，不新增重试。这两项在同一真实Worker/SDK边界以两种已提交顺序和即时SQL拒绝复现2失败/1对照通过，修复后3项通过66.23秒；全套及指定终审以最新检查点为准。

以下是统一交付矩阵，不能以眼前回归通过代替整条生命周期。状态指隔离原型；不代表已部署、真实模型或自然业务证据。

| 生命周期/消费者 | 已有证据与本轮规则 | 尚缺验证或正式接线；保持的拒绝点 |
|---|---|---|
| 正式日研、独立root、单股增量 | 两类root已在隔离原型从原生准备同事务登记版本/owner，接同一真实Worker/SDK代码；增量及独立parent后继原证据保持 | 根选择终审新增发行来源与低层无route两处P1已复现并修复，最新定向结果见§十三；正常增量prepare选择与旧身份隔离已补齐，见§十七；共享派发/取消及真实OS清理已补齐见§十八至二十二，prepare/writer/scan及八工具已补齐见§二十三；正式化前仍禁止启用生产路由 |
| 阶段授权、观察、最终COMMIT未知 | 实际PG提交/回滚确认丢失；新授权未知不发送、不泛化结案；观察须另有可信核验 | 核验自身COMMIT两分支已实测：无确认均停止发送，后续仅按真实已提交核验判断费用；公共消费者已接同一证明，见§十四至十六；不能以无授权/无响应推断零费用 |
| 明确失败、已知前缀、未知后段 | 已知可信前缀可按原账本一次结算；后段未知保留整Run预留；审计否决不消除真实已知费用 | 对最终用途/安装资格仍走原验证，不能把known当合格研究 |
| 取消、批次停止、跨Attempt | 原阶段唯一性、原fence/token与持久化scope stop；原异常保留 | 共享两路/第三股补位、首失败且cleanup未结束及单次调用方取消见§十八；claimed确证未调用的原生原子收尾、公共证明及下一准入已通过§十九；真实OS重复取消与固定cleanup已验见§二十二，不能借队列取消放行running身份 |
| 清理、退出、恢复 | 已知费用可结算但cleanup不明仍占用；宿主维护停用 | 独立及formal公共host只读与锁后恢复已接同一证明，见§十六；真实Popen、重复取消与固定cleanup截止已验见§二十二，不代表真实模型或业务宿主验收 |
| 迟到、孤儿、历史封存 | 正式root的公共恢复与阶段结案同事务；原CLI明确接受、API读回与审计绑定冻结阶段摘要，保留原公共资格检查，见§十四 | 独立v3正常成功、known/unknown/cleanup不明及宿主锁后消费者已验证，见§十六；真实OS证据见§二十二，v3未知封存当前入口不支持，unknown普通准入继续拒绝 |
| 预算与下一轮准入 | 原三类共享预算保持；公共监控收尾/准入核同一终态证明，known且cleanup确认的孤儿可收尾后准入；正式root原显式封存后新请求路径已接隔离实验 | unknown或cleanup未确认继续阻断；独立root既有收尾/正常准入及host已验证，真实未知未接受或释放；派发/进程矩阵见§十八至二十二，prepare/writer/scan已闭合见§二十三 |
| 原期限、工具、prepare/writer/五分钟scan | 原期限/真实DB等待/同物理连接池见§二十；模型及清理等待原生scan与已调用工具短事务证据复用 | 正常prepare及合法writer提交/回滚、scan合格行情与同桶去重、八工具正向完整返回等价已实测，见§二十三 |
| 最终输出、计划接替、旧数据 | 原native最终验证、审计否决、price/disclosure前驱CAS及未知保护保留 | 正式实现后逐入口验证持久化/API/页面；不能用隔离替身、旧CI或健康代替产品链 |

上述安全等价门槛与指定终审现已收口，下一切片直接推进研究专用最小正式实现、迁移与历史读取兼容、同head必需CI、兼容回滚及既有环境部署。部署不包含静默恢复人工停用的业务调度。维护模式、旧请求/通知不重放、未知费用/预算保护、正式风控与execution_locked均保持。

状态：2026-10-29 既定正式实现前隔离安全等价门槛已收口；未启用生产路径。依据整体评审 `nexus-whole-design-review-20261028-01`。本合同细化已批准的职责，不改变用途、交易许可、预算、期限、重试或未知接受政策。

## 转正式实现的剩余关键路径（2026-10-28，按实际消费者收紧）

只按下面的结束条件推进，不把阶段认可或累计测试数量当交付。已有代码、环境和合同未变化的证据直接复用；新事实仅在复现缺陷或明确未覆盖风险时加入原矩阵。当前公共组集中验证清理时点、重复封存读回、损坏拒绝、恢复与迟到写入的两事务顺序及外层COMMIT不明，不再重审已关闭的root选择与迟到cleanup设计。

| 类别/顺序 | 失败风险与实际调用点 | 可复用证据 | 结束条件 |
|---|---|---|---|
| 正式实现前：公共消费者（本轮回归与定向终审完成，见§十六） | 独立`require_research_jobs_settled`与formal `_require_cycle_jobs_settled`均先保留原native资格，再共用精确owner/route与阶段证明；实际host只读及lifetime lock后guard据此判断。历史terminal finish仅幂等读回，不授予当前准入 | 当前集中37项359.46秒；正常三股成功、known/unknown/cleanup未确认、原消费者已关闭却缺cleanup、合法封存unknown的same-day读回均通过；原root/core/Worker证据复用 | 本组实现、37项回归与formal增量定向终审已完成，无新增P1/P2。unknown普通准入仍拒绝，只有原formal明确接受合同保留原效力；不新增v3接受政策，不重复本组无变化验证 |
| 正式实现前：统一派发与取消 | 原queued取消政策保持；claimed零调用须正向证明；scope停止、原未知预留不变 | 正常增量选版见§十七；共享两路/第三股补位、首失败清理占槽见§十八；claimed原子结案与公共证明见§十九 | 本组已按各节收口，复用最终证据；不重复扩建，真实OS重复取消见下一行已完成证据 |
| 正式实现前：期限与进程 | 原固定anchor、SDK阶段/父截止、lease/pricing及未知预算不变；同物理连接LOCAL不能泄漏 | 原期限DB等待、COMMIT及session退出迟确认、真实SDK截止和同物理池四例已通过§二十；成功/失败公共结案时间精确一致 | 期限与物理池切片已收口；真实OS重复取消、原cleanup截止及退出未确认占用保护也已由§二十二最终24/23项与指定终审收口条件验证，本组完成，复用证据 |
| 正式实现前：prepare/writer/scan联合 | 外部等待若仍持事务，会重现原2秒锁失败；解除读锁不得允许冻结证据变化或非法新版本发布 | 原生产Worker+scan RED保持；冻结SQL不可变/合法状态切换反例；模型/cleanup等待时原生scan与短工具证据 | 已由§二十三最终7项及指定终审收口；原生价格校验、合法writer提交/回滚、冻结及写入排序保持，正常prepare/scan并发和八工具等价已验证。复用，不延长timeout或改共享锁 |
| 正式实现后、部署前 | 正式DDL/版本路由、历史读取/回滚、真实API/BFF页面与当前head集成若不验证，隔离原型可能不能落地 | 原型原生prepare/claim/Worker工具和公共证明断言可作为同一实现的验收测试；已冻结案例可复用 | 将同一研究专用机制移入正式模块与迁移，去掉monkeypatch，旧协议不升级/不重放；迁移前后读取/回滚可行，正常API/实际页面、相关回归与同head必需CI通过；最终镜像身份及健康验收。部署仍不授权开启maintenance业务宿主 |
| 不影响本次交付的后续扩展 | 更细性能调优/额外诊断；独立v3新增人工接受未知政策不是当前已有入口 | 实际`manual_paper seal-failed-research`只接cycle，`history_seal`要求真实AgentPaperCycle及agent-paper身份；v3现有unknown必须保持阻断 | 不把不存在的v3封存入口误写成“只缺测试”，也不为实验新增接受权限。若未来明确要求此业务能力，另界定政策与授权；本次只证明现有v3未知拒绝及已知正常收尾/准入 |

迁入复用边界：`staged_protocol`的授权、不可变观察/核验、停止及同事务结案机制，`staged_public`的冻结财务/当前清理证明及原公共验证组合，`staged_routes`的准备时版本/owner选择，`staged_worker`中真实Worker/SDK短事务适配，是同一研究专用实现的原型，不再另建通用执行系统。正式化要把临时DDL/测试注入改为受审迁移和显式版本路由，并保留这些真实路径回归；不能把测试脚本作为产品入口，也不能先启用后补消费者。所有“正式实现前”结束条件满足、集中终审有效风险关闭后，即推进已授权正式实现和交付，不因纯整理或可选优化继续延长原型。

## 目标与实施前提

同一原生研究身份完成 research→critique→synthesis；外部传输与SDK清理不持有业务数据库事务。五分钟正常准备和扫描可以独立推进。每个已观察阶段可读回，但不能提前被监控或正式决策消费。

不改通用短事务handler，不新增通用工作流，不重放历史请求，不跨进程自动续跑。正式日研、独立研究与单股后继共用执行机制，分别保持原用途资格。输入上限、全批/全阶段容量预检、共同cutoff、绝对截止、最大尝试数、两路并发和首失败停派发均保持。

**“T可行性实验完成”不等于“T安全等价通过”。** 当前实验已否定直接移动begin/提交角色后沿用旧恢复的方案；正式持久化协议及配套恢复消费者尚未接入；测试数据库原型进展见第十一节。不能以实验测试全绿来开启新执行路径。

## 一、现有责任与必须调整的位置

下列源码路径相对`engine/src/nexus/`。

| 边界 | 当前权威入口/事实 | 新合同中的责任 |
|---|---|---|
| 准备 | `paper/research_request.py:184`、`agents/persistence.py:784/1335` | 用途、精确Run/Job、冻结身份、全批预算同事务提交；不可出现可领取Job却无预留的间隙 |
| 认领 | `jobs/repository.py:332/579`、`jobs/worker.py:145` | 仍只认领一次，原token/fence/attempt/worker；原历史Job不升级到新协议 |
| 原执行 | `jobs/worker.py:238`、`jobs/repository.py:735`、`agents/execution.py:92` | 新研究专用执行器替代长handler，不全局修改通用handler合同 |
| 外部阶段 | `agents/codex_staged.py:1185`及SDK入口 | 实际输入构建/校验与调用之间增加可等待的持久化派发边界；当前同步dispatch_guard不足 |
| 工具 | `agents/database_tools.py:153/823`、`worker.py:954` | 不将AsyncSession藏入跨模型回调；精确冻结值在短事务中完整读取并校验，事务外只序列化/计量；必要新读取仍为精确冻结身份的短读取 |
| 未调用取消 | `paper/manual_cancel.py:48` | 原queued、attempt=0、无lease/output保护保持；已running任务当前被跳过。不能把已认领或阶段间隙当未调用 |
| 过期处理 | `paper/research_orphan.py:24/111` | 旧协议继续要求无events/outputs；新协议需精确阶段链的专用结案，不删旧guard |
| 通用reclaim | `jobs/repository.py:46/838` | 当前正式研究被排除；仍不得重领已派发研究。generic测试不代替正式孤儿证明 |
| 历史接受 | `paper/history_seal.py:293/612/729`、`research_orphan_proof.py:19` | 新协议另有精确可审计资格分支；旧单output/event证明保持；接受仍需具体用户授权，不自动执行 |
| 下一轮准入 | `paper/research_request.py:360`、`manual_request.py:330/361` | 阶段结果可见不等于Run/Job/lease或unknown可释放；精确识别本Run合法占用，其余占用全部保留 |
| 预算 | `agents/persistence.py:356/522/550/1469` | 原整Run预留持续存在；阶段用量只登记，最终一次性结算，不按阶段释放或重建预算 |
| 安装接替 | `monitoring/source_qualification.py`、`monitoring/reconciliation.py` | 只有合格最终Output可消费；审计、两kind CAS、精确事件成员、正式用途限制不变；安装失败不撤销已知费用 |

## 二、阶段与提交合同

每阶段唯一身份固定为协议版本＋原Run＋固定阶段序号；Job/Attempt/worker/lease token/fence是该身份不可替换的绑定，不是第二次派发的重试维度。数据库唯一约束不得因更换Attempt而允许同Run同阶段第二条授权。正常阶段只有research、critique、synthesis，不能从自由文本新增阶段或重试序号。一次调用返回多个角色，角色行不等于独立外部调用。

| 边界 | 事务内必须完成 | 提交后可做什么 | 失败/确认未知 |
|---|---|---|---|
| 领取 | 原生claim及精确预留绑定 | 当前owner准备第一阶段 | 仅原身份读回，不补claim |
| 派发准备 | 锁定原Job/Lease/Attempt/Run和精确预留；核当前owner、期限、停止、用途、输入/前序hash；插入唯一阶段派发授权 | 只有本次首次授权且确认COMMIT成功的原执行分支可发送一次；读到旧授权不恢复发送权 | 未确认COMMIT不发送；只读确认，不因查到授权而补发或生成第二授权 |
| 外部调用 | 不持业务事务；SDK仍遵守原deadline、计数、cleanup | 只沿授权的实际冻结wire/schema发送 | 断连、超时或进程不明保留可能影响；不能从无响应推断零费用 |
| 阶段观察 | 验证原派发身份、响应hash、用量已知/未知、校验/清理事实；保存不可变观察及对应角色输出 | 确认提交且当前owner/原期限仍有效，才允许准备下一阶段 | 不确认提交就不推进；观察已知不等于Run全部已知 |
| 最终提交 | 验证实际已派发阶段链和后续未派发证明、当前原生围栏；成功结果另核最终消费资格/审计；原预算和Run/Job/Attempt终态原子提交 | 既有独立reconcile可核安装资格 | COMMIT确认未知读原身份；不再调用模型 |
| 计划安装 | 原用途、前驱、版本与事件成员；原子CAS及消费 | 合格计划进入正常检查 | 安装拒绝不回滚已知费用，不重新研究旧请求 |

### 最小持久化事实与未实现部分

现有ResearchEvent/Output允许部分合法行提前提交，但没有完整的唯一阶段派发权。不得把任意`details` JSON当作已经存在的数据库围栏。拟仅补研究专用事实，DDL须先离线原型验证：

1. **阶段派发事实**：绑定Run、Job、Attempt、worker、lease token/fence、固定阶段序号、原预留、实际wire/schema/input hash、前序已提交响应hash、协议版本、原绝对截止与登记时点。阶段身份唯一且不可更新/删除；数据库须校验绑定，不能只靠调用方传入正确值。重复进入只能读回，不能返回新的发送许可；初版不增加跨进程或重入时“读到授权即续发”的入口。
2. **阶段观察事实**：绑定唯一派发，原响应摘要/安全诊断、原始已知用量或unknown、调用是否已确证未开始、响应校验、cleanup及进程确认、实际观察时点。响应/用量观察与cleanup/进程确认分别表达，可先提交响应和已知用量，再追加清理结果；允许“响应已知且已提交、cleanup未确认”，不得等cleanup结束后才保存全部事实。事实不可覆盖；相互矛盾的第二观察不能静默替换。一次调用返回5个角色，用量仍只按一个派发身份累计一次；幂等读回不重复记费。观察时点和登记时点分开，不能回填成研究cutoff。阶段成功消费必须引用精确已提交合格观察，清理占用仍按原条件保留。
3. **研究终结证明**：引用完整阶段链和原attempt，区分完整已知、确证未调用与可能未知；可复用既有终态记录，但必须有协议版本与完整关联。旧结果不补字段、不重新解释旧未知。

允许将第2/3项与现有合适表复用，前提是SQL与消费者能完整证明唯一性和绑定。未完成此映射前不决定迁移编号，不编写一套通用事件系统。

仅数据库唯一派发记录不能消除“提交后、真正发送前”的崩溃窗口。该窗口一律保守视为可能外部影响。禁止声称外部exactly-once；本合同保证不因未知而自行再次派发。

## 三、取消、期限、所有权及迟到写入

- **派发授权前**：当前owner可停止后续阶段；只有原生精确未调用证明齐备才可按既有规则结案。无Output/阶段记录本身不是零调用证明。
- **派发授权后**：控制器可阻止后续阶段和消费，但不能据fence变化推导已发调用被撤回、释放预留或授权另一个owner重发。同Run阶段唯一身份永久保留。
- **当前lease**：每次授权及业务写回读DB当前Job/Lease/Attempt，不仅检查内存expires_at。原`JobHandlerContext.require_dispatch_lease`仍可作本地快速拒绝，不能作为唯一准入。
- **固定期限**：沿用原调用域的monotonic work/cleanup绝对边界；数据库记录其原身份和适用墙钟界限。不得从“现在＋完整timeout”重建阶段期限。`checkpoint_job`目前会延长lease，不能直接当只读校验器；研究专用checkpoint不得改变原执行截止。墙钟/单调钟映射与现有心跳上限必须进入原型断言，不能引入更长窗口。
- **进程退出**：SDK清理不持业务事务，但未确认SDK退出仍保留原生命周期占用。宿主健康恢复不授权旧阶段自动续跑。
- **过期/崩溃**：新专用孤儿入口以精确已提交阶段链生成保守终态，继续保留未知费用和整Run预留。已知阶段不得因后续未知被抹掉；未派发下一阶段不代表全Run零费。旧协议继续原单event/output证明。
- **迟到观察**：旧fence不得写当前终态、最终Output、结算或计划。若需要保留可核验的迟到诊断，只能写独立、绑定原派发的观察域；它不改变旧Run/预留，也不自动解除未知。没有合格观察入口时仍保持unknown，不回填旧结果。
- **人工接受**：只改变已授权范围的历史占用，既有费用/预算与审计保留；新阶段协议不会产生默认接受或扩大旧授权。

**原批次首失败与取消的串行关系。** 每次阶段授权同时核对原请求/manifest或cycle的有效状态与持久化停止事实，和停止写入使用同一明确的原dispatch范围串行边界；保留现有本地stop检查。停止先于授权生效：不创建授权、不发送，兄弟下一阶段与第三股补位同样拒绝。授权先于停止生效：保留可能外部影响，仍须尽力阻止已观察停止后尚未发送的调用，不能释放预留、换owner或重发；未来阶段不再授权。不能以“授权点提前”扩大原首失败停派发窗口。普通取消回执不得把已授权在途阶段称作零调用已取消。原型须验证双路兄弟和补位，不仅验证单Run。

取消、孤儿、封存资格、准入、部署停用与阶段执行必须理解同一协议版本后才可启用。不能只更新Worker，让其他owner继续使用旧零记录假设。

## 四、预算与已知用量

原三类预算共享同一业务日总额；增量调用次数另计，不将增量上限解释成日研＋增量总数。每个阶段沿用原整Run预留，不增额、不换日、不再充值。

`reserved→settled/released`为一次性终态，SQL禁止恢复到reserved。阶段一用量已知时只能保存阶段观察，不能调用整Run settlement：否则后续阶段失去有效预留，也会错误腾出余额。所有已授权阶段都具备可信已知用量或精确未调用证明、后续阶段被终结围栏禁止派发且Run终结时，才在原围栏下累计实际用量并一次结算；不要求三个阶段全部成功，也不以研究合格或可安装计划作为已知费用结算条件。第一/第二阶段校验失败、确定性停止且未再派发，同样结算已发生的已知实际用量。任何已授权阶段仍可能unknown，则完整原预留仍计入总额；单凭没有下一阶段记录不足以证明未调用。

观察用量、可计价用量、最终结算分别表达。响应校验失败不自动抹去可信已知用量；cleanup失败不能伪造零调用。已知观察提交不明时只读确认原身份，后续阶段不得先跑。

## 五、价格证据与当前资格必须分开

已经提交的Fosun日线研究冻结链有独立SQL保护：0008禁止manifest/归档原地变更，0011禁止bar/authority/binding变更及authority后的bar增长，0012禁止calendar根/session变更，0029禁止关联Fosun日线manifest发布后的calendar追加，0027禁止capability变更，0005禁止policy变更。原生产/冻结事务仍保留calendar→manifest互斥，防止发布竞态。

因此不能虚构“模型等待时合法修改旧policy/calendar/capability”的反例。正常新增版本不替换旧冻结引用；原件读取仍校验hash，不把数据库不可变等同对象存储绝不损坏。

旧价格复验使用binding.selected_at与provider_authorized_at，证明历史资格。当前IntegrationStatus可正常停用，当前资料采集准入随之拒绝，而历史冻结价格可以保持有效。新阶段必须按原职责分别核历史依据和当前派发资格，不把采集许可新增成历史有效性条件，也不借历史有效跳过当前时效/模型pricing/执行门禁。

T初版不需要先把价格reader改共享：可先在短读取事务里使用原完整validator并结束事务。所有工具的冻结读取同样必须结束，不能漏掉get_price_features。是否仍需共享锁只能由完整正常prepare→scan实验决定，不并行铺A与T。

## 六、三个关键离线实验与首轮证据（10:57，最新见第十一节）

测试环境：自有55439隔离PostgreSQL，各用例独立临时数据库；既有正常准备、Job/Run/预算/SQL guard保留，外部传输采用既有受控夹具。没有真实模型、通知、券商或宿主动作。

| 实验 | 必须验证的安全等价条件 | 当前实际结果与限制 |
|---|---|---|
| E1 外部等待与并行检查 | 原生Worker等待时完整normal prepare→scan落盘；第三连接见派发事实；无跨模型业务事务；原合法writer保护不变 | 原native Worker+scan仍在原2秒超时失败，原断言保留。独立冻结价格实验验证提交后释放reader锁，三种变更仍23514拒绝、前后hash一致；正常停用采集后历史资格与当前准入正确分离。**仅价格不可变前提通过，T完整并行未通过** |
| E2 派发/响应/提交故障 | COMMIT不明不派发/不推进；阶段已知事实不丢；取消/过期不重领；迟到不发布；正常未知结案及资格仍成立 | 新原生checkpoint实验覆盖rollback/commit/提交返回后失联读回（未模拟COMMIT回执失败）；实际研究阶段响应实验覆盖100/100已知用量和5角色，在rollback后消失，提交后仍被旧orphan拒绝。预留保留、无重领；已知量尚无独立持久化域。**证明直接拆事务不等价，新的收尾协议待实现验证** |
| E3 共享预算与时点/围栏 | 同日整Run预留持续、非零总额边界不变；每阶段当前fence/截止/时效检查；晚结果不接替 | 原三类固定0.0007/0.0008预算边界通过；提前partial settlement实验确证不能再准入或恢复reserved；旧内存lease在任务已结案后仍通过，而DB checkpoint拒绝；原checkpoint延长/回滚/过期回归通过；首阶段已知用量但输出校验失败的提前终结另作对照。**预算和围栏原语通过，不等于T全阶段pricing/前驱变化已通过** |

实验文件：`engine/tests/integration/test_research_stage_boundary_pg.py`。它明确是设计反例/前提测试，不提供可启用的执行器。原生并发红回归仍在`test_monitor_mixed_increment_pg.py`，未改断言。

## 七、进入实现的门槛与有限路线

1. 将上述最小事实映射成研究专用离线原型：不暴露正式入口、不修改旧协议；复用原生claim与终态、完整资格和预算，不能替换关键DB边界。
2. 在原型中联动新阶段结案/取消/封存资格消费者，证明精确阶段链、未知全预留、禁止重派和迟到隔离；补同owner阶段间pricing/期限变化和前驱CAS。
3. 用原生Worker专用路径跑完整normal prepare→scan与合法writer交错，第三连接确认提交/实际锁；不能以当前前提实验或通用checkpoint替代。
4. 三组安全等价实验全部满足后，才做生产迁移与新请求协议选择；保留旧协议读写/资格分支，旧running/failed/unknown不迁移或重放。回滚时停新准入、保持在途保护，不能让旧worker领取新协议。
5. 两合法主入口及单股后继、页面/结算/收尾联合回归和终审、精确head全部CI齐备后，才按既有授权部署。原浏览器4项失败和供应链候选独立收口，不用本设计评审代替。

当前未通过项具有明确代码原因，不重复检查blocked、不追加真实研究试探。没有安全等价证据时保持正式路径不变；下一动作是有边界的离线协议原型，不退回局部锁补丁。

## 八、本轮独立实验复核修订

只读代码复核指出两项P2，已按原执行合同修正：①未完成三阶段也可能全调用用量已知，提前失败/停止仍须原子结算实际费用，不能永久reserved；已补正常Worker首阶段100/100、无第二调用、校验失败终态结算对照。②原用例在COMMIT已返回后抛异常，只证明提交后失联读回；现已改名，不再宣称覆盖COMMIT回执失败。原型仍须在提交边界注入“实际已提交/未提交、调用方收到同类异常”两分叉，并验证派发和后续推进均不发生。没有用文档更名代替这个缺失实验。

## 九、Chat合同复核收口与原型门槛

2026-10-28 原Chat已完整返回idle：marker `nexus-staged-contract-experiments-20261028-01`，request `1243e803-5f24-4c27-bb0b-a45d8cce616c`，response `99c0fc54-2b81-423e-bc95-cf5ccff85288`。结论是**有条件通过隔离原型阶段，不是T安全等价或生产实现通过**。两项P1合同约束（跨Attempt同Run阶段唯一发送权、原dispatch范围首失败/取消串行）和两项P2（提前已知失败结算、响应与cleanup分开持久化）均已并入正文；新增约束尚未被原型证明，不标代码修复完成。

复核引用发送时15项结果；随后独立审查修正已收齐，最终为16项通过/32.43秒，包含提前已知失败对照。提交后失联实验不代表真实COMMIT确认丢失，已更名并保留欠项；没有再发一次请求冒充纠正。正式源码与开始检查点完全一致，隔离测试容器已退出。

下一份v0.2必须交付同一个可丢弃数据库协议原型的三个操作——阶段授权提交、观察提交、精确链终结——及配套消费者，不写三个互不衔接helper。最低门槛为：

- E1：正常prepare→scan在模型和cleanup Event未释放前提交，由第三连接读到；完整工具读取不保留业务事务。验证原发布竞态与合法新版本writer，旧对象负例继续保持。
- E2：授权COMMIT实际成功/未成功但调用方收到同类异常；不发送/不推进，重复进入和换Attempt不产生发送权；取消先/授权先、双路首失败/补位、观察COMMIT未知、cleanup失败、新协议孤儿结案、迟到观察隔离；中间阶段记录不得被监控或正式consumer消费。
- E3：同日非零共享预算下，完整已知、已知前缀合法终止、后续unknown三链；阶段间pricing失效、原claim失效、正常前驱换代；记录原work/cleanup边界与lease变化。费用事实保留，unknown整额预留，CAS失败不重放。

若需要放宽资格、延长timeout、删除旧orphan证明、回写终态或以Python字典代替DB围栏才能通过，停止该实现；保持现有正式服务，不退回局部锁补丁。三组真实安全等价实验全通过后才讨论正式迁移/正常入口切换与旧版本回滚。

## 十、11:09续办时的环境阻塞与提交故障注入器（历史，已由第十一节推进）

2026-10-28 11:09北京时间开始新切片，独立计时；不沿用上一切片45分钟截止。正常宿主status为stopped、desired仍enabled；正常恢复检查返回`AGENT_PAPER_RECOVERY_PROCESS_UNCONFIRMED`。原进程诊断入口进一步返回`CONTAINER_LIST_UNCONFIRMED`，Docker daemon无法连接，故检查未到数据库。不能据此断言schema mismatch、账户unknown或预算不足，也不能把旧HEALTHY当当前。未启动Docker或宿主；共享Docker恢复可能连带原enabled服务，不用它代替隔离数据库准备。

新增`helpers/commit_ack_fault.py`与`integration/test_research_commit_ack_pg.py`：在SQLAlchemy dialect的DBAPI COMMIT内部，分别实际提交再报告确认丢失、实际回滚且报告同一确认丢失。Session.commit均未返回成功；独立engine读取真实提交结果，并断言原claim、尝试数、lease绝对截止和后续动作状态。它是可复用的**故障注入器测试**，尚未接入T授权/观察，不是T协议或真实网络丢包证明。两用例已可收集，Ruff通过；因Docker不可用尚未执行，不能算通过或RED→GREEN。

本切片未实现研究专用持久化协议；生产源码、旧guard及原扫描红回归未改。测试库恢复后先实跑这两分叉，随后同一个DB原型按第九节完成E1/E2/E3；不得以当前两个测试的收集结果、旧16项通过或通用checkpoint代替任何完整安全等价实验。

## 十一、隔离协议核心实验 v0.2：51项通过，但完整安全等价尚未通过

2026-10-28 11:19起按用户新授权，经正常maintenance入口暂停业务自动恢复，再启动Docker，只恢复身份已核对的55439隔离PostgreSQL。业务宿主保持stopped/desired=maintenance，业务容器均保持停止。没有启停业务研究、重放请求或通知、接受未知或释放预算。共享Docker保持可用，测试容器收尾停止、卷保留；业务恢复应另走正常安全入口，本轮不重新enable。

同一个研究专用测试原型位于`engine/tests/helpers/staged_protocol.py`，仅允许`nexus_test_<uuid>`数据库安装临时表/SQL guard，复用现有原生prepare、claim、预算、Run/Job终态；生产代码不导入它。包含授权、观察、cleanup、停止和精确原身份结案，不提供自动续发、SDK适配器或最终合格研究。输入为实验字节和受控响应用量，不能冒充实际model schema/input或角色输出验证。

| 方向 | 本轮已实跑 | 仍未完成的完整门槛 |
|---|---|---|
| E1 | 正常混合事件准备/原生领取后，以完整原价格validator短事务复验；受控model与cleanup等待期间正常scan落盘、第三连接读提交，idle-in-transaction为0，无新trigger/outbox | 实际SDK/Worker专用执行器、全部工具短session、完整正式准备链和合法新版本发布writer交错尚未接入。原Worker+scan RED仍原样，不能用原型绿灯替代 |
| E2 | 实际DBAPI COMMIT两种物理结果/同类异常；授权、观察、终结不明后同scope持久化停止；重复/换Attempt不重授；取消先/授权先、两路/补位、不可撤销stop；原生终态/过期orphan；先late后orphan和反向顺序、冲突观察、cleanup完整origin校验 | 正式新协议版本选择、不同用途owner注册、真实未调用证明、正常取消/孤儿/封存资格及下一轮准入消费者未集成；stop写入本身失败保守标记但仍需与真实共享dispatch控制器联合验证 |
| E3 | 三类非零日预算在同库：原.0007+新整笔.0002=.0009，阶段间不释放；3阶段已知后.00076，已知前缀后.00072，后续unknown仍.0009；其他两类旧行逐字段不变。定价过期、原deadline、原claim失效均阻止下一授权，原lease不续期 | 完整SDK monotonic work/cleanup边界映射、合格最终输出及正常前驱CAS换代未验证；原型只生成failed/unpublished，不把已知用量等同研究成功 |

数据库原型有两层边界：native Job/Attempt/Lease/Run/预算仍权威，临时阶段表提供同Run阶段唯一授权和append-only观察；费用已知与清理确认分别表达。终态不自动腾出未确认cleanup的两路占用。当前原型scope注册仅供该实验，不等于三个正式用途的完整注册合同；正常accept/admission未改，也未生成允许下一轮的生产凭证。

独立复核找到3项P1/1项P2并先红后修：COMMIT不明/失败结案仅本地stop→改同scope持久化；终态提前排除进程占用→SQL仍计未确认cleanup；late先到导致orphan结算→单独diagnostic eligibility，unknown继续reserved；cleanup只核token→job/attempt/worker/fence/token全绑定。追加终态COMMIT未提交分支同样实测RED→GREEN。未修改原业务断言来变绿。

最终集中51passed/73.35s（新原型29、扫描1、预算3、commit2，前轮9+原checkpoint6+共享预算1）；Ruff/format/diff通过。原native扫描RED复用相同代码既有证据，未在本轮被修复；CI未重跑，PR51旧head检查不覆盖WIP。没有生产迁移、生产源码更改、提交推送或部署。完整证据在私有`protocol-implementation-checkpoint.json`、`protocol-final-regressions.log`、`protocol-review-closure.md`及各红绿日志。

下一切片须沿同一合同完成真实适配与消费者，先补其失败用例；不得先把临时表或测试-only coordinator放进正式handler。只有上表完整门槛全部被证明，才允许正式迁移/正常入口切换与交付。当前结论是数据库核心方案可继续实验，**不是安全等价通过**。


## 十二、真实Worker/SDK边界的隔离接线（2026-10-28，本轮仍未通过完整T）

新增`tests/helpers/staged_worker.py`与`test_staged_worker_pg.py`：保留原生领取、registry/准备与完整冻结读取，在独立准备事务结束后执行真实StructuredStagedCodexCommittee及invoke_codex边界；SDK进程/响应仍是受控替身，不是自然模型验收。固定工具统一按精确Run与当前claim短session读取，包含原价格validator；最终短事务复用原execute_research_run的审计、用途、价格资格、预算及输出验证。暂仅绑定MonitorIncrement的精确Run/Job/增量身份；formal和independent root尚未选择新协议。

原mixed场景断言直接复用：正常完成、审计否决、未解决、安装回滚、unknown、独立parent后继、正常新价格/披露前驱阻止旧结果接替及下一轮账户准入。实际SDK run和cleanup边界各执行正常扫描；独立连接见阶段事实、无跨等待idle-in-transaction。相同五分钟槽的第二次扫描必须去重，不人为推进时钟制造第二条记录。

联合故障实验揭示并修复的原型接线问题：观察提交异常仍必须执行SDK清理；结算只用精确匹配的持久化观察，未确认观察保持unknown整额预留；wire＋输出schema摘要、实际响应hash和token在下一授权及最终发布前核对；终态COMMIT不明不进入通用Worker `_fail`破坏可结案身份；已观察停止进入实际SDK dispatch guard；终态固定纳入观察成员，同刻迟到不改变闭合证明。研究专用未知结果目前用测试专用异常边界表达，不是正式API。

已知费用与退出保护独立：原型消费者在原正常account admission前核阶段占用；100/100可结算，但cleanup未确认不能准入。已知前缀后合法停止只结算已授权的已知阶段，不把未授权下一阶段错误算作未知调用。新orphan只读组件复用原expired proof并核阶段链；common cycle/model/host/用户接受仍由原公共入口负责，本轮没有执行真实封存/接受。

原正常Worker+scan红回归文件与已有生产源码、依赖、原测试差异逐项hash保持不变。无新迁移、推送、CI、合入或部署；所有代码仍是隔离原型。

完整T剩余门槛：两类root正常准备时的协议选择及精确owner/旧Worker隔离；claimed零调用取消与公共history-seal/common-host/授权消费者；实际SDK固定cleanup/cancellation边界及合法发布竞争联合验证。不能把增量链通过扩大为这几项已完成。最新结果、评审关联和续办见私有`worker-implementation-checkpoint.json`；此前DB核心检查点保留。


2026-10-28 本轮Chat终审已返回且idle（marker nexus-sdk-worker-prototype-final-20261028-01），结论未通过本切片安全等价、更不允许生产切换。新增2P1：授权COMMIT已提交但未确认会缺expected_stages并落通用失败；不可信观察+终态回滚后orphan按eligible/non-null重新结算。另P2：stop写失败覆盖原SDK/取消异常。新实际Worker反例在sdk-chat-review-red.log，下一步先统一可结算的持久化观察及normal/orphan证明，不能直接扩root或迁移。完整回复与关联ID在worker-implementation-checkpoint.json。集中69pass/1fail的测试缓存问题已修，定向3pass75.42s；这不覆盖新Chat反例，不能称全绿。


2026-10-28 本切片实测收口：完整81项617.21秒＋额外核验COMMIT两分支9.87秒，共83个不同案例通过；加强授权/已知孤儿下一准入4项另通过（重叠）。原SDK错误/取消对象、停止失败附注、无后续发送及cleanup保持均通过。已知孤儿结案后的native准入拒绝已由真实Worker案例确认，不再仅是静态推断。终审及后续入口见最新私有worker-implementation-checkpoint.json；未获得完整T或生产启用结论。


2026-10-28 当前切片最终收口：final-all-suite.log86passed677.20s；终态两缺陷3项定向66.23s及SQLcause定位1项26.21s通过（重叠不加数）。指定Chat fd518/1147定向条件通过，无新P1/P2；实际回归和cause定位条件均满足，未要求重复送审。原异常、COMMIT未知、normal/orphan可信证据、停止与发布、COMMIT前SQL失败在本切片收口。完整T依合同顶部/终审四组仍未通过，公共known orphan后正常准入明确仍拒绝。宿主stopped/maintenance、测试容器退出卷保留、生产与旧RED差异/index保护已核对；未启动正式实现/迁移/CI/交付。


## 十三、根入口准备时协议选择（2026-10-28，隔离原型）

正式日研与独立三股研究均从原native准备入口进入；Run/Job/整Run预留与版本、owner、精确run/job/scope route同事务提交。正式root复用原生冻结paper context和原件验证，不把candidate原snapshot当最终冻结snapshot；独立root复用request manifest。route不可变，重复读取重新核owner但不产生发送权。旧Worker在原claim事务拒领已选新协议Job；未选历史Job保留旧路由，禁止事后升级。

创建依据不能只看Job当前行版本：保存点下Job xmin与顶层事务不同，旧行合法更新也可产生新版本。原型在Job真实INSERT时发行不可变prepared_job回执，并限制首次发行必须来自该嵌套INSERT且Job属进行中事务；route只接受未提交的创建回执。原生queued→queued普通UPDATE本就被拒，不能将审查假设写成已确认公共API漏洞。隔离反例保留原native版本/状态/时间守卫，经合法变换后仍须由精确route拒绝，公共API可达性未作主张。

启用root选择的实验环境内，root member建立和首次授权都核同一exact route；不能从无route低层install绕过，也不能拿安装overlay前已有member取得首授权。原increment实验的exact owner例外保留，不泛化到root。无route primitive测试仍可在不安装root overlay的独立库运行，不把它当root选择围栏证据。

提交前独立连接看不到精确三股Run/Job/预留；外层准备回滚后无残留。正常root三股各三阶段，沿原native最终验证完成；SDK transport/进程受控，非真实模型或真实OS退出。原native降级测试只增加before_downgrade hook：先核实验结果，删除一次性实验DDL，再原样验证原生V11拒绝及旧数据断言。这是分层保护证据，不是携带新协议事实的整系统降级通过。

首轮10项根回归110.33秒通过。指定Chat本切片终审发现两P1：overlay前旧Job补回执可注册（两root真实RED）；无route root直接install或旧member首授权可过（真实native Worker claim后四RED）。已在发行、member及authorization边界修复，原断言保持，最新16项及定向终审以`root-implementation-checkpoint.json`为准。原型集中`root-unified-suite.log`实际84项746.99秒通过（含4根路径），不能误报成前轮86项加4；额外容量8项3.38秒。受控回归不等于GitHub CI或正式实现。

本切片未完成：SET LOCAL连接池复用负例、锚定后DB等待期限反例、全部两路派发/claimed零调用取消、公共root封存/host恢复/明确接受与孤儿后正常准入、真实OS取消及prepare/writer/scan联合实验。known orphan的dead_letter仍受原正常准入拒绝，禁止仅扩终态白名单。按顶部统一矩阵继续，不再从零重建协议，不启动业务宿主、不重放、不解除未知费用或预算。

本切片新增两P1修复后16项根回归全部通过118.36秒；当前源Ruff/format/diff通过。指定Chat定向复核已完成idle（nexus-root-boundary-fix-20261028-01，response fb055d61-0868-43b5-8631-39f07ce7c420），修复代码认可、无新P1/P2，要求的当前16项实测条件已满足；本root选择切片收口，完整T仍未通过。业务宿主正常status为stopped、desired为maintenance；隔离测试容器已停止且卷保留。


## 十四、公共恢复、封存和孤儿后准入（2026-10-28，隔离原型）

`staged_public.read_terminal_proof`统一读取原owner、精确Run/Job/Attempt/Lease、模型版本与预算、阶段授权、核验及冻结终态成员。known按冻结可信观察和原usage_cost重新核算；当前cleanup独立核原授权的不可变清理行，不从Run终态或费用已结算推导退出。结案后补到的精确清理只影响当前生命周期，不重算冻结财务证明。正式源码只抽取原monitor单成员/finish谓词和orphan行锁、共同资格、结案函数，默认语义保持；新消费者通过测试适配安装，不是生产协议启用。

公共monitor finish/admission复用同一证明。已知且cleanup确认的孤儿可按原finish_increment收尾后正常准入；unknown保留原预留并阻断，cleanup未确认则不推进触发终态。原account锁、整组触发成员与审计保持，不是直接给dead_letter放行。真实Worker/SDK代码路径使用受控SDK响应及清理，不是自然模型或OS进程验收。

正式root公共恢复仍先核原共同资格；实验适配按scope→Job顺序取锁，在外层恢复事务内调用同一个close_in_transaction。结案后注入异常时，原Run/Job/Attempt/Lease/预算/事件和阶段终态一起回滚。原standalone close仍保留确认未知处理，不以公共调用拆出独立COMMIT。选过新route却缺member时拒绝，不能退回旧零记录假设。

封存沿原CLI显式接受、原公共账户/周期/模型/宿主静默及审计入口。先通过原expired unknown完整证明，再核冻结阶段链和cleanup；审计fact_hash额外绑定协议版本、stage_chain_hash及cleanup_hash，写入和历史读回同一逻辑。cleanup按读取时点过滤；封存须完整确认各原授权阶段，历史读回核原审计时点的同一不可变清理行，不以未来新行扩大旧证明。原不接受、进程仍活跃、迟到native事实、封存回滚及新身份准入断言保持。这里的明确接受全部是隔离合成案例，不涉及任何真实未知影响。cleanup未确认时无封存审计，预算仍reserved。

当前正式root用例复用原手动流程，但阶段与进程证据受控；独立v3 root收尾/未知拒绝和宿主恢复消费者未全验。源码核实v3没有既有cycle封存入口，不能把新增接受政策当作待跑测试。真实OS重复取消与固定cleanup截止、两路派发/claimed零调用取消、锚定后数据库等待期限、SET LOCAL连接池复用，以及完整prepare/合法writer/scan联合实验仍未完成。完整T未通过，不启动正式迁移或新协议发布。实际回归与终审结果见私有public-implementation-checkpoint.json及本节追加结果；旧Worker+scan生产红用例保持。


2026-10-28 公共消费者本轮实测：修改后初次集中113项892.57秒通过；未安装实验适配的原封存21项125.48秒通过。Chat终审335d9a1f发现P2：迟到精确cleanup被冻结财务成员排除，known孤儿无法公共收尾。真实Worker两例原断言2失败49.20秒，修正为财务证明不变、当前生命周期另核原授权清理行；封存审计追加cleanup_hash并按原审计时点读回。修正后公共13项161.58秒全部通过，另加强两项46.91秒通过（重叠）：错token/attempt拒绝，原terminal/Output/tokens/预算不变，零新claim/SDK/计划/封存审计。公共恢复回滚和cleanup未确认保护包含于13项。Ruff/format/diff和两份生产源码Mypy通过。

指定Chat定向复核已发送nexus-public-late-cleanup-fix-20261028-01（request91d8b3cc），当前结果以public-implementation-checkpoint.json为准，未收齐不得称终审完成。公共proof损坏拒绝、两事务恢复与迟到写入交错、外层COMMIT不明仍须补齐（公共外层rollback已证）。本轮不是完整T或生产启用，业务宿主继续维护停用；不重放、接受或释放真实未知，不新增迁移/推送/CI/合入部署。


定向终审已完整返回idle（response a7aae28c-e87a-429e-89da-c2dee0272f09）：迟到cleanup P2代码修复认可，无新P1/P2；其当时待收取的加强2项/public13项已实际通过。仍需补当前cleanup时点/缺末项/未确认对照，以及unknown terminal后追加cleanup再显式seal、cleanup_hash重复读回与迟到诊断的窄边界证据。不得将代码认可及已有绿灯扩大为公共生命周期或完整T全部收口。测试保证无新增封存审计，正常finish业务审计仍保留。


## 十五、公共边界补证收口（2026-10-28）

本轮没有修改生产策略或阶段协议，只增加公共边界回归及root夹具控制接缝。三阶段cleanup缺末项、confirmed=false、早于授权拒绝、未来记录到达as-of前后；unknown terminal后追加精确cleanup，经原CLI显式seal、API/审计/新身份准入，再追加合法迟到诊断和跨日重复读回；公共recover外层实际COMMIT/rollback丢确认；恢复与过期原claim迟到observe两顺序的真实scope锁等待；chain/known/cleanup损坏在封存前与历史读回拒绝，均已实跑。损坏注入先核SQL不可变，再仅在可丢弃库绕guard，不能声称正常API可造成损坏。

集中26项261.28秒通过（本轮13＋原public13）。独立代码复核发现COMMIT成功分支原测试只读部分状态的P2证据缺口，补为提交前全组native facts/terminal快照与另一engine提交后等值，并调用完整proof；定向2项18.22秒通过（与26重叠）。另加原monitor finish/admission对损坏terminal拒绝1项26.05秒通过。初次并发夹具缺asyncio导入，修正后2项18.52秒通过，不计产品RED。正常finish业务审计保留，断言“没有新增审计”仅指封存审计。相关Ruff/format/diff通过；未变化的生产/协议旧证据直接复用。

指定Chat有限终审已完成idle：nexus-public-boundaries-closure-20261028-01，request37b1eb3f-7185-4efe-aea0-7c8057999021，responsead2cb120-e808-423b-8fb6-d92e4c50a5b6。当前公共边界定向缺口认可，无新增P1/P2；其待收26项终态已实际通过，不再重复送审。此关闭的是formal/monitor公共proof这一组，不是独立v3/common-host、派发期限/真实OS和prepare/writer/scan全部安全等价。

原型转正式的硬门槛、实际消费者、可复用代码及正式实现后交付验证已集中到顶部矩阵。特别纠正：独立v3现有入口没有cycle seal能力，必须保持unknown拒绝，不应新造接受政策，也不把不存在入口写成普通待跑测试。其已有恢复/已知收尾/下一准入及宿主只读/锁后检查仍是硬门槛。达到硬门槛即迁入同一机制并交付，不因可选性能调优或纯整理延长原型。

业务宿主正常status仍stopped/desired=maintenance。所有自有测试结束后隔离PG已停止、restart=no/卷保留；未重放、接受或解除真实未知，未推送/CI/合入部署。最新检查点public-boundaries-checkpoint.json，原公共/根检查点保留作证据。


## 十六、根研究与宿主共用阶段终态证明（2026-10-28）

独立v3正常finish先执行原native settled，再核原manifest与精确route/owner；formal周期历史先执行原native cycle settled（保留read_only、lease及原accepted synthesis参数），再按原cycle事件成员核同一证明。两者共用_selected_member_settled：已选且有阶段行必须有有效terminal及cleanup；无阶段行不能当作无调用，须满足原精确取消证明。原research_failure逐member谓词仅等价抽取，owner_id对应原manual-cancel请求或cycle身份，无新的取消或接受政策。

独立cleanup缺失两例实际RED，修正后四组合通过；formal实际正确scope下缺cleanup的host误ready为一RED、一对照，修正后两例通过。早期formal夹具两失败来自runtime scope错误，明确不计产品RED。原消费者已写unavailable的历史请求仍可幂等读回，不追改旧结果；新host/普通准入必须阻断。正常三股成功走原最终输出、三Run proof读回、公共finish completed与真实readiness。host均到实际RecoveryActivation及隔离lifetime锁后复核，第二锁拒绝，业务heartbeat/calendar回调禁止执行。boot/process静默仍受控，不能当真实OS或业务启动证据。

最终selected-consumers-final.log集中37passed359.46秒：本轮10个参数化案例及原公共27。之前34passed343.53秒在formal增量之前、正常成功与历史加强2passed33.37秒及formal2passed15.78秒均重叠，不相加。原未安装适配的preclaim拒绝14passed45.59秒，抽取谓词的原行为保持。原断言、未知预留与正式门禁不变。取消包装器只暂置原生未调用入口需要的LOCAL标记，普通失败恢复旧值、SQL事务失败保留同一原异常与固定note；验证包括后续Session读回，尚未证明同一物理连接池复用，不能关闭下一组期限/池矩阵。

独立消费者指定Chat终审e8e9a57a-cbe8-4dc0-b518-ea312edf1d41无新增P1/P2，所需正常成功及marker异常结果已通过。formal增量首包fd5de54b仅因源码截取缺段待核；补全精确wrapper、共用helper及native前置和生产diff后，nexus-selected-consumers-source-20261028-01/request58e6106c-1a2e-4c00-bfba-021c71d66527已idle返回13ffbc46-0f6b-45ae-882c-88232f1582d8：无新增P1/P2，本消费者切片结束条件满足，不需重复同范围送审；完整T未放行。原公共/root/protocol/Worker不变证据复用。

下一组只推进原矩阵剩余硬门槛：正常增量prepare选择、共享两路/第三股补位及claimed零调用取消；原期限下DB等待和同物理连接LOCAL隔离；真实OS重复取消/cleanup；完整prepare/合法writer/scan联合验证。齐备后迁入同一正式机制，不另建框架。当前未启用正式协议、迁移或交付，业务宿主保持维护停用。


## 十七、正常单股准备选版与旧身份隔离（2026-10-28）

同一既有实验路线补齐normal admit_batch/admit_trigger：在原外层事务内加一层savepoint，将native admission和既有不可变route登记一起完成。只有当前事务Job INSERT receipt证明的新任务或已存在精确route可登记/读回；已提交但未选择的旧增量正常幂等读回保持legacy。Worker删除“存在MonitorIncrement就走新协议”的隐式fallback，只消费已持久化且重验合格的route；选中任务旧Worker拒claim。owner再核原require_increment_binding；低层member与首authorization均核increment精确run/job/scope/kind/version，覆盖预先已有member的绕行。未新增通用机制、生产入口、DDL迁移或接受政策。

六项原缺口实际RED：两parent正常混合批次缺route、旧增量隐式升级（3failed57.66秒）；未登记导致原子观察不到、无route原claim后可install及预存member可authorize（3failed62.70秒）。修复后六项171.04秒通过。首轮修复三失败来自新增spy误将原fixture第二次empty poll当第一次；保留一次工作/随后false与原全部业务断言，明确区分0→3授权且不再增加，不计产品RED。另复用price-only原CLI的settled/unknown两路径；当前受影响集中87passed1305.65秒，包括新8、Worker26、root16、公共37；随后回滚用例加强为两参数2passed35.26秒，替代原一项，覆盖当前88项不同案例而非单次88项运行。未修改的核心协议/旧原生证据复用。

独立代码复核及指定Chat 0ac12669-e46d-4bb5-a221-fbcfd1971ea4（marker nexus-increment-selection-20261028-01，request d8f3b922-b0a7-4cd1-991b-0bc0035d5097）未发现新增实现P1/P2，指出outer rollback不能证明外层捕获后继续提交。已补原正常CLI实际捕获MONITOR_CONTEXT_INVALID并提交blocked诊断：既有route schema保留，本次Run/Job/预算/member/scope/route/birth receipt均不残留，旧全类预算及attempt数不变、零SDK；两种rollback参数2passed35.26秒。隔离子Python仅在内存移除savepoint，同一已有schema用例如期失败18.49秒，源码不变；该失败为负向对照，不是候选残留失败。仅承诺wrapper内部产生的事实原子，不回滚调用方在进入前已flush的其他工作。

本组不证明两路并发/第三股补位、已claim零调用取消、锚定期限/池复用、真实OS退出或完整prepare/writer/scan。集中结果及窄原子补证已满足Chat定向终审条件，本组选版切片收口，不需重复送审。上述仍是原矩阵剩余硬门槛，齐备即迁入同一正式机制；不能因原生扫描单例通过宣称全链安全等价。宿主维护停用，旧请求/通知及真实未知费用/预留不动。

本轮Ruff/format/diff通过；生产源码、公共证明/core协议与原锁冲突RED及受保护差异未变。所有测试完成后，隔离55439零临时库/零其他client，精确容器已停止，restart=no且卷保留；正常业务宿主stopped/maintenance。无正式迁移/CI/交付、真实模型或通知重放、未知接受或释放。


## 十八、真实共享派发与资源退出保护（2026-10-28，隔离验证）

使用正常独立v3准备产生的原manifest/三Run/三Job/预留和不可变route，调用原 `_dispatch_two_flight_members`、原Worker与实际SDK边界，SDK进程受控。并发实测首先复现测试adapter跨await全局patch串用callback（OBSERVATION_CONFLICT）；改为七个固定seam和按Worker的ContextVar回调，绑定缺失的SDK构造直接拒绝，子Task继承创建时回调；共享ResearchDispatch仍是同一个对象。context reset不是停止权限，仍走原scope/claim/SDK guard。

本地只读复核进一步指出受控pre-final异常会进入旧Worker通用_fail；补断言实际复现Job变dead_letter。该受控中断现走BaseException研究边界，先保存原共享失败原因，外层核cause身份后抛原异常；不带hook的正常SDK失败/正常终结不变。取消仍原CancelledError。新断言验证原Job/Attempt running、Run prepared、单次claim、无terminal、原worker/token/fence/expiry和预留均保留，不能用异常对象不变替代这些持久化事实。取消初次补断言误写Attempt.outcome=None，已按原schema纠正为running，单独标为测试错误。

指定Chat首轮750a2adf指出原控制器finally dispose可覆盖主异常P2。新隔离用例实际复现普通SDK错误和取消被资源释放RuntimeError覆盖；唯一新增生产修正仅资源出口：读取实际传播异常sys.exc_info()[1]，已有主异常时只加固定安全note，无主异常时dispose失败仍抛。没有开启阶段协议、改调度/超时/门禁/预算或生产迁移。

同轮补齐首失败与cleanup占槽交叉窗口：首股SDK原边界已置共享stop而cleanup仍Event挂起，第二股当前已授权stage完成后退出；释放首股cleanup前第三连接见authorization2/cleanup1/terminal0，第一股仍active，第三股未领取。hook没有代写stop。旧控制器下该窗口2对照通过、dispose2分支失败44.32秒；修正后6项全部通过71.29秒，包括成功之后dispose错误不吞、失败/取消原对象及固定note保持。

其余受影响结果：Worker26项643.78秒通过（窄pre-final wrapper前的ContextVar版）；最终wrapper及原异常/实际SDK scan/terminal SQL故障7项132.89秒通过（含当时3派发例，与上述6的基础例重叠）；旧未选increment和两类root7项107.53秒通过。各组重叠不累计为单次全量，最新native原派发回归、静态检查与定向终审终态见私有dispatch-checkpoint.json。终审补包marker nexus-shared-dispatch-closure-20261028-01已发同一指定会话，当前未取回不预称通过。

本组不证明claimed零调用结案、固定期限下数据库等待/同物理连接LOCAL隔离、真实OS反复取消/退出，或完整normal prepare/合法writer/scan联合安全等价。这些仍为原矩阵硬门槛，齐备才迁入同一正式机制；不扩建原型。宿主stopped/maintenance，无请求/通知重放、未知费用/预算接受释放、推送/CI/合入部署。原生产Worker+scan RED保留。


最终收口（2026-10-28）：Chat 补包 3ad7e591-6e43-4598-9239-afca6dde6bca 已返回；外层 except 继承旧异常的 P2 已用实际控制器单测复现（1 失败、5 通过），completed_normally 修复后 39 单测通过，最终 18 项数据库回归通过（125.84 秒）。类型、格式和差异检查通过，本地复核无新增问题；评审明确的本切片结束条件已满足，无需重发。分批覆盖 51 个不同数据库案例，并非单次全量。隔离库零临时库、零其他连接后已停用保卷，宿主仍维护停用。完整 T 尚未通过；下一切片是已领取但确证未调用的正常结案及公共证明消费，其后保留期限、连接池、真实进程和 prepare/writer/scan 矩阵。未推送、合入部署、重放或释放真实未知预算。

## 十九、已领取且确证未调用的原生结案（2026-10-28，隔离验证）

实际原型缺陷已复现：原生 `stop_uncalled_research` 在 `execute_research_run` 捕获前返回，旧adapter将Run/零用量/释放预算提交后才报 `PREPARATION_NOT_CAPTURED`，未与原生Job/Attempt终态原子提交；修正后公共finish又拒绝已消费但未到期的lease。当前adapter在同一短事务执行原生finalizer、正向证明、scope stop和commit；正常Prepared仍整体回滚准备事务后才进入SDK。锁序统一为scope→Job/Lease→预算，未改变业务写保护或时限。

共享 `require_claimed_uncalled_research_closed` 核对原Run/Job/唯一Attempt与lease、零tokens/费用和released原预留，以及精确prepared→abstained/model_research_stopped两个事件、唯一零调用输出及原canonical摘要。缺stage本身不证明未调用。已选版公共消费者另核精确route、scope stopped和stage member不存在；存在member时继续使用原阶段链证明。普通finish、账户准入与宿主读取同一事实，不新增接受未知入口或执行权。

本地只读复核发现AGENT_ABSTAINED不是纯准备停止专用码；已缩小原生分支至seq2明确停止事件，其余弃权沿原lease政策。使用真实 `execute_research_run` 的evidence_authority_missing、原生claim/finalize验证租约过期后正常结案；仅在独立子进程内恢复旧宽分支后，同一用例实际失败 `MANUAL_ACCOUNT_BUSY_OR_UNKNOWN`，候选源码未改。不得将合法资料缺口、模型审计否决或已调用弃权一律解释为未调用。

四种原子结果已验证：正常提交、真实COMMIT成功但确认丢失、实际ROLLBACK但确认丢失、终态SQL失败。独立引擎在提交前只见原running/prepared/reserved，提交不明后另引擎读实际落盘；回滚/失败保持claim及预算，公共finish与host拒绝。提交不明不会重试或落入generic `_fail`。纯停止在真实bind_claim之后、peer准入前注入，所有业务写入仍由native handler/finalizer完成；该用例不冒称两路交错，真实共享并发由原派发组覆盖。

最终聚焦12项（新6＋原派发6）131.76秒通过；含事件/输出摘要、预算、tokens及lease损坏读取负例（未flush）、真实不可变迟到UPDATE拒绝、旧claim重新install拒绝、重复finish不改原结果、实际lifetime锁后恢复检查及下一独立请求正常queued。恢复验收在业务循环前停止，无宿主/SDK真实调用。13项相关单测3.96秒、Ruff/format、两生产文件Mypy通过。公共/root53项599.90秒通过；该组在最终分类收窄和锁序补齐前已加载，最终差异另由上述12项覆盖，不冒称同一次全量同版。其他关联结果与指定终审终态以私有 `uncalled-checkpoint.json` 为准。

当前仅本地候选：两处原生证明/消费改动、两个隔离adapter和回归，无新迁移、历史结果或未知费用变更。正式阶段协议仍未启用，PR51/head1244e6c4不变；未推送/CI/合入部署。完整T仍欠原期限下DB等待与同物理连接LOCAL、真实OS重复取消/cleanup、完整normal prepare/合法writer/scan矩阵；齐备才迁入同一正式实现，不继续扩建原型。原生产Worker+scan红证据和用户未提交内容保持。

本组最终收口：32项Worker/dispatch通过810.85秒，同样在最终分类/锁序两处收窄前加载，差异由最终12项覆盖；最终代码的原生策略31项136.78秒通过，保留旧版取消、未知预算阻断和前置拒绝。指定Chat marker `nexus-claimed-uncalled-20261028-01`，request `53b48f17-5343-4777-bcd4-632b2e85b2af`，response `ba5c9723-d005-4e36-9f0a-c1d7387ac21e` 已完整idle返回，无新增P1/P2；其最终6＋6及静态检查条件均满足，不需重发。各组重叠不累计为一次全量。所有测试结束后55439零临时库/零其他client，精确容器stop且保留卷/restart=no；正常宿主status stopped/desired maintenance。保留未知费用及预算，未发生真实调用或历史处置。下一切片继续剩余期限/物理池、真实OS和prepare/writer/scan硬门槛。


## 二十、原期限下数据库等待与同物理连接隔离（2026-10-28 23:39，离线切片收口）

Worker只在入口建立一次wall/monotonic映射，沿原committee和SDK分配的阶段截止取较早者；scope、Job/Lease、budget锁等待后及授权INSERT、COMMIT前后刷新资格，并在异步session成功退出后、无await的Ticket返回前最后检查。跨过截止不返回发送权，已提交授权仍保留，不能从零发送或缺响应推零费用；原lease/pricing和预算限制不变。实际SDK原时长验证在阶段截止已过而父期限有效时零授权/零调用，真实native final落failed/unknown，预算reserved且下一准入拒绝，没有用pre-final中断替代收尾。

真实PG反例包括scope/job/budget等待3失败、verify/COMMIT迟确认2失败、实际SDK分配截止1失败、session退出跨阶段/父截止2失败。扩大public回归再发现原supplied time被fresh clock修复的3个反例，现先校验原授权/观察时间再刷新当前资格；不改旧断言。正常终态两次取时的真实差异4.726056/4.730970导致公共证明拒绝，现同一finalize取一次closed_at供terminal及JobCompletion；新成功/已知前缀停止两例在真实INSERT后推进1秒，核terminal=Job=Attempt、公共proof/finish/正常准入。原fixture后续时钟倒退仅向前校正，不放宽正式时效。两次过短deadline夹具及一次漏Job导入的错误单列，不冒充产品RED。

同物理池四例使用pool_size=1/max_overflow=0及相同pg_backend_pid；成功、普通异常、SQL错误、CancelledError后旧LOCAL标记不泄漏；沿同连接原生claim仍以staged worker required拒绝，attempt/auth/SDK均零。显式CancelledError不等于真实OS重复取消，后者继续保留。

最终代码：deadline18 18 passed in 323.41s；Worker/dispatch/uncalled38 38 passed in 828.20s；root/public53 53 passed in 599.50s。各组不合成一个独立总数。Mypy engine/src 672文件、Ruff/format/diff通过。较早core43、Worker38、public53及14项保留其加载版本边界；最终结果不由早期通过替代。

指定Chat首包d12d563c-767a-4677-ab33-e3c9027b7947/response f4305ee1-34fc-4df6-9fe9-fbd73436a0e7的两P2均已复现修正；补包marker nexus-deadline-pool-closure-20261028-01/request d6f92261-246b-4345-bfb7-5ce0b21ff64f/response 9ca8f261-62d8-43eb-8175-b81e00aef246完整idle返回，无新增P1/P2，最终本地条件已满足，无需重发。补包finalize代码块因截取范围错误为空，审查明确允许本地实际源码与回归补证；已保存并核对deadline-finalize-confirmed.py单次取时及双用途，没有宣称Chat逐行核验了未收到的片段。本地定向复核无新增问题。

本轮仅测试适配与回归、既有计划更新；生产代码、公用reader/route、原Worker+scan RED、保护diff和PRIMARY暂存内容保持，HEAD1244e6c4/PR51未变。无推送/CI/合入部署、生产迁移、真实模型/通知重放或未知接受释放。所有测试退出后55439零临时DB/零其他client，精确容器3f7306ee8683停用保卷/restart=no；正常宿主仍stopped/desired maintenance。

下一切片只补原矩阵真实OS重复取消/原cleanup截止，然后完整normal prepare/合法writer/五分钟scan联合验证；已有阶段证据复用，全部硬门槛齐备才迁入同一正式机制，不扩原型。最新私有入口deadline-checkpoint.json及final-sources；完整T未通过。

## 二十一、真实进程与重复取消：本地矩阵通过，指定终审待收取（2026-10-29 00:28）

本轮15:50:26UTC开始，16:35:26UTC为有界回合截止。按原T矩阵补真实OS子进程、重复取消、固定清理截止与数据库确认未知，未增加产品用途或恢复权限。最新私有入口为writer的 `.secrets/staged-execution-20261028/process-checkpoint.json`。

实际复现原close被重复取消后父调用先返回而child仍活（2RED）、观察被真实PG锁阻塞而SDK close尚未开始（1RED），以及hook吞取消后迟到写入成功cleanup（1RED）。研究适配层复用原SDK core和有界close，保留原state、run_task与captured child，仅close一次；原work/parent/lease映射与close入口7秒取最紧固定截止。首次主异常/取消同步停止本地及共享派发，并发落盘scope stop；观察、进程、回执、停止均在原边界内等待，残留任务保持归属并收取结果。原异常对象保留，辅助错误仅固定安全说明。

当前调用的cleanup在hook返回后重核monotonic截止，并向持久化边界传原anchor映射的可选not_after，在读取后、INSERT后及COMMIT返回后检查；历史恢复调用不传该参数，原公共迟到合同不变。COMMIT实际成功但确认迟到或丢失，原行仍可按同一身份只读核实，调用方不改判成功；不能声称超时意味着数据库必定回滚。known费用仍取已提交verification，未知预算与cleanup占用分别保护。

新增矩阵23个参数场景已分组覆盖：响应/未响应的正常退出、强制退出、kill不可用、poll失败、close吞取消；观察锁阻塞；observe/verify/cleanup/stop的真实COMMIT成功与回滚确认丢失；迟到hook；原SDK错误叠加重复取消与辅助失败；跨截止才返回的COMMIT两分支。不是一次23项全量运行，加载版本及复验边界见checkpoint。初次预算断言误取夹具父研究，已改为精确MonitorIncrement.run_id，原reserved断言未变；startup观察改为同时观察原Task与事件，原40秒watchdog不变。

共享派发旧夹具将peer三个研究阶段塞进first的7秒清理窗口，集中及重跑各出现一次失败、单独诊断通过；现复用before_final_hook对齐真实交接点，first仍在cleanup占槽，peer最终提交后才第三股补位。所有原持久化/并发/费用断言和7秒保持，最终共享派发6项98.52秒通过。Worker单文件26项835.87秒、原SDK/派发单测213项8.76秒、672文件Mypy、Ruff/format/diff通过。既有期限/池及公共消费者证据复用；新可选截止边界由新增迟到/COMMIT及最终共享派发覆盖，不能把旧加载版本标成最终代码全量重跑。

指定Chat设计回复843c77f5要求的接线已实施；独立fresh-context复核发现迟到回执P2并复现修复，定向复核无新P1/P2。实际core、SDK finally与bounded-close原文已回送同一Chat，marker `nexus-real-process-final-20261028-01`、request `48b2ae74-c148-44fa-b9d3-f95774f43e23`，当前仍active，未宣称终审通过，不重发。后续只取回此请求并按实际代码核实意见。

业务宿主正常status仍stopped/maintenance。隔离55439在零临时库、零其他连接后已停用保卷。生产SDK、原公共reader/route、既有生产差异、原Worker+scan RED及PRIMARY用户index均保持。无模型/通知调用、未知接受或预算释放、生产迁移、推送/CI/合入部署。完整T仍须当前终审收口及normal prepare/合法writer/五分钟scan联合证明；不得自动开启业务宿主。

## 二十二、清理首次失败、异常归属和原截止终审收口（2026-10-29）

唯一writer仍为 `/Users/wangchen/.codex/worktrees/synthesis-failure-facts/投资`，branch `edward/research-lifecycle-closure`、HEAD1244e6c4、PR51不变。本回合02:17:36UTC开始，03:02:36UTC边界；本切片完成即结束。最新入口 `.secrets/staged-execution-20261029/checkpoint.json`，合同§二十二。

原1P1＋2P2已逐项RED→修正：第一次cleanup/观察/SQL/COMMIT失败立即停共享派发，不等内部进程退出或回滚；本次工作异常、清理异常和调用方旧异常分离；观察吞取消后不能跨原D_close启动或完成新的可信验证，截止前已发COMMIT仍按真实落盘只读判断。原stage+7、parent、lease映射及close入口+7取最紧界限，不延时、不重放。指定终审补出ambient旧异常＋本次cleanup失败，两例实际复现并以隔离core私有载体修正，原异常身份和旧对象不变；同时RED修复重新分类把已测清理耗时覆盖成0。

最终同版定向11＋共享派发6＋受影响Worker7共24项通过；最终真实OS/COMMIT矩阵23项通过。Worker完整26项通过的加载版在私有载体前，其受影响7项已纳入最终24，不累计冒报同版全量。原SDK/派发213项复用本轮通过证据，最终Mypy672、Ruff/format/diff通过。新增ambient夹具曾遗漏已审阶段公共reader安装，末尾准入断言失败；只补隔离接线，原拒绝断言未改，不计新产品缺陷。各日志精确时长/节点和版本见checkpoint。

指定Chat response `09d72765-b04e-49af-ba50-134b1718186f` 已完整返回，允许上述有限补例及最终矩阵满足后本地收口，无需未变内容再次送审；条件现已满足。本地定向只读复核无新P1/P2。本结论仅关闭真实OS重复取消/固定cleanup组，完整T仍欠正常prepare、合法writer与五分钟scan联合证明；不重做已关闭组，齐备才迁入同一正式机制。

正常业务status仍stopped/maintenance。全部测试退出，精确自有55439零临时库/零其他连接后停用，卷保留。生产源码、原Worker+scan RED、用户未提交差异与PRIMARY index保持；无真实模型或通知调用/重放、未知接受释放、生产迁移、推送/CI/合入部署。下一回合从剩余联合矩阵继续，不能启用业务宿主。


## 二十三、正常准备、合法写入与五分钟扫描联合验证及终审收口（2026-10-29）

唯一writer仍为 `/Users/wangchen/.codex/worktrees/synthesis-failure-facts/投资`，branch `edward/research-lifecycle-closure`、HEAD1244e6c4、PR51不变。当前回合03:27:38UTC起、04:12:38UTC边界。最新证据 `.secrets/staged-execution-20261029/joint/checkpoint.json`，前组日志/检查点保持。

本轮只新增正常prepare/合法writer/五分钟scan及八工具正向回归，生产源码和四个staged helper与入场指纹相同。模型等待期间，原生reader的排他锁确实使合法新manifest writer排队；原reader提交后writer继续。新manifest提交/回滚与正常coordinator→observe→真实导入代码→scan并行；网络边缘和SDK响应受控。scan可独立消费旧冻结版本，不把这一分支冒称为prepare被新writer阻塞后重读。新发布提交后authority/hash/count与正常prepare一致，回滚后不可见；旧snapshot链接/hash/selected_at不变，三类预算未变。

本地fresh-context评审指出仅零trigger/outbox不足以证明有效无触发，已补全部价格计划的精确check集合、false/reasonNone及原生最新observation与准备输出的精确相等。新合成行情不修改旧触发和研究；披露unknown继续保持unknown。重复kick/observe/scan没有新增采集、触发、通知。八个固定工具全部以真实claim、严格原生reader正向结果比对，逐次返回后无idle事务且可立即锁定同一Job。`unresolved`是原夹具业务事件状态，不冒称本测试覆盖未知用量；未知预算保护复用前组证据。

最终7项70.03秒通过：联合提交/回滚2、工具2（覆盖8工具）、原SQL冻结/合法写入排序3。Ruff/format/diff通过。先前夹具失败是模型字段/正向资料/原生返回语义及旧价格条件假设错误，不计产品缺陷RED；未删除或放宽既有断言。前组最终24/23、原SDK213及Mypy672等同代码证据直接复用，不冒称本轮全量或CI。

正常业务宿主stopped/maintenance；全部测试已退出，精确自有55439零临时库/零其他连接后停用，卷保留。原生产Worker+scan RED、用户改动及PRIMARY index保持。无真实模型/通知/资料调用、重放、未知接受/释放、生产迁移、提交/推送/CI/合入部署。

指定Chat终审已完整返回idle并核对同一marker/request，response `cc12395c-a3a3-4bbe-b1b1-575e12148783`，无新增P1/P2。实际新断言/最终7项日志、四helper与前组闭合指纹已本地核对；正式实现前的既定T安全等价门槛已收齐。当前切片结束，下一切片直接将同一研究专用机制迁入正式模块、迁移及显式版本/owner路由，不再扩建原型；部署前仍须历史读取/旧身份隔离、在途保护/兼容回滚、正式API/BFF/实际页面、同head必需CI和镜像验收。维护停用、未知费用/预算及v3未知拒绝政策不变。该结论不是正式实现已落地、部署或真实自然业务验收。

## 二十四、正式存储、迁移与版本路由基础（2026-10-29）

唯一writer仍为 `/Users/wangchen/.codex/worktrees/synthesis-failure-facts/投资`，branch `edward/research-lifecycle-closure`、HEAD1244e6c4、PR51不变。本轮04:06:47UTC起、04:51:47UTC边界，完成当前切片即结束。最新入口 `.secrets/staged-execution-20261029/formal/checkpoint.json`。

已把同一授权/观察/核验/清理/停止/结案核心、精确准备时版本路由及统一只读终态证明迁入 `engine/src/nexus/agents/research_stages/`。新增0099受审迁移持有DDL，正式模块不导入测试helper或monkeypatch、不在运行时建表；代码schema authority同步0099。37条DDL与原机制顺序及约束保持，仅正式namespace/version改名；原未提交代码和四helper指纹保持。原primitive专用旧孤儿诊断没有再复制，统一使用已验证canonical proof。

正常正式root、独立root和两种父来源的增量在原准备事务夹具中调用正式router，验证同事务birth、精确owner/version、提交后禁止补选、不可变路由及旧Worker拒领。旧未选版Job仍走legacy，迁移/空stage回退不改旧Job及预算。已知用量一次结算、未知保持reserved，统一读证明同时核财务与cleanup；没有把测试normal准备注入冒称为已启用生产路由。

新回退路径的P2已两次明确RED→修正：stage先锁会阻塞正常jobs→scope准备；只改jobs先锁又会阻塞已有scope→Job阶段。现有事实先只读拒绝，Jobs fence后重核新提交事实，stage锁NOWAIT后再核，不能持Jobs等待已有stage工作；并发提交scope时拒绝删除并保留预算、版本。fresh-context定向复核无新P1/P2，统一proof原函数规范化AST未偏离。

最终16项139.59秒通过：正式存储/迁移/路由/proof12、schema authority2及原prepare/writer/scan2；676文件Mypy、Ruff/format/diff通过。此前claim夹具遗漏原research profile/locked guard与schema authority未同步的失败已记录并修正，原断言未放宽。前组安全等价矩阵复用，不冒称已在正式Worker全量重跑或当前CI通过。

本切片是正式持久化基础，不是整套机制上线。正常生产者、Worker/SDK及公共收尾/封存/恢复/下一轮准入尚未切换；下一切片把它们一起接同一正式模块，不能先放行新选版Job再补消费者，也不再扩建原型。仍须历史读取/在途保护、正式BFF/实际页面、同head CI和最终镜像。无阶段归属时空schema回退已验；已有事实必须留schema并用兼容reader/Worker镜像回滚，该镜像仍待交付前验证。

正常宿主stopped/maintenance；全部测试退出后自有55439零临时库、零其他连接，已停止并保卷。无真实模型/通知/资料调用或重放、未知接受/预算释放、live迁移、提交/推送/CI/合入部署。保持本切片检查点，不启动宿主。

## 二十五、正式生产者、Worker/SDK 与公共消费者统一接线（2026-10-29）

唯一writer `/Users/wangchen/.codex/worktrees/synthesis-failure-facts/投资`，branch `edward/research-lifecycle-closure`、HEAD1244e6c4、PR51未变。本回合04:24:34UTC起、05:09:34UTC边界。最新私有入口 `.secrets/staged-execution-20261029/runtime/checkpoint.json`。

正常formal三股根、独立v3根、普通/混合增量现在在原准备事务登记正式版本；生产Worker在原资格检查之后读取精确route，显式委托 `research_stages.executor`。准备/工具/阶段观察与验证/清理/最终发布使用原机制的短事务，selected路径不落入通用重试/失败结算。SDK新增可选显式生命周期hook，区分本次工作、cleanup和ambient异常；legacy调用不带hook保持原接缝。公共monitor收尾/账户准入、独立及manual结案、孤儿恢复和已有封存入口统一消费正式proof，保留原账户、进程、租约、未知接受政策，不新增v3未知接受。无测试helper导入或运行时DDL。

本地只读评审发现正式选版后的增量capacity前置拒绝缺claim标记及零调用公共读路径，已用原正常场景复现staged worker required并修复；仍须精确native零调用回执、stopped scope、无stage member。后续复核指出不能跳过trigger unknown，现零调用member分支委托原legacy reader。负例在会话内将同run全部trigger一致设unknown，先确认零调用proof确实成立，再确认账户拒绝，未flush或改旧事实。不是靠缺stage推断零费用。

验证分组：正式runtime7项193.50秒（两根正常Worker/SDK各9阶段、正常增量模型等待时真实五分钟scan、已知/未知/容量拒绝及下一准入）；最终reachable unknown负例1项49.06秒；known用量但cleanup失败1项46.92秒，可信费用正常结算而准入仍拒绝。公共恢复/CLI封存/未接受/迟到事实/close rollback4项30.88秒，复用原正常案例断言。正式storage兼容12项分10项26.23秒和2项42.80秒通过，旧出生与旧父夹具仅明确选择历史版本，原断言不改。SDK151＋新异常归属7共158项6.17秒；Mypy680、Ruff/format/diff通过。各组不累计冒充一次全量。原protocol/proof/0099及四个实验helper指纹保持，未变T证据复用，不宣称原实验矩阵已全部迁接正式Worker。

指定Chat同一请求已完整返回，response `ca6112ff-5d29-48fb-950b-a4424a5aa028`。指出selected final事务SQL失败后，在回滚释放锁但尚未返回期间，共享派发未及时停止的P1。真实PG双flight已RED复现；现prepare/final均接原dispatch_failure_boundary，在异步回滚前同步停止，正常Prepared在围栏内捕获不误停。最终正式双flight正反2项32.95秒通过：原SQL异常身份保持、兄弟无下一授权、第三股不补位、失败Job/Attempt仍running且无错误终态、原预算reserved及已提交阶段证据保留；正常对照9调用/3终态。fresh-context定向复核无新增P1/P2。最终受影响runtime9项278.35秒、Mypy680、Ruff/format/diff通过，满足该回复给出的本地有界收口条件；不是发布批准，也未另发全量评审。实际回复/包和精确会话元数据均在runtime私有目录。

本切片仍是本地候选，没有提交/推送/CI/合入部署或live0099迁移。下一切片把既有回归的版本与SDK边缘接点迁到正式路径后跑CI；`helpers/staged_worker`仍引用原worker导出，原normal夹具只mock `codex_staged.invoke_codex`不能拦截新的SDKhook，禁止直接运行成真实SDK请求，必须沿本次受控AsyncCodex边缘或显式legacy版本夹具迁接，不放宽产品门禁。仍欠完整同head必需CI、正式BFF/实际页面、最终镜像及非空阶段事实的兼容回滚镜像验证；空stage schema回退已验，存在事实禁止删除0099。

正常宿主经正式status复核仍stopped/maintenance。全部测试退出；精确自有55439容器零临时库、零其他client后已停止，保留卷/restart=no。未知费用、预算和旧结果不变；无真实模型/通知/来源调用、重放或未知接受释放。PRIMARY index、无关改动和原四helper已核对保留。
