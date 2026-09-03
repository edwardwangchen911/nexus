# 手动快照诊断（日流程首片）

本入口只把一个明确的已持久化快照串为“日根 → 对账校验 → 正式诊断”。不采集券商、不调用模型、不创建方案、paper 或实盘订单，不启动调度。`completed` 只表示本次诊断流程完成，不等于研究/投资决策合格。

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

不修改旧 `daily.pipeline` schema，不启用旧同名研究/优化阶段。无新增依赖或迁移。停止使用新命令即可回滚使用方式，不删除历史 Job/诊断。现有长驻入口不是本命令，不能用“启动全部 worker”代替这次显式推进。

测试使用隔离 PostgreSQL 与合成快照：入口→原 worker→真实诊断持久化；验证未对账/未来事实/越界子节点、并行根隔离、未来可用时间立即返回。业务案例、模型研究接线、实际研究证据驱动方案仍是后续独立验收，不能以本片测试通过宣称整个 L1 完成。
