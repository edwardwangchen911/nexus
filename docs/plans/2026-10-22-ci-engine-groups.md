# Engine CI 分组提速

范围：保留当前合并测试、静态检查、迁移和手动全量恢复演练，在独立 runner 和 PostgreSQL 服务上分组执行。应用代码、研究并发、部署、账户权限不变。

## 依据和方案

成功运行 39730961885 的 Engine job 耗时 23分46秒，pytest 1307.72秒，安装依赖4秒。进度日志显示 PG 测试约19分钟；没有逐测试耗时，不据此承诺固定加速倍数。

沿用原74个选择器一次定义，分为 fast、pg-a、pg-b。前者包含原 unit/contract/evals/property/health；PG 从首个 candidate_multistock_path 的 v11 synthesis lifecycle 节点分界，粗粒度日志约各9至10分钟。各组保持串行 pytest、相同数据库变量、迁移、超时和标记规则，不采用进程内并发。

`engine-postgres` 保持同名，always 聚合全部 Engine 组，只有全部成功才能通过；fail-fast=false，保留所有失败证据。手动运行只执行 full 组，仍覆盖 performance 和隔离恢复演练。每组输出持续时间和合成测试 JUnit，方便核验后续失衡。

## 验证和集成

1. 测试先证明失败、取消、skip、空测试退出不能变绿，以及手动全量不丢 performance。
2. 独立收集旧命令与新三组全部 node ID，验证多重集合完全一致、没有重复。
3. 完整 policy 测试和相关 Ruff；集中审查差异。
4. 同一候选全部 GitHub CI 成功后安全合入；以实际 job 时长比较原基线。并发占用更多标准 runner，墙钟收益与总计算用量分别报告。

CI 配置可用普通 revert 回滚，不涉及数据库或运行服务更新。此次不修改远端分支保护：检查发现目前传统保护和 rulesets 均未配置，因此只声明保留工作流失败门，不声称服务器已强制禁止绕过合并。

研究并行另以已完成唯一真实三股事实送现有 ChatGPT 评审，本切片不实施，也不重复真实请求测速。
