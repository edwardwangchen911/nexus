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
