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
