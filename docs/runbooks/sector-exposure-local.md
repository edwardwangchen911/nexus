# 本地受审核行业暴露导入

该入口只把已经人工核对的行业映射或发行人行业权重写入本地不可变
`sector_exposures`。它不联网、不调用模型，也不把 FMP profile、fixture 或未经核对的
供应商标签自动升级为风险权威。

CSV 列必须严格为：

```text
instrument_id,sector_code,weight,as_of,source_url,content_hash
```

- `sector_code` 只能使用 `NEXUS_SECTOR_V1` 的 11 个标签。
- `source_url` 必须是无凭据、无 fragment 的 HTTPS 地址。
- `content_hash` 是人工核对时所用来源原件的 SHA256；入口只核验格式和批次一致性，
  不会重新下载远端内容来替代人工核对。
- 普通股票必须且只能有一行、权重为 1；权益 ETF 可以有多行，合计必须在
  0.995–1.005。所有行必须对应已有 USD 权益 Instrument。

先为最新 READY 组合生成私有审核模板：

```bash
mkdir -m 700 /absolute/private/path/sector-review
uv run --project engine nexus-engine-sector-exposure-local \
  --repo-root /absolute/path/to/repo \
  --review-template /absolute/private/path/sector-review/review.json
```

模板只列出正数量、需要行业风险输入且在该冻结时点没有合格证据的标的，并给出允许的
行业代码和待填写导入行。文件以 `0600` 新建且不覆盖已有文件；终端只返回聚合数量，
不输出持仓代码或内部标识。模板是人工审核工作单，不是行业证据，也不能直接解除门禁。

Agent 内部模拟候选不依赖真实组合快照。为一个已冻结候选生成私有审核单时，同时提供其本地
Instrument ID 和周期 decision time；入口只读取该时点有效的唯一 ticker 与既有行业事实：

```bash
uv run --project engine nexus-engine-sector-exposure-local \
  --repo-root /absolute/path/to/repo \
  --review-template /absolute/private/path/sector-review/agent-review.json \
  --instrument-id "$NEXUS_AGENT_CANDIDATE_ID" \
  --decision-time "$NEXUS_AGENT_DECISION_TIME"
```

该模式最多输出一个缺口，不扩展股票池，也不把供应商分类提示自动升级为风险权威。

审核完成后，将 `import_row` 按上述严格列顺序整理为 CSV，再运行：

```bash
uv run --project engine nexus-engine-sector-exposure-local \
  --repo-root /absolute/path/to/repo \
  --csv /absolute/private/path/reviewed-sector.csv
```

成功只返回导入标的数和行数，并保持 `execution_locked=true`。记录不可变；如果进程在
提交边界中断，先查询数据库，不要直接重放。新证据不会回填旧组合快照或旧方案，必须在
后续新的只读组合快照和方案草案中使用。
