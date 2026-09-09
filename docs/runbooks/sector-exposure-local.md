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

运行：

```bash
uv run --project engine nexus-engine-sector-exposure-local \
  --repo-root /absolute/path/to/repo \
  --csv /absolute/private/path/reviewed-sector.csv
```

成功只返回导入标的数和行数，并保持 `execution_locked=true`。记录不可变；如果进程在
提交边界中断，先查询数据库，不要直接重放。新证据不会回填旧组合快照或旧方案，必须在
后续新的只读组合快照和方案草案中使用。
