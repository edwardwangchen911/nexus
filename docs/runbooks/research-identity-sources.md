# SEC 研究身份证据采集

此入口仅归档 SEC 当前 ticker/exchange 目录和对应 issuer submissions，供后续研究身份绑定使用。
不连接业务数据库，不注册研究权威，不创建周期、模型任务或订单，也不修改已有身份原件。
`archived` 只证明本批来源取得且关联检查通过，不代表研究身份、决策或执行准入已完成。

从仓库根目录运行正常 CLI；受保护配置中的 `SEC_CONTACT` 由正常配置入口读取，不写入回执：

```sh
uv run --project engine python -m nexus.ops.research_identity_local \
  --repo-root "$PWD" --symbol NVDA --symbol AVGO --symbol AAPL \
  --output-dir '/absolute/private/new-leaf-directory'
```

父目录必须已存在、属于当前用户且权限为 0700；叶子目录必须不存在，入口创建为 0700。
所有归档及回执权限为 0600。目录存在时拒绝运行，应只读检查原结果，不更换目录重试。

一次目录请求供同批 1–5 个唯一 symbol 共享，每个 symbol 最多一次 submissions 请求。
所有请求固定 SEC HTTPS origin、禁止重定向且不重试，总窗口 60 秒；首个失败停止本批。
精确核对唯一 symbol/exchange/CIK 对应，不以字段分别存在代替关联；重复 JSON 字段拒绝。

成功归档包括 `directory.json`、`directory-receipt.json`、每只标的的
`<symbol>-submissions.json` 与 receipt，以及 `result.json`。回执保存原始字节 hash、长度、
来源 URL 和实际 received_at；received_at 不代表 SEC 文件生成或更新时间。
失败保留已取得的非敏感原件，result 只保存固定错误码、阶段和请求次数。

研究用途的 `research_current_identity_session_v1` 已经独立方案审阅：旧严格身份证明作为基础，
两份 SEC 当前来源须在原 Run decision_time 所属纽约日期取得，且全部依赖及引用在决策前可用。
Paper 另须核对正式 decision_session。该引用包装、冻结和 tool 接线尚未完成；本采集入口不会
自行应用该政策，更不会将旧来源或本次接收时间倒填到历史 Run。
