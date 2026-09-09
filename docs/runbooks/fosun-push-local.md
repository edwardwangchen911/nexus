# 复星行情 Push 本地有界刷新

该入口只把一次完整的复星 WebSocket `qt` 归档导入本地展示价格。它不调度、不自动重连、不创建研究或订单，`delivery_mode=unknown` 且 `execution_locked=true`。当前 Engine authority 仅支持美股 `us...` 代码。

所有输出路径必须是已有 `0700` 私有目录中的新文件；命令不覆盖旧结果。凭据仍由现有受保护 Gateway launcher 注入，不放入参数、仓库或聊天。

## 一次手动闭环

1. 在打开 WebSocket 前冻结订阅权限和精确 ticker 身份：

```bash
uv run --package nexus-engine nexus-engine-fosun-push-local prepare \
  --repo-root "$PWD" \
  --code usAAPL \
  --output /absolute/private/run/authority.json
```

2. 把 prepare 输出的 `authority_sha256` 传给受保护 Gateway launcher，并让 probe 产生归档目录与规范化交接文件：

```bash
nexus-fosun-push-probe \
  --manifest /absolute/private/provider-manifest.json \
  --code usAAPL \
  --duration 20 \
  --max-frames 20 \
  --output-dir /absolute/private/run/archive \
  --authority-sha256 '<prepare stdout digest>' \
  --normalized-output /absolute/private/run/push-quote.json
```

3. 仅对 `bounded_complete` 结果导入。Engine 会重算 frame 哈希、价格、接收时间、权限摘要和标的身份，写入对象存储后回读，再于同一数据库事务写入 `DataArtifact` / `LatestQuote`：

```bash
uv run --package nexus-engine nexus-engine-fosun-push-local import \
  --repo-root "$PWD" \
  --authority /absolute/private/run/authority.json \
  --handoff /absolute/private/run/push-quote.json \
  --output /absolute/private/run/import-result.json
```

若 import 启动后被中断或返回 `FOSUN_PUSH_CHECK_PERSISTED_STATE`，先查对象存储、`data_artifacts` 和 `latest_quotes`；不要重放 Provider 调用。同一交接文件的 import 可幂等重跑，冲突内容会拒绝。
