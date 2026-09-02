# 真实证据链核查与阶段展示切片

核查日期：2026-09-02。实施基线 `ce523bf`，独立 worktree `593c`。

## 价格证据：真实样本，不修改准入

- 从既有复星日线选择一个样本，manifest `01a0602d-f0e6-752b-9a92-cc2420d856ee`。
- 原始对象 schema 为 `fosun_gateway_market_bars_v1`，读取本地对象存储并校验 SHA-256 一致。
  响应顶层为 items/source_request_id/has_more/next_cursor/as_of/quality，没有上游复权参数证明。
- 数据库为 277 行、277 个不同交易日、一个 revision；2025-07-28 至 2026-09-01。
  冻结日历预期 277 个交易日，缺失 0；来源 fosun、feed l1，申报 raw，delivery unknown。
- Gateway SDK 当前 `_bar_sdk_arguments` 显式发送 `right=noward`，但旧原始响应不能证明
  当时实际请求参数。不能通过修改旧标签或引用现在的代码使旧数据获得价格权威。
- Provider normalize_bars 将响应可用时间绑定到接收时刻；本地刷新设置固定 unknown，
  manifest 保留该值。`nexus_prepare_price_bar_set_v2` 仅接受 real_time/delayed；
  `nexus_prepare_price_bar_set_v2_at` 真实只读调用无合格结果。研究价格冻结也排除 unknown。
- 研究价格权威为 0，真实研究仍 partial/watch，正式方案与 Shadow 未创建。
  这是缺证据，不能据此认定不调整有充分理由，也不能把原始价格冒充公司行动调整后总收益。

## 本切片交付

- 既有 research GET 增加可选 `stage_trace`；只对实验 v2 读取已保存角色输出。
- 重新核对冻结证据、提示词、输出内容哈希、角色顺序、引用和前序阶段回应；异常记录不展示论点。
- 研究页展示每阶段保存情况、论点及引用、回应关系、已保存输出用量和同源证据摘要。
  失败调用的未知用量仍明确标注；未保存输出不等于该阶段未发生计费。
- 真实数据库与临时只读 API/BFF/页面交叉读取到同一旧失败记录，三阶段均 unavailable，
  总用量仍为 12,107 输入 / 904 输出，无新模型调用。没有重试、交易或调度。
- 临时页面沿用当前主目录进程的本地认证模式与现有 HMAC；API 只允许 GET/HEAD。
  未修改主目录服务、账户库结构、认证文件或部署配置。

## 验证与集中审查

- 41 项 Python 聚焦单测通过，覆盖损坏摘要/引用、跨运行数据、未来阶段回应、遗漏反方和部分失败。
- 独立 `55436` PostgreSQL、临时 `nexus_test_*` 数据库 7 项集成测试通过，模型调用全部 mock。
- 69 项研究组件/BFF 测试、TypeScript、Ruff、mypy、lint、前端 build 通过。
- 集中审查确认：只读 API 不登记任务；默认准入仍排除 v2；不修改最终意图、优化与执行条件。
- 无 schema、依赖或权限变更。回滚应用提交即可撤回展示；数据库历史保留。

## 未完成与下一步

持久目标仍 active。本切片不替代真实组合决策闭环。
继续补齐后续 K 线请求口径的原始留存；时效/复权/公司行动证据不足时仍拒绝准入。
新的三阶段真实模型验收已提出一次性预算请求，尚待明确批准，不因 Goal 登记而获得授权。
