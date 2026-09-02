const QUALITY_REASON_LABELS: Readonly<Record<string, string>> = {
  SOURCE_CONFLICT: "来源数据相互冲突",
  SOURCE_POLICY_UNAVAILABLE: "行情来源策略不可用",
  NO_ELIGIBLE_SOURCE: "没有通过质量门禁的行情主源",
  QUOTE_UNAVAILABLE: "报价当前不可用",
  SELECTED_SOURCE_STALE: "已选主源数据已过期",
  QUOTE_STALE: "报价已过期",
  QUOTE_PRICE_UNAVAILABLE: "报价价格不可用",
  QUOTE_QUALITY_FLAGS: "报价包含质量标记",
  TICK_SIZE_UNKNOWN: "最小报价单位未知",
  NO_HISTORY_DATA: "历史行情当前不可用",
  NO_DAILY_HISTORY_DATA: "日线历史当前不可用",
  HISTORY_INSTRUMENT_MISMATCH: "历史行情与所选标的不一致",
  HISTORY_TRUNCATED: "历史行情已按页面上限截断",
  HISTORY_DELIVERY_UNVERIFIED: "K 线实时性尚未获得证明",
  FUTURE_DATA_EXCLUDED: "未来时点数据已排除",
  FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE: "上游报价未提供成交时间",
  FOSUN_ADJUSTMENT_REQUEST_UNVERIFIED: "未保存 SDK 不复权请求证明",
  INTRADAY_BID_ASK_UNAVAILABLE: "盘口买卖价未提供",
  INTRADAY_DELIVERY_UNVERIFIED: "实时性尚未获得证据",
  INTRADAY_SOURCE_DIVERGENCE: "盘中来源数据存在偏差",
  INTRADAY_SOURCE_CONFLICT: "盘中来源数据相互冲突",
  INTRADAY_SOURCE_POLICY_UNAVAILABLE: "盘中来源策略不可用",
  INTRADAY_AUTHORITY_INVALID: "盘中数据权威校验失败",
  INTRADAY_OUTSIDE_SESSION: "当前不在正常交易时段",
  INTRADAY_RETURN_5M_INCOMPLETE: "5 分钟变化所需样本不完整",
  INTRADAY_RETURN_30M_INCOMPLETE: "30 分钟变化所需样本不完整",
  INTRADAY_RANGE_30M_INCOMPLETE: "30 分钟振幅所需样本不完整",
  INTRADAY_QUOTE_UNAVAILABLE: "盘口报价不可用",
  INTRADAY_QUOTE_NOT_VISIBLE: "盘口报价在当前证据时点不可见",
  INTRADAY_QUOTE_QUALITY_FLAGS: "盘口报价包含质量标记",
  INTRADAY_QUOTE_STALE: "盘口报价已过期",
  INTRADAY_TRADE_STALE: "最新价证据已过期",
  INTRADAY_BAR_STALE: "分钟线证据已过期",
  INTRADAY_INSTRUMENT_MISMATCH: "盘中证据与所选标的不一致",
  INTRADAY_OBSERVED_AT_MISSING: "分钟线缺少观测时间",
  INTRADAY_BAR_NOT_VISIBLE: "分钟线在当前证据时点不可见",
  INTRADAY_BAR_INCOMPLETE: "分钟线尚未完整收盘",
  INTRADAY_VOLUME_INVALID: "分钟成交量无效",
  INTRADAY_BAR_QUALITY_FLAGS: "分钟线包含质量标记",
  INTRADAY_BAR_OUTSIDE_SESSION: "分钟线不在正常交易时段",
  INTRADAY_TIMESTAMP_DUPLICATE: "分钟线时间戳重复",
  INTRADAY_WINDOW_GAP: "分钟线窗口存在缺口",
  INTRADAY_DATA_UNAVAILABLE: "盘中数据当前不可用",
  INSUFFICIENT_CONTIGUOUS_BARS: "连续分钟样本不足",
};

export function marketSourceLabel(source: string | null, feed: string | null): string {
  if (!source || !feed) return "来源未选定";
  if (source === "fosun" && feed === "l1") return "复星 L1（只读）";
  return `${source.toUpperCase()} · ${feed.toUpperCase()}`;
}

export function marketDeliveryModeLabel(mode: string | null): string {
  if (mode === "real_time") return "实时";
  if (mode === "delayed") return "延迟（具体时长未声明）";
  if (mode === "fixture") return "离线示例";
  if (mode === "not_applicable") return "不适用";
  if (mode === null) return "实时性不可用";
  return "实时性未获证明";
}

export function marketAdjustmentLabel(adjustment: string | null): string {
  if (adjustment === "raw") return "未复权（raw）";
  if (adjustment === "all") return "全复权（all）";
  if (adjustment === "split") return "拆股复权（split）";
  if (adjustment === "dividend") return "分红复权（dividend）";
  if (adjustment === null) return "未选定";
  return `未知口径（${adjustment}）`;
}

export function qualityReasonLabel(reason: string): string {
  return QUALITY_REASON_LABELS[reason] ?? "存在未分类的数据质量问题";
}
