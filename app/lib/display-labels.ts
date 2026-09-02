import { qualityReasonLabel } from "./market-evidence-labels";

const statuses: Record<string, string> = {
  offline_ready: "离线准备完成", read_only_activated: "只读访问已激活", simulation_write_activated: "模拟交易已激活",
  external_activation_blocked: "外部连接未激活", externally_activated: "外部连接已激活",
  conflict: "存在差异", acknowledged: "已确认", completed: "已完成",
  display_ready: "可以查看", display_unavailable: "暂无可展示数据", decision_ready: "快照已通过核验",
  reconciliation_pending: "等待对账", reconciliation_required: "需要重新对账", data_stale: "数据已过期",
  "human approval": "需逐笔人工确认",
  proposed: "已提议", risk_approved: "风控通过", risk_rejected: "风控拒绝", awaiting_approval: "等待批准",
  not_sent: "尚未发送", working: "等待成交", dispatching: "发送中", partially_completed: "部分完成",
  submission_unknown: "提交结果待核对", reconciling: "对账中", reconciled: "已对账",
  shadow_running: "前向观察中", effective: "已生效", available: "可用", verified: "已核验",
  genesis: "初始登记", pending_boundary: "等待下个生效时点", admitted: "已准入", backtest_passed: "回测已通过",
  ready: "已就绪", partial: "资料不完整", stale: "已过期", complete: "完整", incomplete: "不完整",
  pending: "待处理", queued: "排队中", running: "进行中", succeeded: "已完成", failed: "失败",
  draft: "草稿", compared: "已比较", shadowing: "观察中", validated: "已验证", rejected: "已拒绝", expired: "已过期",
  unknown: "未知", unavailable: "暂不可用", blocked: "受阻", disabled: "已禁用", enabled: "已启用",
  active: "已生效", paused: "已暂停", prepared: "待批准", approved: "已批准", submitted: "已提交",
  accepted: "已接受", filled: "已成交", partially_filled: "部分成交", canceled: "已撤销", cancelled: "已撤销",
  matched: "对账一致", mismatched: "对账有差异", locked: "已锁定", not_activated: "未激活",
  pass: "通过", fail: "未通过", offline_fixture: "离线示例", fixture: "离线示例", local_read_only: "本地只读",
  research_required: "需要补充证据", optimization_candidate: "值得比较方案", no_action: "保持现状",
  buy_candidate: "买入候选", reduce_candidate: "减仓候选", hold: "保持观察", watch: "仅观察",
  buy: "买入", sell: "卖出", limit: "限价单", market: "市价单", day: "当日有效", regular: "常规交易时段",
  hold_current: "保持现状", minimum_change: "最小调整", strategy_target: "策略目标",
};

export function statusLabel(value: string | null | undefined): string {
  if (!value) return "未知";
  if (/[\u3400-\u9fff]/.test(value)) return value;
  return statuses[value.toLowerCase()] ?? `未识别状态（${value}）`;
}

const reasons: Record<string, string> = {
  paper_partial_fill: "模拟订单部分成交", paper_fill: "模拟订单成交",
  HEADROOM_UNAVAILABLE: "缺少当前值或阈值，无法计算余量",
  HEADROOM_DIRECTION_UNKNOWN: "该指标未定义阈值方向，无法计算余量",
  LIQUIDITY_POST_PROPOSAL_NOT_PROVEN: "调整后的流动性尚未得到验证",
  PASS: "通过核验",
  VOLATILITY_POST_PROPOSAL_NOT_PROVEN: "调整后的波动率尚未得到验证",
  DRAWDOWN_POST_PROPOSAL_NOT_PROVEN: "调整后的回撤尚未得到验证",
  CORRELATION_POST_PROPOSAL_NOT_PROVEN: "调整后的相关性尚未得到验证",
  ADV20_MISSING: "缺少近 20 个交易日的平均成交额",
  PRICE_HISTORY_MISSING: "缺少历史价格",
  DRAWDOWN_252_COVERAGE_INSUFFICIENT: "不足 252 个交易日，无法可靠计算历史回撤",
  BETA_CORRELATION_126_COVERAGE_INSUFFICIENT: "相关性计算所需的 126 个交易日数据不足",
  MARKET_EVIDENCE_UNAVAILABLE: "历史风险证据尚未齐备",
  FORMAL_DIAGNOSTIC_PENDING: "正式诊断尚未完成",
  CASH_FLOW_RECONCILIATION_PENDING: "现金流水尚未完成对账",
  FX_QUALIFIED_VIEW_MISSING: "缺少经过核验的汇率估值",
  DRAFT_FX_VALUATION_UNAVAILABLE: "缺少外币现金的汇率估值，暂不能生成全账户草稿",
  NO_SECURITIES: "没有可用于计算的证券持仓",
  RISK_BUCKET_MISSING: "缺少持仓风险分类",
  POSITION_PRICE_EVIDENCE_MISSING: "缺少持仓价格证据",
  COST_NOT_PROVEN: "交易成本尚未核实", TAX_DATA_UNAVAILABLE: "税务资料不可用",
  STRATEGY_EVIDENCE_INCOMPLETE: "策略研究证据不完整", SHADOW_DATA_INCOMPLETE: "前向观察数据不完整",
  VOLATILITY_REQUIRES_PRICE_HISTORY: "计算波动率需要历史价格",
  DRAWDOWN_REQUIRES_PRICE_HISTORY: "计算回撤需要历史价格",
  CORRELATION_REQUIRES_PRICE_HISTORY: "计算相关性需要历史价格",
  LIQUIDITY_REQUIRES_ADV20: "评估流动性需要近 20 个交易日的平均成交额",
  SECTOR_EXPOSURE_REQUIRES_IDENTITIES: "计算行业占比需要标的行业资料",
  CURRENCY_EXPOSURE_REQUIRES_IDENTITIES: "计算币种占比需要标的币种资料",
  model_usage_uncertain: "部分模型调用未返回用量",
  "filled at target-session open": "按目标交易日开盘价模拟成交",
};

export function reasonLabel(value: string): string {
  if (reasons[value]) return reasons[value];
  const market = qualityReasonLabel(value);
  if (market !== "存在未分类的数据质量问题") return market;
  return /[\u3400-\u9fff]/.test(value) ? value : "尚无中文说明";
}

export function reasonText(value: string | null | undefined): string {
  if (!value) return "原因未提供";
  const label = reasonLabel(value);
  return label === value ? value : `${label}（${value}）`;
}

export const metricLabels: Record<string, string> = {
  top1: "最大单项权重", top3: "前三项权重", hhi: "持仓集中度指数（HHI）",
  sector_exposure: "最大行业占比", currency_exposure: "最大币种占比", volatility: "历史波动率",
  drawdown: "回撤", correlation: "相关系数", liquidity: "流动性", total_weight: "总权重",
  unallocated_cash: "未分配现金占比", single_name_warning: "单一持仓预警数",
  single_name_critical: "单一持仓严重超限数", top3_warning: "前三大持仓超限数",
  cash_minimum: "最低现金占比不达标数", violation_count: "约束不达标总数",
};

export function operatorLabel(value: string): string {
  return ({ lt: "小于", lte: "不高于", gt: "大于", gte: "不低于", eq: "等于", neq: "不等于" } as Record<string, string>)[value] ?? value;
}

export function signalLabel(value: string): string {
  return ({ revenue_growth_ttm: "近 12 个月收入增长率", close: "收盘价", drawdown: "回撤", volatility: "波动率", position_weight: "持仓权重", cash_ratio: "现金占比" } as Record<string, string>)[value] ?? value;
}
