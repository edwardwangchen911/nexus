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
  read_only: "只读", promote: "晋级", rollback: "回滚",
  success: "成功", error: "错误", dead_letter: "已停止重试", full: "完整",
  point_in_time: "按当时可用信息", unlocked: "未锁定", recovered: "已恢复", ok: "正常",
};

export function statusLabel(value: string | null | undefined): string {
  if (!value) return "未知";
  if (/[\u3400-\u9fff]/.test(value)) return value;
  return statuses[value.toLowerCase()] ?? `未识别状态（${value}）`;
}

const reasons: Record<string, string> = {
  ACTIVE_FAMILY_EXISTS: "已有实验组正在进行，请先查看当前实验",
  LINEAGE_AUTHORITY_INCOMPLETE: "缺少已核验的研究版本来源，暂不能生成策略实验",
  REGISTRY_AUTHORITY_INVALID: "策略登记记录未通过核验",
  EXPERIMENT_PROJECTION_INVALID: "实验记录未通过核验",
  PROVIDER_UNAVAILABLE: "数据源暂不可用",
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
  DRAWDOWN_252_COVERAGE_INSUFFICIENT: "252 个交易日窗口内有效价格覆盖不足",
  BETA_CORRELATION_126_COVERAGE_INSUFFICIENT: "126 个交易日窗口内有效价格覆盖不足",
  VOLATILITY_63_COVERAGE_INSUFFICIENT: "63 个交易日窗口内有效价格覆盖不足",
  VOLATILITY_20_COVERAGE_INSUFFICIENT: "20 个交易日窗口内有效价格覆盖不足",
  MARKET_EVIDENCE_UNAVAILABLE: "历史风险证据尚未齐备",
  FORMAL_DIAGNOSTIC_PENDING: "正式诊断尚未完成",
  CASH_FLOW_RECONCILIATION_PENDING: "现金流水尚未完成对账",
  FX_QUALIFIED_VIEW_MISSING: "缺少经过核验的汇率估值",
  DRAFT_FX_VALUATION_UNAVAILABLE: "缺少外币现金的汇率估值，暂不能生成全账户草稿",
  NO_SECURITIES: "没有可用于计算的证券持仓",
  RISK_BUCKET_MISSING: "缺少持仓风险分类",
  POSITION_PRICE_EVIDENCE_MISSING: "缺少持仓价格证据",
  COST_NOT_PROVEN: "交易成本尚未核实", TAX_DATA_UNAVAILABLE: "税务资料不可用",
  COST_MODEL_NOT_AVAILABLE: "交易成本模型尚未提供",
  COST_EVIDENCE_INCOMPLETE: "成本证据不完整",
  BROKER_COMMISSION_SCHEDULE_UNAVAILABLE: "缺少适用的券商佣金费率依据",
  BID_ASK_EVIDENCE_UNAVAILABLE: "缺少可用于情景成本分析的冻结买卖报价依据；现有盘中行情仅用于展示",
  SLIPPAGE_MODEL_UNAVAILABLE: "缺少适用的滑点模型依据",
  REGULATORY_FEE_SCHEDULE_UNAVAILABLE: "缺少适用的监管费率依据",
  BROKER_REGULATORY_FEE_PASS_THROUGH_UNAVAILABLE: "公开监管费率已核验，但缺少券商向账户转嫁、聚合与舍入依据",
  TURNOVER_BUDGET_EXHAUSTED: "换手预算已用尽",
  STRUCTURAL_CONCENTRATION_FLOOR: "当前持仓结构存在无法继续降低的集中度下限",
  SELLABLE_QUANTITY_EXHAUSTED: "可卖数量已用尽",
  SCENARIO_BASELINE_NOT_READY: "当前冻结快照尚不能作为情景基线",
  SCENARIO_DIAGNOSTIC_MISMATCH: "诊断与当前冻结快照不一致",
  SCENARIO_AS_OF_MISMATCH: "情景时点与冻结快照不一致",
  SCENARIO_OBJECTIVE_UNSUPPORTED: "当前情景目标不受支持",
  SCENARIO_INPUT_INVALID: "情景输入未通过校验",
  SCENARIO_US_EQUITY_SCOPE_INVALID: "当前组合缺少可用于 USD 证券情景的持仓",
  STRATEGY_EVIDENCE_INCOMPLETE: "策略研究证据不完整", SHADOW_DATA_INCOMPLETE: "前向观察数据不完整",
  VOLATILITY_REQUIRES_PRICE_HISTORY: "计算波动率需要历史价格",
  DRAWDOWN_REQUIRES_PRICE_HISTORY: "计算回撤需要历史价格",
  CORRELATION_REQUIRES_PRICE_HISTORY: "计算相关性需要历史价格",
  LIQUIDITY_REQUIRES_ADV20: "评估流动性需要近 20 个交易日的平均成交额",
  SECTOR_EXPOSURE_REQUIRES_IDENTITIES: "计算行业占比需要标的行业资料",
  CURRENCY_EXPOSURE_REQUIRES_IDENTITIES: "计算币种占比需要标的币种资料",
  staged_action_semantics_unverified: "角色表态尚不能确定投资方向或证据缺口是否解决",
  model_stage_allowance_exhausted: "研究阶段达到既定用量、调用次数或时间上限，未继续调用模型",
  model_usage_uncertain: "部分模型调用未返回用量",
  model_execution_failed: "模型执行未完成，具体原因尚未确认",
  model_stage_output_invalid: "研究阶段输出未通过内容校验",
  SHADOW_PRICE_AUTHORITY_UNAVAILABLE: "当前价格证据尚不能用于新的前向观察",
  conflicting_facts: "研究报告了尚未解决的事实矛盾",
  model_stage_review_invalid: "阶段回应引用不符合前序论点约定，研究已停止",
  ADV20_VOLUME_UNIT_UNVERIFIED: "成交量单位未确认，暂无法评估流动性",
  "filled at target-session open": "按目标交易日开盘价模拟成交",
};

export function reasonLabel(value: string): string {
  if (reasons[value]) return reasons[value];
  const market = qualityReasonLabel(value);
  if (market !== "存在未分类的数据质量问题") return market;
  return /[\u3400-\u9fff]/.test(value) ? value : "原因尚未归类";
}

export function reasonText(value: string | null | undefined): string {
  if (!value) return "原因未提供";
  const label = reasonLabel(value);
  return label === value ? value : `${label}（${value}）`;
}

export function jobTypeLabel(value: string): string {
  const labels: Record<string, string> = {
    "daily.pipeline": "每日数据与研究流程", daily_pipeline: "每日数据与研究流程",
    "broker.sync": "账户只读同步", "broker.reconcile": "账户对账",
    "broker.simulation.dispatch": "券商模拟订单发送", "broker.simulation.reconcile": "券商模拟订单对账",
    "collect.market": "行情采集", "collect.fosun_daily_instrument": "复星日线采集",
    "publish.fosun_daily": "复星日线汇总", "collect.sec_macro": "财报与宏观资料采集",
    "collect.intraday_snapshot": "盘中行情采集", "quality.freeze": "数据质量核验",
    "diagnostics.run": "组合诊断", "agent.research": "AI 研究生成",
    "optimization.propose": "优化方案生成", "paper.admit_orders": "内部模拟订单核验",
    "paper.execute_due": "内部模拟成交", "paper.performance": "内部模拟绩效计算",
    "shadow.advance_due": "方案前向观察", "daily.brief": "每日简报生成",
    "audit.manifest": "审计记录汇总", "strategy.rnd": "策略实验",
  };
  return labels[value] ?? "后台任务";
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

export function actorLabel(value: string): string {
  const labels: Record<string, string> = {
    user: "用户", system: "系统", service: "后台服务",
    task18: "模拟订单评估服务", task19: "模拟成交服务", reconciliation: "对账服务",
  };
  return labels[value] ?? `未识别操作方（${value}）`;
}

export function policyScopeLabel(value: string): string {
  if (value === "portfolio") return "组合范围";
  if (/^portfolio:[A-Z]{3}$/.test(value)) return `组合范围：${value.slice(10)}`;
  if (/^currency:[A-Z]{3}$/.test(value)) return `币种范围：${value.slice(9)}`;
  if (/^instrument:[0-9a-f-]{36}$/i.test(value)) return `标的范围：${value.slice(11)}`;
  return `未识别范围（${value}）`;
}

export function integrationNextActionLabel(value: string | null): string {
  const labels: Record<string, string> = {
    "A successful read-only connectivity check is required.": "需先通过只读连接检查",
    "A valid capability admission is required.": "需先完成有效的能力准入核验",
    "Read-only admission is required.": "需先完成只读准入核验",
  };
  if (value === null) return "无需操作";
  return labels[value] ?? (/[\u3400-\u9fff]/.test(value) ? value : `尚无中文说明（${value}）`);
}
