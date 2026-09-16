"use client";

import { marketDeliveryModeLabel } from "../../lib/market-evidence-labels";

import { statusLabel } from "../../lib/display-labels";

import { formatPercent } from "../../lib/display-format";

import { AlertTriangle, Check, RefreshCw, ShieldCheck } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import { AgentPaperCycle, AgentPaperSessionSummary, AgentPaperVersionObservation, type AgentPaperCycleFact, type AgentPaperSessionSummaryFact, type AgentPaperVersionObservationFact } from "../../components/agent-paper-cycle";
import { DecisionPath } from "../../components/decision-path";
import { formatPaperDecimal, formatPaperTime, OrderTimeline, type PaperOrder } from "../../components/order-timeline";
import type { components } from "../../lib/generated/schema";

export type PaperPortfolio = components["schemas"]["PaperPortfolioResponse"];
type OrderStatus = PaperOrder["status"];
type Notice = { kind: "success" | "error"; text: string } | null;
type PendingCancel = { order: PaperOrder; idempotencyKey: string; unknown: boolean };
type PaperPanelProps = {
  state: DataStateKind;
  portfolio: PaperPortfolio | null;
  orders: PaperOrder[];
  nextCursor: string | null;
  statusFilter: "" | OrderStatus;
  loadingMore: boolean;
  notice: Notice;
  cycles?: AgentPaperCycleFact[];
  cycleSummary?: AgentPaperSessionSummaryFact | null;
  versionObservation?: AgentPaperVersionObservationFact | null;
  cycleUnavailable?: boolean;
  onRefresh: () => void;
  onStatusFilter: (status: "" | OrderStatus) => void;
  onLoadMore: () => void;
  onCancel: (order: PaperOrder, trigger?: HTMLButtonElement) => void;
};

const LOAD_TIMEOUT_MS = 10_000;
const CANCEL_TIMEOUT_MS = 30_000;
const ORDER_PAGE_SIZE = 20;
const MAX_ORDERS = 500;
const statusOptions: Array<["" | OrderStatus, string]> = [
  ["", "全部状态"], ["proposed", "已提议"], ["risk_rejected", "风控拒绝"],
  ["risk_approved", "风控通过"], ["awaiting_approval", "等待批准"],
  ["submitted", "已提交模拟盘"], ["partially_filled", "部分成交"],
  ["filled", "已成交"], ["cancelled", "已取消"], ["rejected", "已拒绝"],
  ["reconciling", "对账中"],
];

export default function PaperPage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [portfolio, setPortfolio] = useState<PaperPortfolio | null>(null);
  const [orders, setOrders] = useState<PaperOrder[]>([]);
  const [cycles, setCycles] = useState<AgentPaperCycleFact[]>([]);
  const [cycleSummary, setCycleSummary] = useState<AgentPaperSessionSummaryFact | null>(null);
  const [versionObservation, setVersionObservation] = useState<AgentPaperVersionObservationFact | null>(null);
  const [cycleUnavailable, setCycleUnavailable] = useState(false);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<"" | OrderStatus>("");
  const [loadingMore, setLoadingMore] = useState(false);
  const [notice, setNotice] = useState<Notice>(null);
  const [pendingCancel, setPendingCancel] = useState<PendingCancel | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const loadVersion = useRef(0);
  const orderVersion = useRef(0);
  const portfolioRef = useRef<PaperPortfolio | null>(null);
  const ordersRef = useRef<PaperOrder[]>([]);
  const filterRef = useRef<"" | OrderStatus>("");
  const cursorHistory = useRef(new Set<string>());
  const cancelInFlight = useRef(false);
  const returnFocus = useRef<HTMLElement | null>(null);

  const clearFacts = useCallback((): void => {
    portfolioRef.current = null;
    ordersRef.current = [];
    setPortfolio(null);
    setOrders([]);
    setCycles([]);
    setCycleSummary(null);
    setVersionObservation(null);
    setCycleUnavailable(false);
    setNextCursor(null);
  }, []);

  const refresh = useCallback(async (): Promise<boolean> => {
    const version = loadVersion.current + 1;
    loadVersion.current = version;
    orderVersion.current += 1;
    setNotice(null);
    if (!portfolioRef.current) setState("loading");
    try {
      const filter = filterRef.current;
      const [portfolioResponse, orderResponse, cycleResponse] = await Promise.all([
        fetchWithTimeout("/api/v1/paper/portfolio"),
        fetchWithTimeout(orderPath(filter, null)),
        fetchWithTimeout("/api/v1/paper/cycles?limit=20").catch(() => new Response(null, { status: 502 })),
      ]);
      if (version !== loadVersion.current) return false;
      if ([401, 403].includes(portfolioResponse.status) || [401, 403].includes(orderResponse.status)) {
        clearFacts(); setState("forbidden"); return false;
      }
      if (portfolioResponse.status === 404) { clearFacts(); setState("empty"); return true; }
      if (!portfolioResponse.ok) {
        setState(portfolioRef.current ? "stale" : portfolioResponse.status === 409 || portfolioResponse.status === 422 ? "partial" : "error");
        setNotice({ kind: "error", text: "无法读取模拟账户权威事实；未触发任何订单动作。" });
        return false;
      }
      const portfolioPayload = await portfolioResponse.json() as { data?: PaperPortfolio };
      const nextPortfolio = validPortfolio(portfolioPayload.data);
      if (!nextPortfolio) throw new Error("paper portfolio invalid");
      portfolioRef.current = nextPortfolio; setPortfolio(nextPortfolio);
      if (!orderResponse.ok) {
        ordersRef.current = []; setOrders([]); setNextCursor(null);
        setState(nextPortfolio.data_state === "STALE" ? "stale" : "partial");
        setNotice({ kind: "error", text: "组合事实可用，但订单事实读取失败；取消操作保持关闭。" });
        return false;
      }
      const orderPayload = await orderResponse.json() as { data?: { items?: PaperOrder[]; next_cursor?: string | null } };
      const page = validOrderPage(orderPayload.data);
      if (!page) throw new Error("paper orders invalid");
      ordersRef.current = page.items; setOrders(page.items); setNextCursor(page.next_cursor);
      cursorHistory.current = new Set(page.next_cursor ? [page.next_cursor] : []);
      if (cycleResponse.ok) {
        const cyclePayload = await cycleResponse.json() as { data?: { items?: AgentPaperCycleFact[]; summary?: AgentPaperSessionSummaryFact; version_observation?: AgentPaperVersionObservationFact | null } };
        const nextCycles = validCyclePage(cyclePayload.data);
        if (!nextCycles) throw new Error("paper cycles invalid");
        setCycles(nextCycles.items); setCycleSummary(nextCycles.summary); setCycleUnavailable(false);
        setVersionObservation(nextCycles.version_observation);
      } else {
        setCycles([]); setCycleSummary(null); setVersionObservation(null); setCycleUnavailable(true);
      }
      setState(portfolioState(nextPortfolio.data_state));
      return true;
    } catch (error) {
      if (version !== loadVersion.current) return false;
      setState(portfolioRef.current ? "stale" : "error");
      setNotice({ kind: "error", text: error instanceof DOMException && error.name === "AbortError"
        ? "模拟盘读取超时；保留最后已知事实，不触发任何订单动作。"
        : "模拟盘事实读取失败；未触发任何订单动作。" });
      return false;
    }
  }, [clearFacts]);

  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [refresh]);

  async function changeStatusFilter(value: "" | OrderStatus): Promise<void> {
    filterRef.current = value; setStatusFilter(value);
    const version = orderVersion.current + 1; orderVersion.current = version;
    setLoadingMore(true); setNotice(null);
    try {
      const response = await fetchWithTimeout(orderPath(value, null));
      if (version !== orderVersion.current) return;
      if ([401, 403].includes(response.status)) { clearFacts(); setState("forbidden"); return; }
      if (!response.ok) throw new Error("paper order filter failed");
      const payload = await response.json() as { data?: { items?: PaperOrder[]; next_cursor?: string | null } };
      const page = validOrderPage(payload.data);
      if (!page) throw new Error("paper order page invalid");
      ordersRef.current = page.items; setOrders(page.items); setNextCursor(page.next_cursor);
      cursorHistory.current = new Set(page.next_cursor ? [page.next_cursor] : []);
      if (portfolioRef.current) setState(portfolioState(portfolioRef.current.data_state));
    } catch {
      if (version === orderVersion.current) {
        setState(portfolioRef.current ? "partial" : "error");
        setNotice({ kind: "error", text: "无法应用订单筛选；当前列表不执行取消操作。" });
      }
    } finally {
      if (version === orderVersion.current) setLoadingMore(false);
    }
  }

  async function loadMoreOrders(): Promise<void> {
    const cursor = nextCursor;
    if (!cursor || loadingMore || ordersRef.current.length >= MAX_ORDERS) return;
    const version = orderVersion.current;
    setLoadingMore(true); setNotice(null);
    try {
      const response = await fetchWithTimeout(orderPath(filterRef.current, cursor));
      if (version !== orderVersion.current) return;
      if ([401, 403].includes(response.status)) { clearFacts(); setState("forbidden"); return; }
      if (!response.ok) throw new Error("paper order page failed");
      const payload = await response.json() as { data?: { items?: PaperOrder[]; next_cursor?: string | null } };
      const page = validOrderPage(payload.data);
      if (!page || ordersRef.current.length + page.items.length > MAX_ORDERS) throw new Error("paper order page invalid");
      const ids = new Set(ordersRef.current.map((order) => order.order_id));
      if (page.items.some((order) => ids.has(order.order_id))) throw new Error("duplicate paper order");
      if (page.next_cursor && cursorHistory.current.has(page.next_cursor)) throw new Error("paper cursor cycle");
      const next = [...ordersRef.current, ...page.items];
      ordersRef.current = next; setOrders(next); setNextCursor(page.next_cursor);
      if (page.next_cursor) cursorHistory.current.add(page.next_cursor);
    } catch {
      setNotice({ kind: "error", text: "无法加载更多订单；已保留当前有界列表。" });
    } finally { setLoadingMore(false); }
  }

  function openCancel(order: PaperOrder, trigger?: HTMLButtonElement): void {
    if (!order.cancellable || cancelInFlight.current) return;
    returnFocus.current = trigger ?? (document.activeElement instanceof HTMLElement ? document.activeElement : null);
    setPendingCancel({ order, idempotencyKey: `paper-cancel-${crypto.randomUUID()}`, unknown: false });
  }

  function closeCancel(): void {
    if (cancelInFlight.current) return;
    setPendingCancel(null);
    const target = returnFocus.current;
    globalThis.setTimeout(() => target?.focus(), 0);
  }

  async function confirmCancel(): Promise<void> {
    const pending = pendingCancel;
    if (!pending || cancelInFlight.current) return;
    cancelInFlight.current = true; setCancelling(true); setNotice(null);
    try {
      const response = await fetch(`/api/v1/paper/orders/${encodeURIComponent(pending.order.order_id)}/cancel`, {
        method: "POST",
        headers: { "content-type": "application/json", "idempotency-key": pending.idempotencyKey },
        body: JSON.stringify({ expected_event_seq: pending.order.current_event_seq }),
        signal: AbortSignal.timeout(CANCEL_TIMEOUT_MS),
      });
      let payload: { data?: { order_id?: string; status?: string; result_event_seq?: number }; error?: { code?: string } } = {};
      try { payload = await response.json() as typeof payload; } catch { /* handled as an unknown result below */ }
      if (response.ok && payload.data?.order_id === pending.order.order_id
        && payload.data.status === "cancelled"
        && typeof payload.data.result_event_seq === "number"
        && payload.data.result_event_seq > pending.order.current_event_seq) {
        setPendingCancel(null);
        const refreshed = await refresh();
        setNotice(refreshed
          ? { kind: "success", text: "模拟订单已取消；已重新读取服务端事实。" }
          : { kind: "error", text: "模拟订单已取消，但最新事实重读失败；取消操作保持关闭。" });
        return;
      }
      if (response.status >= 500 || response.ok) {
        setPendingCancel({ ...pending, unknown: true });
        setNotice({ kind: "error", text: "取消结果未知。该订单保持锁定；请继续确认原操作结果。" });
        return;
      }
      if (response.status === 409) {
        setPendingCancel(null);
        const refreshed = await refresh();
        setNotice({ kind: "error", text: refreshed
          ? payload.error?.code === "IDEMPOTENCY_CONFLICT"
            ? "原取消请求与另一操作冲突；已重新读取订单记录。"
            : "订单事件版本已变化；已重新读取订单事实。"
          : "订单状态已变化，且最新事实重读失败；取消操作保持关闭。" });
        return;
      }
      setPendingCancel(null);
      setNotice({ kind: "error", text: "模拟订单未取消；请刷新订单事实后重试。" });
    } catch {
      setPendingCancel({ ...pending, unknown: true });
      setNotice({ kind: "error", text: "取消结果未知。该订单保持锁定；请继续确认原操作结果。" });
    } finally {
      cancelInFlight.current = false; setCancelling(false);
    }
  }

  return <>
    <PaperPanel state={state} portfolio={portfolio} orders={orders} nextCursor={nextCursor}
      cycles={cycles} cycleUnavailable={cycleUnavailable}
      cycleSummary={cycleSummary}
      versionObservation={versionObservation}
      statusFilter={statusFilter} loadingMore={loadingMore} notice={notice}
      onRefresh={() => void refresh()} onStatusFilter={(value) => void changeStatusFilter(value)}
      onLoadMore={() => void loadMoreOrders()} onCancel={openCancel} />
    {pendingCancel ? <PaperCancelDialog pending={pendingCancel} submitting={cancelling}
      onDismiss={closeCancel} onConfirm={() => void confirmCancel()} /> : null}
  </>;
}

export function PaperPanel({ state, portfolio, orders, nextCursor, statusFilter, loadingMore, notice, cycles = [], cycleSummary = null, versionObservation = null, cycleUnavailable = false, onRefresh, onStatusFilter, onLoadMore, onCancel }: PaperPanelProps): React.ReactElement {
  const hasFacts = portfolio !== null && portfolio.performance !== null && ["ready", "partial", "stale"].includes(state);
  return (
    <section className="page-stack paper-page" aria-label="内部模拟盘控制台" data-state={state}>
      <header className="page-heading">
        <div><p className="eyebrow">内部模拟 · 美元现金账户</p><h1>内部模拟盘</h1><p>先看账户绩效和风险暴露，再核对持仓、归因与订单账簿。这里不是实盘。</p></div>
        <div className="page-actions"><span className="state-chip" data-state={state}>{dataStateLabels[state]}</span><button className="button button--quiet" type="button" onClick={onRefresh}><RefreshCw aria-hidden="true" size={17} />刷新事实</button></div>
      </header>
      <DecisionPath current="paper" />
      <div role="status" aria-label="模拟盘操作状态" aria-live="polite" aria-atomic="true">
        {notice ? <p className={`inline-notice inline-notice--${notice.kind}`}>{notice.text}</p> : null}
      </div>
      {!hasFacts ? <DataState state={state} title="内部模拟账户" detail={paperStateDetail(state)} announce={false} /> : <>
        <section className="paper-boundary" aria-label="模拟账户边界">
          <ShieldCheck aria-hidden="true" size={22} />
          <div><p className="eyebrow">模拟账户权威</p><h2>{portfolio.name}</h2><p>初始资金 <strong className="numeric-data">USD {formatPaperDecimal(portfolio.initial_capital, 2)}</strong> · 仅内部模拟成交 · 实盘能力保持关闭</p></div>
          <span className="mode-badge">不是实盘</span>
        </section>
        {cycleSummary ? <AgentPaperSessionSummary summary={cycleSummary} /> : null}
        {versionObservation ? <AgentPaperVersionObservation observation={versionObservation} /> : null}
        <AgentPaperCycle cycles={cycles} unavailable={cycleUnavailable} />
        {state === "partial" || state === "stale" ? <p className="paper-state-warning"><AlertTriangle aria-hidden="true" size={18} />{paperStateDetail(state)}</p> : null}
        <PerformanceFacts portfolio={portfolio} />
        {portfolio.comparison ? <PaperComparison comparison={portfolio.comparison} /> : null}
        <Positions portfolio={portfolio} />
        <Attributions portfolio={portfolio} />
        <section className="paper-orders" aria-labelledby="paper-orders-title">
          <header className="paper-orders__heading"><div><p className="eyebrow">已保存的订单记录</p><h2 id="paper-orders-title">订单与模拟成交</h2><p>订单状态与可取消性以已保存的最新记录为准。</p></div><label className="paper-orders__filter">订单状态筛选<select value={statusFilter} onChange={(event) => onStatusFilter(event.target.value as "" | OrderStatus)} disabled={loadingMore}>{statusOptions.map(([value, label]) => <option key={value || "all"} value={value}>{label}</option>)}</select></label></header>
          {orders.length === 0 ? <p className="paper-orders__empty">当前筛选下没有订单事实。</p> : <div className="paper-order-list">{orders.map((order) => <OrderTimeline key={order.order_id} order={order} allowCancel={state === "ready" && !loadingMore} onCancel={onCancel} />)}</div>}
          {nextCursor ? <button className="button button--quiet paper-orders__more" type="button" onClick={onLoadMore} disabled={loadingMore}>{loadingMore ? "正在加载" : "加载更多订单"}</button> : null}
        </section>
      </>}
    </section>
  );
}

function PaperComparison({ comparison }: { comparison: NonNullable<PaperPortfolio["comparison"]> }): React.ReactElement {
  const labels: Record<(typeof comparison.rows)[number]["key"], string> = {
    agent: "Agent",
    hold_current: "保持不动（现金）",
    baseline_v1: "baseline_v1",
    market_benchmark: "市场基准（SPY）",
  };
  const stateLabels: Record<(typeof comparison.rows)[number]["data_state"], string> = {
    READY: "完整",
    PARTIAL: "资料不完整",
    STALE: "已过期",
    UNAVAILABLE: "缺少同窗前向事实",
  };
  // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- The wide comparison table needs keyboard focus for horizontal scrolling.
  return <section className="paper-table-section paper-comparison" aria-labelledby="paper-comparison-title"><header><div><p className="eyebrow">同起点 · 同一观察窗</p><h2 id="paper-comparison-title">连续模拟对照</h2></div><p>{formatPaperTime(comparison.window_start)} 至 {formatPaperTime(comparison.as_of)}</p></header><p className="paper-comparison__note">只比较同窗持久化事实；历史回测不会填入当前前向结果。</p><div className="table-scroll" role="region" aria-label="连续模拟四方对照表，可横向滚动" tabIndex={0}><table className="paper-comparison-table numeric-data" aria-label="连续模拟四方对照"><caption>Agent、保持不动、固定策略与市场基准的统一口径。</caption><thead><tr><th scope="col">对照</th><th scope="col">数据完整性</th><th scope="col">净值</th><th scope="col">累计收益</th><th scope="col">最大回撤</th><th scope="col">换手</th><th scope="col">累计成本</th></tr></thead><tbody>{comparison.rows.map((row) => <tr key={row.key} data-state={row.data_state.toLowerCase()}><th scope="row">{labels[row.key]}</th><td><span className="paper-comparison__state">{row.reason_code === "MIXED_STRATEGY_FACTS_UNAVAILABLE" ? "账户含非 Agent 成交" : stateLabels[row.data_state]}</span></td><td>{row.nav === null ? "—" : `USD ${formatPaperDecimal(row.nav, 2)}`}</td><td>{row.total_return_ratio === null ? "—" : formatPercent(row.total_return_ratio)}</td><td>{row.max_drawdown_ratio === null ? "—" : formatPercent(row.max_drawdown_ratio)}</td><td>{row.turnover_ratio === null ? "—" : formatPercent(row.turnover_ratio)}</td><td>{row.total_cost === null ? "—" : `USD ${formatPaperDecimal(row.total_cost, 2)}`}</td></tr>)}</tbody></table></div></section>;
}

function PerformanceFacts({ portfolio }: { portfolio: PaperPortfolio }): React.ReactElement {
  const performance = portfolio.performance!;
  return <section className="paper-performance" aria-labelledby="paper-performance-title"><header><div><p className="eyebrow">冻结绩效事实</p><h2 id="paper-performance-title">账户绩效</h2></div><p className="numeric-data">截至 {formatPaperTime(performance.as_of)}</p></header><dl aria-label="模拟账户绩效事实">
    <MoneyFact term="账户净资产（NAV）" value={performance.nav} />
    <MoneyFact term="可用现金" value={performance.available_cash} />
    <MoneyFact term="预留现金" value={performance.reserved_cash} />
    <MoneyFact term="持仓市值" value={performance.market_value} />
    <MoneyFact term="已实现损益" value={performance.realized_pnl} signed />
    <MoneyFact term="未实现损益" value={performance.unrealized_pnl} signed />
    <MoneyFact term="累计费用" value={performance.fees} />
    <div><dt title="当前净资产相对历史净资产峰值的回撤，不是历史最大回撤">当前回撤</dt><dd className="numeric-data">{formatPercent(performance.drawdown_ratio)}</dd></div>
  </dl></section>;
}

function Positions({ portfolio }: { portfolio: PaperPortfolio }): React.ReactElement {
  // Bounded horizontal data regions must remain keyboard-scrollable on narrow screens.
  // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
  return <section className="paper-table-section" aria-labelledby="paper-positions-title"><header><div><p className="eyebrow">持仓 · 准入收盘价</p><h2 id="paper-positions-title">持仓</h2></div><p>参考价格采用已核验的收盘价，并非实时价格。</p></header>{portfolio.positions.length === 0 ? <p className="paper-orders__empty">当前没有模拟持仓。</p> : <div className="table-scroll" role="region" aria-label="内部模拟持仓表，可横向滚动" tabIndex={0}><table className="paper-position-table numeric-data" aria-label="内部模拟持仓"><caption>数量、成本、市值与价格时点来自同一份已保存快照。</caption><thead><tr><th scope="col">标的</th><th scope="col">数量</th><th scope="col">可用 / 预留</th><th scope="col">平均成本</th><th scope="col">参考收盘</th><th scope="col">市值</th><th scope="col">未实现损益</th><th scope="col">来源 / 数据通道</th><th scope="col">交付</th><th scope="col">价格时点</th></tr></thead><tbody>{portfolio.positions.map((position) => <tr key={position.instrument_id}><th scope="row">{position.symbol ?? "未知标的"}</th><td>{formatPaperDecimal(position.quantity)}</td><td>{formatPaperDecimal(position.available_quantity)} / {formatPaperDecimal(position.reserved_quantity)}</td><td>USD {formatPaperDecimal(position.average_cost, 2)}</td><td>USD {formatPaperDecimal(position.reference_close, 2)}</td><td>USD {formatPaperDecimal(position.market_value, 2)}</td><td>USD {formatPaperDecimal(position.unrealized_pnl, 2)}</td><td>{position.price_source} / {position.price_feed}</td><td>{marketDeliveryModeLabel(position.delivery_mode)}</td><td>{formatPaperTime(position.price_as_of)}</td></tr>)}</tbody></table></div>}</section>;
}

function Attributions({ portfolio }: { portfolio: PaperPortfolio }): React.ReactElement {
  // Bounded horizontal data regions must remain keyboard-scrollable on narrow screens.
  // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
  return <section className="paper-table-section" aria-labelledby="paper-attribution-title"><header><div><p className="eyebrow">持久化绩效归因</p><h2 id="paper-attribution-title">绩效归因</h2></div><p>行业证据缺失时标记为资料不完整，不猜测分类。</p></header>{portfolio.attributions.length === 0 ? <p className="paper-orders__empty">尚无归因事实。</p> : <div className="table-scroll" role="region" aria-label="内部模拟绩效归因表，可横向滚动" tabIndex={0}><table className="paper-attribution-table numeric-data" aria-label="内部模拟绩效归因"><caption>现金、标的与行业维度的冻结归因。</caption><thead><tr><th scope="col">维度</th><th scope="col">归因对象</th><th scope="col">市值</th><th scope="col">已实现</th><th scope="col">未实现</th><th scope="col">费用</th><th scope="col">滑点</th><th scope="col">完整性</th></tr></thead><tbody>{portfolio.attributions.map((item) => <tr key={`${item.dimension}:${item.dimension_key}`}><th scope="row">{attributionLabel(item.dimension)}</th><td>{item.dimension_key}</td><td>USD {formatPaperDecimal(item.market_value, 2)}</td><td>USD {formatPaperDecimal(item.realized_pnl, 2)}</td><td>USD {formatPaperDecimal(item.unrealized_pnl, 2)}</td><td>USD {formatPaperDecimal(item.fees, 2)}</td><td>USD {formatPaperDecimal(item.slippage, 2)}</td><td>{statusLabel(item.completeness)}</td></tr>)}</tbody></table></div>}</section>;
}

function PaperCancelDialog({ pending, submitting, onDismiss, onConfirm }: { pending: PendingCancel; submitting: boolean; onDismiss: () => void; onConfirm: () => void }): React.ReactElement {
  const backRef = useRef<HTMLButtonElement>(null);
  const symbol = pending.order.symbol ?? "未知标的";
  useEffect(() => { backRef.current?.focus(); }, []);
  function onKeyDown(event: React.KeyboardEvent<HTMLDialogElement>): void {
    if (event.key === "Escape" && !submitting) { event.preventDefault(); onDismiss(); return; }
    if (event.key !== "Tab") return;
    const controls = [...event.currentTarget.querySelectorAll<HTMLElement>("button:not([disabled])")];
    if (controls.length === 0) return;
    const first = controls[0]; const last = controls.at(-1)!;
    if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
    else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
  }
  return <div className="confirmation-dialog" role="presentation"><dialog open aria-modal="true" aria-labelledby="paper-cancel-title" aria-describedby="paper-cancel-detail" onKeyDown={onKeyDown}><h3 id="paper-cancel-title">确认取消 {symbol} 模拟订单</h3><p id="paper-cancel-detail">将按当前事件版本 <strong className="numeric-data">{pending.order.current_event_seq}</strong> 请求取消剩余 <strong className="numeric-data">{formatPaperDecimal(pending.order.remaining_quantity)}</strong> 股。该操作只影响内部模拟账本，不会发送券商订单。</p>{pending.unknown ? <p className="inline-notice inline-notice--error">上次取消结果未知。继续确认会沿用原请求，不会重复发起取消。</p> : null}<div className="confirmation-dialog__actions"><button ref={backRef} className="button button--quiet" type="button" onClick={onDismiss} disabled={submitting}>返回订单</button><button className="button button--danger" type="button" onClick={onConfirm} disabled={submitting}><Check aria-hidden="true" size={17} />{submitting ? "正在确认" : pending.unknown ? "继续确认原操作结果" : "确认取消模拟订单"}</button></div></dialog></div>;
}

function MoneyFact({ term, value, signed = false }: { term: string; value: string; signed?: boolean }): React.ReactElement {
  return <div><dt>{term}</dt><dd className="numeric-data" data-sign={signed ? decimalSign(value) : undefined}>USD {formatPaperDecimal(value, 2)}</dd></div>;
}

function orderPath(status: "" | OrderStatus, cursor: string | null): string {
  return `/api/v1/paper/orders?limit=${ORDER_PAGE_SIZE}${status ? `&status=${encodeURIComponent(status)}` : ""}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`;
}
function validPortfolio(value: PaperPortfolio | undefined): PaperPortfolio | null { return value?.mode === "internal_paper" && value.currency === "USD" && Array.isArray(value.positions) && Array.isArray(value.attributions) ? value : null; }
function validOrderPage(value: { items?: PaperOrder[]; next_cursor?: string | null } | undefined): { items: PaperOrder[]; next_cursor: string | null } | null { return value && Array.isArray(value.items) && value.items.length <= ORDER_PAGE_SIZE && (value.next_cursor === null || typeof value.next_cursor === "string" && value.next_cursor.length > 0 && value.next_cursor.length <= 2048) ? { items: value.items, next_cursor: value.next_cursor } : null; }
function validCyclePage(value: { items?: AgentPaperCycleFact[]; summary?: AgentPaperSessionSummaryFact; version_observation?: AgentPaperVersionObservationFact | null } | undefined): { items: AgentPaperCycleFact[]; summary: AgentPaperSessionSummaryFact; version_observation: AgentPaperVersionObservationFact | null } | null { return value && Array.isArray(value.items) && value.items.length <= 20 && value.summary?.execution_locked === true ? { items: value.items, summary: value.summary, version_observation: value.version_observation ?? null } : null; }
function portfolioState(state: PaperPortfolio["data_state"]): DataStateKind { return ({ EMPTY: "empty", READY: "ready", PARTIAL: "partial", STALE: "stale" })[state] as DataStateKind; }
function paperStateDetail(state: DataStateKind): string { return ({ loading: "正在读取冻结绩效、持仓和订单事实。", empty: "尚无已发布的内部模拟绩效快照。", ready: "模拟盘事实已就绪。", partial: "部分归因证据不完整；保留已发布事实，不猜测缺失值。", stale: "显示最后已知快照；新的取消操作需先核对订单版本。", error: "读取失败；没有触发任何模拟或真实订单动作。", forbidden: "当前身份无权读取内部模拟盘。" })[state]; }
function attributionLabel(value: PaperPortfolio["attributions"][number]["dimension"]): string { return ({ cash: "现金", instrument: "标的", sector: "行业" })[value]; }
function decimalSign(value: string): "positive" | "negative" | "zero" { return value.startsWith("-") && !/^-0(?:\.0+)?$/.test(value) ? "negative" : /^0(?:\.0+)?$/.test(value) ? "zero" : "positive"; }
async function fetchWithTimeout(path: string): Promise<Response> { return fetch(path, { headers: { accept: "application/json" }, signal: AbortSignal.timeout(LOAD_TIMEOUT_MS) }); }
