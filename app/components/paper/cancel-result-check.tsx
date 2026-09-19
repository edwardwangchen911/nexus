"use client";
import { useId, useRef, useState } from "react";
import { formatPaperDecimal, formatPaperTime } from "../order-timeline";
import { statusLabel } from "../../lib/display-labels";
import type { components } from "../../lib/generated/schema";
type Result = components["schemas"]["PaperCancelResultResponse"];
export type CancelAttempt = { orderId: string; key: string; requestedAt: string };
type Props = { attempt: CancelAttempt | null; planId?: string | null; targetId?: string | null };

export function CancelResultCheck(props: Props): React.ReactElement {
  return <CancelResultSession key={`${props.attempt?.orderId ?? "missing"}:${props.attempt?.key ?? "missing"}`} {...props} />;
}
function CancelResultSession({ attempt, planId, targetId }: Props): React.ReactElement {
  const headingId = useId();
  const [result, setResult] = useState<Result | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [readAt, setReadAt] = useState<string | null>(null);
  const inFlight = useRef(false);
  async function check(): Promise<void> {
    if (!attempt || inFlight.current) return;
    inFlight.current = true; setBusy(true); setError(null);
    try {
      const response = await fetch(`/api/v1/paper/orders/${encodeURIComponent(attempt.orderId)}/cancel-result`, { method: "GET", cache: "no-store", headers: { "Idempotency-Key": attempt.key }, signal: AbortSignal.timeout(10_000) });
      if (response.status === 401 || response.status === 403) { setResult(null); setReadAt(null); setError("无权核对原操作结果"); return; }
      if (!response.ok) throw new Error("read failed");
      const envelope = await response.json() as { data?: Result; meta?: { as_of?: string } };
      if (!envelope.data || envelope.data.order?.order_id !== attempt.orderId || !["NOT_RECORDED", "COMMITTED", "CONFLICT", "UNKNOWN"].includes(envelope.data.status)) throw new Error("invalid result");
      setResult(envelope.data); setReadAt(envelope.meta?.as_of ?? null);
    } catch { setError("尚未查明原取消结果；保留最后已知事实与占用。"); }
    finally { inFlight.current = false; setBusy(false); }
  }
  return <section className="pm-panel pm-cancel" aria-labelledby={headingId}><header className="pm-panel__heading"><h2 id={headingId}>核对原操作结果</h2></header><div className="pm-panel__body">
    <dl className="pm-evidence"><div><dt>操作类型</dt><dd>取消请求</dd></div>{planId ? <div><dt>调整计划</dt><dd>{planId}</dd></div> : null}{targetId ? <div><dt>目标标识</dt><dd>{targetId}</dd></div> : null}<div><dt>原订单</dt><dd>{attempt?.orderId ?? "未定位"}</dd></div><div><dt>本地取消尝试键</dt><dd>{attempt?.key ?? "缺少原请求标识，暂不能核对"}</dd></div><div><dt>请求发起时间</dt><dd>{attempt ? formatPaperTime(attempt.requestedAt) : "未记录"}</dd></div><div><dt>最近核对时间</dt><dd>{readAt ? formatPaperTime(readAt) : "尚未核对"}</dd></div></dl>
    <button className="button button--quiet" type="button" disabled={!attempt || busy} onClick={() => void check()}>{busy ? "正在核对原取消请求…" : "核对原操作结果"}</button>
    <div role="status" aria-live="polite">{error ? <p>{error}</p> : null}{result ? <><p>{({ NOT_RECORDED: "服务端未记录原取消结果", COMMITTED: "原取消已确认", CONFLICT: "原请求关联冲突", UNKNOWN: "尚未查明原取消结果" })[result.status]}</p><p>订单：{result.order.status ? statusLabel(result.order.status) : "未知"} · 已确认成交 {formatPaperDecimal(result.order.filled_quantity)} 股</p><p>预留：{result.reservation_released === true ? "已释放" : result.reservation_released === false ? "尚未释放" : "尚未查明"}{result.reservation?.outstanding_amount != null ? ` · ${result.reservation.reservation_type === "cash" ? "USD " : ""}${formatPaperDecimal(result.reservation.outstanding_amount)}${result.reservation.reservation_type === "position" ? " 股" : ""}` : ""}</p></> : null}</div>
    <p className="pm-footnote">仅核对原取消请求结果；不重发买入、卖出、取消或改单操作。核对结果不等于资源释放。</p>
  </div></section>;
}
