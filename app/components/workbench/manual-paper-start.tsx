"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { isManualReceipt, manualEntryErrors, type ManualMode, type ManualReceipt } from "../../lib/paper/manual-receipt";

const STORAGE_KEY = "nexus-manual-paper-attempt-v1";
type Attempt = { key: string; mode: ManualMode };
const labels: Record<ManualReceipt["status"], string> = {
  queued: "已登记，等待宿主处理", preparing: "正在准备研究输入", active: "周期正在运行",
  completed: "周期已完成", unavailable: "输入条件未满足，周期已结束", unknown: "周期结果尚未确认",
};

export function ManualPaperStart(): React.ReactElement {
  const [attempt, setAttempt] = useState<Attempt | null>(null);
  const [receipt, setReceipt] = useState<ManualReceipt | null>(null);
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [restored, setRestored] = useState(false);
  const [rejected, setRejected] = useState(false);
  const inFlight = useRef(false);
  const version = useRef(0);

  const send = useCallback(async (original: Attempt, method: "GET" | "POST") => {
    if (inFlight.current) return;
    inFlight.current = true;
    const current = ++version.current;
    setBusy(true);
    setRejected(false);
    setReceipt(null);
    try {
      const response = await fetch("/api/v1/paper/manual-requests", {
        method, cache: "no-store", signal: AbortSignal.timeout(10_000),
        headers: { "Idempotency-Key": original.key, ...(method === "POST" ? { "Content-Type": "application/json" } : {}) },
        ...(method === "POST" ? { body: JSON.stringify({ mode: original.mode }) } : {}),
      });
      const body: unknown = await response.json();
      if (current !== version.current) return;
      if (!response.ok) {
        if (response.status === 401 || response.status === 403) setReceipt(null);
        const reason = (body as { error?: { details?: { reason_code?: unknown } } })?.error?.details?.reason_code;
        if (method === "POST" && [400, 409, 422].includes(response.status) && typeof reason === "string"
          && Object.hasOwn(manualEntryErrors, reason) && !["MANUAL_RESULT_UNKNOWN", "MANUAL_IDEMPOTENCY_CONFLICT", "MANUAL_NOT_FOUND"].includes(reason)) {
          setMessage(manualEntryErrors[reason]);
          setRejected(true);
          return;
        }
        throw new Error("unconfirmed");
      }
      const data = (body as { data?: unknown })?.data;
      if (method === "GET" && data === null) {
        setMessage("未读取到持久化回执，原提交结果尚未确认。请保留原请求继续核对。");
        return;
      }
      if (!isManualReceipt(data) || data.mode !== original.mode) throw new Error("invalid receipt");
      setReceipt(data);
      setMessage("");
    } catch {
      if (current === version.current) setMessage("提交结果尚未确认。请核对原请求，避免重复开始。");
    } finally {
      if (current === version.current) { inFlight.current = false; setBusy(false); }
    }
  }, []);

  useEffect(() => {
    const lifetime = version;
    const flight = inFlight;
    const timer = globalThis.setTimeout(() => {
      try {
        const stored = sessionStorage.getItem(STORAGE_KEY);
        if (stored) {
          const parsed = JSON.parse(stored) as Attempt;
          if (typeof parsed.key !== "string" || !/^[a-zA-Z0-9-]{1,255}$/.test(parsed.key)
            || !["research_only", "research_and_paper"].includes(parsed.mode)) throw new Error("invalid attempt");
          setAttempt(parsed);
          void send(parsed, "GET");
        }
        setRestored(true);
      } catch { setMessage("无法安全读取原请求标识，请保留此页面并检查浏览器存储。"); }
    }, 0);
    return () => { globalThis.clearTimeout(timer); ++lifetime.current; flight.current = false; };
  }, [send]);

  const start = (mode: ManualMode) => {
    if (!restored || attempt || inFlight.current) return;
    const original = { key: crypto.randomUUID(), mode };
    try { sessionStorage.setItem(STORAGE_KEY, JSON.stringify(original)); }
    catch { setMessage("无法保存原请求标识，尚未提交。请检查浏览器存储。"); return; }
    setAttempt(original);
    void send(original, "POST");
  };
  const resetAllowed = rejected || receipt?.status === "completed" || receipt?.status === "unavailable"
    || receipt?.status === "unknown" && receipt.preparation_disposition === "unknown_acknowledged";
  const reset = () => {
    if (busy || !resetAllowed) return;
    try { sessionStorage.removeItem(STORAGE_KEY); }
    catch { setMessage("无法清除本地请求标识，请保留原请求。"); return; }
    setAttempt(null); setReceipt(null); setRejected(false); setMessage("");
  };
  return <section className="pm-panel" aria-label="开始研究与内部模拟">
    <div className="pm-panel__heading"><h2>开始新周期</h2><p>沿用已批准候选池与现有预算。内部模拟须通过数据、时段和风控检查。</p></div>
    <div className="pm-panel__body">
      <div className="actions">
        <button type="button" className="button button--quiet" disabled={!restored || !!attempt || busy} onClick={() => start("research_only")}>仅研究</button>
        <button type="button" className="button" disabled={!restored || !!attempt || busy} onClick={() => start("research_and_paper")}>研究并内部模拟</button>
      </div>
      <p>实盘执行保持锁定。浏览和刷新不会开始新周期。</p>
      {receipt && <div><p>{receipt.mode === "research_only" ? "仅研究" : "研究并内部模拟"} · <strong>{labels[receipt.status]}</strong></p>
        <p>请求时间：<time dateTime={receipt.requested_at}>{receipt.requested_at}</time></p>
        {receipt.preparation_disposition === "unknown_acknowledged" && <p>准备结果未知已明确接受，调度占用已解除。历史调用、费用和预算保持原样；新请求仍需重新通过准入。</p>}
        {receipt.cycle_id && <a href={`/decisions/${receipt.cycle_id}`}>查看周期决策</a>}
      </div>}
      {message && <p role="status">{message}</p>}
      {attempt && <button type="button" className="button button--quiet" disabled={busy} onClick={() => void send(attempt, "GET")}>{busy ? "正在核对…" : "核对原请求"}</button>}
      {resetAllowed && <button type="button" className="button button--quiet" disabled={busy} onClick={reset}>开始新的请求</button>}
    </div>
  </section>;
}
