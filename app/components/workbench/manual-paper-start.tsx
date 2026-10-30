"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { isManualReceipt, manualEntryErrors, type ManualMode, type ManualReceipt } from "../../lib/paper/manual-receipt";

const STORAGE_KEY = "nexus-manual-paper-attempt-v1";
type Attempt = { key: string; mode: ManualMode };
const labels: Record<ManualReceipt["status"], string> = {
  queued: "已登记，等待宿主处理", preparing: "正在准备研究输入", active: "周期正在运行",
  completed: "周期已完成", unavailable: "输入条件未满足，周期已结束", unknown: "周期结果尚未确认",
};
const researchLabels: Record<ManualReceipt["status"], string> = {
  queued: "已登记，等待宿主处理", preparing: "正在准备研究输入", active: "研究正在处理",
  completed: "研究处理已完成", unavailable: "研究条件未满足，请求已结束", unknown: "研究结果尚未确认",
};
const memberStatusLabels: Record<string, string> = {
  prepared: "等待处理", running: "处理中", validated: "已校验", audited: "已审查", synthesized: "已综合",
  completed: "处理完成", failed: "处理失败", abstained: "已停止研究",
};
const qualityLabels: Record<string, string> = { complete: "内容完整", partial: "内容受限" };

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
            || !["research_only", "research_and_paper", "research_and_monitor"].includes(parsed.mode)) throw new Error("invalid attempt");
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
    || receipt?.status === "unknown" && (receipt.preparation_disposition === "unknown_acknowledged"
      || receipt.independent_disposition === "unknown_acknowledged");
  const reset = () => {
    if (busy || !resetAllowed) return;
    try { sessionStorage.removeItem(STORAGE_KEY); }
    catch { setMessage("无法清除本地请求标识，请保留原请求。"); return; }
    setAttempt(null); setReceipt(null); setRejected(false); setMessage("");
  };
  return <section className="pm-panel" aria-label="开始研究与内部模拟">
    <div className="pm-panel__heading"><h2>开始研究或内部模拟</h2><p>沿用已批准候选池与现有预算。仅研究支持盘外处理；内部模拟须通过数据、时段和风控检查。</p></div>
    <div className="pm-panel__body">
      <div className="actions">
        <button type="button" className="button button--quiet" disabled={!restored || !!attempt || busy} onClick={() => start("research_only")}>仅研究</button>
        <button type="button" className="button button--quiet" disabled={!restored || !!attempt || busy} onClick={() => start("research_and_monitor")}>研究并持续监控</button>
        <button type="button" className="button" disabled={!restored || !!attempt || busy} onClick={() => start("research_and_paper")}>研究并内部模拟</button>
      </div>
      <p>实盘执行保持锁定。浏览和刷新不会开始新请求。</p>
      {receipt && <div><p>{receipt.mode === "research_only" ? "仅研究" : receipt.mode === "research_and_monitor" ? "研究并持续监控" : "研究并内部模拟"} · <strong>{receipt.code === "MANUAL_PREPARATION_CANCELLED" ? "准备已安全取消" : (receipt.research_contract ? researchLabels : labels)[receipt.status]}</strong></p>
        <p>请求时间：<time dateTime={receipt.requested_at}>{receipt.requested_at}</time></p>
        {receipt.research_contract && <>
          {receipt.research_contract === "independent_monitor_research_v1" ? <p>通过审计的条件可进入持续监控，命中后仅复核受影响证券。历史观察价不等于实时行情；研究及后续复核不构成正式决策，不产生组合目标或订单。</p> : <p>基于历史观察资料开展研究，远端最新状态未核实。结果不构成正式决策，不产生组合目标、计划或订单。</p>}
          {receipt.research ? <>
            <p>研究资料冻结时间：<time dateTime={receipt.research.research_as_of}>{receipt.research.research_as_of}</time>（UTC）</p>
            <p>处理完成仅表示流程结束，内容质量须逐只核对；正式决策仍须重新核验当时资料与风控。</p>
            <ul aria-label="各证券研究结果">
              {receipt.research.members.map((member, index) => <li key={member.run_id}>
                <a href={`/research/${member.instrument_id}?run_id=${member.run_id}`}>查看第 {index + 1} 只证券研究</a>
                <p>处理状态：<strong>{Object.hasOwn(memberStatusLabels, member.status) ? memberStatusLabels[member.status] : "处理状态未知"}</strong></p>
                <p>内容质量：<strong>{member.quality && Object.hasOwn(qualityLabels, member.quality) ? qualityLabels[member.quality] : "内容质量未知"}</strong></p>
                <p>{member.usage_known ? "用量已确认" : "用量未知，保留预算占用"}</p>
              </li>)}
            </ul>
          </> : <p>研究资料尚未冻结；请求时间不代表研究资料截止时间。</p>}
        </>}
        {receipt.independent_disposition === "unknown_acknowledged" && <p>独立研究未知影响已明确接受。原结果、未知费用和预算保留；历史占用的接受不代表下一轮已获准，开始前仍须重新核验。</p>}
        {receipt.independent_disposition === "recheck_required" && <p>独立研究的接受记录需要重新核验，当前不能据此解除历史占用。原未知费用和预算继续保留。</p>}
        {receipt.preparation_disposition === "unknown_acknowledged" && <p>准备结果未知已明确接受，调度占用已解除。历史调用、费用和预算保持原样；新请求仍需重新通过准入。</p>}
        {receipt.preparation_recovery?.disposition === "cancelled_before_call" && <p>已确认未开始外部操作，本次准备已取消。新请求仍需重新通过全部准入。</p>}
        {receipt.preparation_recovery?.disposition === "unknown_preserved" && <p>准备已收尾，但外部结果仍未知，费用和预算继续保留；当前不能开始新请求。</p>}
        {receipt.preparation_recovery?.disposition === "cycle_reconciled" && <p>已找回原周期，请继续核对原周期结果；没有创建第二个周期。</p>}
        {!receipt.cycle_id && receipt.preparation_recovery?.cycle_id && <a href={`/decisions/${receipt.preparation_recovery.cycle_id}`}>查看原周期决策</a>}
        {receipt.cycle_id && <a href={`/decisions/${receipt.cycle_id}`}>查看周期决策</a>}
      </div>}
      {message && <p role="status">{message}</p>}
      {attempt && <button type="button" className="button button--quiet" disabled={busy} onClick={() => void send(attempt, "GET")}>{busy ? "正在核对…" : "核对原请求"}</button>}
      {resetAllowed && <button type="button" className="button button--quiet" disabled={busy} onClick={reset}>开始新的请求</button>}
    </div>
  </section>;
}
