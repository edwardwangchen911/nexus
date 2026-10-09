"use client";

import { useEffect, useState } from "react";
import { formatDisplayDecimal } from "../../lib/display-decimal";
import { formatDisplayTime } from "../../lib/display-format";

type Cash = { as_of: string; available_cash: string; reserved_cash: string };
function cashFacts(value: unknown): Cash | null {
  if (!value || typeof value !== "object") return null;
  const data = value as Record<string, unknown>;
  if (data.data_state !== "READY" || typeof data.as_of !== "string"
    || !/(Z|[+-]\d{2}:\d{2})$/.test(data.as_of) || !Number.isFinite(Date.parse(data.as_of))
    || typeof data.available_cash !== "string" || !/^-?\d+(\.\d+)?$/.test(data.available_cash)
    || typeof data.reserved_cash !== "string" || !/^-?\d+(\.\d+)?$/.test(data.reserved_cash)) return null;
  return { as_of: data.as_of, available_cash: data.available_cash, reserved_cash: data.reserved_cash };
}

export function CurrentCash({ readVersion }: { readVersion: object }): React.ReactElement {
  const [result, setResult] = useState<{ version: object; cash: Cash | null } | null>(null);
  useEffect(() => {
    const controller = new AbortController();
    void (async () => {
      let cash: Cash | null = null;
      try {
        const response = await fetch("/api/v1/paper/actual", {
          method: "GET", cache: "no-store",
          signal: AbortSignal.any([controller.signal, AbortSignal.timeout(10_000)]),
        });
        if (response.ok) {
          const payload: unknown = await response.json();
          cash = cashFacts(payload && typeof payload === "object" ? (payload as Record<string, unknown>).data : null);
        }
      } catch { /* Failed reads remain unknown, never zero. */ }
      if (!controller.signal.aborted) setResult({ version: readVersion, cash });
    })();
    return () => controller.abort();
  }, [readVersion]);
  const current = result?.version === readVersion ? result : null;
  return <section className="workbench-panel workbench-account" aria-label="当前账本现金（内部模拟）">
    <header className="workbench-panel__heading"><div><p className="eyebrow">INTERNAL PAPER LEDGER</p><h2>当前账本现金（内部模拟）</h2></div></header>
    <p>按读取时点核对成交与预留，不代表实时净值或绩效。</p>
    {!current ? <p>正在读取当前账本现金</p> : !current.cash ? <p>当前账本现金无法核定，请查看账本记录；未知不等于零。</p> : <>
      <dl><div><dt>可用现金</dt><dd className="numeric-data">USD {formatDisplayDecimal(current.cash.available_cash, { group: true })}</dd></div>
        <div><dt>预留现金</dt><dd className="numeric-data">USD {formatDisplayDecimal(current.cash.reserved_cash, { group: true })}</dd></div></dl>
      <p>读取时点 {formatDisplayTime(current.cash.as_of)}</p>
    </>}
    <a className="button button--quiet" href="/paper#paper-current-actual">查看当前账本与持仓</a>
  </section>;
}
