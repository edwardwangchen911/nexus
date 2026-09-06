"use client";

import { useEffect, useState } from "react";
import type { components } from "../lib/generated/schema";
import { formatDisplayTime } from "../lib/display-format";

type News = components["schemas"]["NewsResponse"];

export function SavedNews({ instrumentId }: { instrumentId: string }) {
  return <SavedNewsContent key={instrumentId} instrumentId={instrumentId} />;
}

function SavedNewsContent({ instrumentId }: { instrumentId: string }) {
  const [data, setData] = useState<News | null>(null);
  const [failed, setFailed] = useState(false);
  const [loading, setLoading] = useState(true);
  const [read, setRead] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    fetch(`/api/v1/instruments/${encodeURIComponent(instrumentId)}/news`, {
      method: "GET", signal: controller.signal, cache: "no-store", credentials: "same-origin",
    }).then(async (response) => {
      if (!response.ok) throw new Error("NEWS_READ_FAILED");
      const envelope = await response.json();
      if (envelope.data.instrument_id !== instrumentId) throw new Error("NEWS_IDENTITY_INVALID");
      if (!controller.signal.aborted) setData(envelope.data);
    }).catch(() => { if (!controller.signal.aborted) setFailed(true); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [instrumentId, read]);
  return <section className="saved-news" aria-label="最新消息" aria-busy={loading}>
    <div className="section-heading"><h2>最新消息</h2>
      <button className="button button--quiet" type="button" disabled={loading} onClick={() => {
        setLoading(true); setFailed(false); setData(null); setRead((n) => n + 1);
      }}>
        {loading ? "正在读取" : "读取已保存消息"}
      </button></div>
    <p>单一来源 Alpaca · 查询截止至少滞后采集 15 分钟，实际发布延迟未核实。未启用实时推送或自动采集。</p>
    {failed ? <p role="status">消息暂时无法读取，请稍后重新读取。</p> : null}
    {data?.status === "unavailable" ? <p>尚未采集该标的消息，不代表没有重要消息。</p> : null}
    {data?.status === "invalid" ? <p role="status">已保存消息的原件校验未通过，暂不展示。</p> : null}
    {data && ["saved", "empty"].includes(data.status) ? <>
      <p>{data.symbol} · 采集时间 {formatDisplayTime(data.received_at)}<br />
        查询窗口 {formatDisplayTime(data.window_start)} 至 {formatDisplayTime(data.window_end)}</p>
      <p>{data.has_more ? "仅展示本次最新一页，仍有更早消息未读取。" : "该查询页已读完，不代表来源覆盖全部消息。"}消息尚未纳入既有冻结研究，也不构成调整依据。</p>
      {data.status === "empty" ? <p>本次查询未返回消息；这不是“没有消息”的证明。</p> : <ul className="saved-news__list">
        {data.articles.map((article) => <li key={article.id}>
          <article>
            <h3>{article.url ? <a href={article.url} target="_blank" rel="noopener noreferrer">{article.headline}</a> : article.headline}</h3>
            <p>{article.source} · {article.author || "作者未提供"}</p>
            <p>发布时间 {formatDisplayTime(article.created_at)}<br />更新时间 {formatDisplayTime(article.updated_at)}</p>
            {article.summary ? <p>{article.summary}</p> : <p>来源未提供摘要，可查看原文。</p>}
          </article>
        </li>)}
      </ul>}
      <details><summary>原件追溯</summary><p className="saved-news__hash">SHA-256 {data.content_hash}</p></details>
    </> : null}
  </section>;
}
