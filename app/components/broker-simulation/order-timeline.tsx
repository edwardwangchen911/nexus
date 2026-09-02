import { statusLabel } from "../../lib/display-labels";
import type { components } from "../../lib/generated/schema";
import { formatPaperTime } from "../order-timeline";

type BrokerEvent = components["schemas"]["BrokerEventResponse"];

export function BrokerOrderTimeline({ events }: { events: BrokerEvent[] }): React.ReactElement {
  return (
    <section className="broker-section" aria-labelledby="broker-events-title">
      <header className="broker-section__heading">
        <div><p className="eyebrow">仅追加回执</p><h2 id="broker-events-title">事件时间线</h2></div>
        <p>{events.length} 条脱敏事件</p>
      </header>
      {events.length === 0 ? <p className="broker-section__empty">暂无外部事件；不以推断状态代替回执。</p> : (
        <ol className="broker-timeline" aria-label="券商订单事件时间线">
          {events.map((event) => (
            <li key={`${event.order_id}:${event.sequence}`}>
              <span className="broker-timeline__seq numeric-data">E{String(event.sequence).padStart(2, "0")}</span>
              <div><strong>{statusLabel(event.status)}</strong><p>{event.source} · {shortId(event.order_id)}</p></div>
              <time className="numeric-data" dateTime={event.occurred_at}>{formatPaperTime(event.occurred_at)}</time>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}

function shortId(value: string): string { return `${value.slice(0, 8)}…${value.slice(-4)}`; }
