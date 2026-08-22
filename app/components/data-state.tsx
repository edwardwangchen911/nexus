import { useId, type ReactNode } from "react";
import type React from "react";

export type DataStateKind =
  | "loading"
  | "empty"
  | "ready"
  | "partial"
  | "stale"
  | "error"
  | "forbidden";

export const dataStateLabels: Record<DataStateKind, string> = {
  loading: "正在加载",
  empty: "暂无数据",
  ready: "数据已就绪",
  partial: "数据不完整",
  stale: "数据已过期",
  error: "加载失败",
  forbidden: "无权访问",
};

type DataStateProps = {
  state: DataStateKind;
  title: string;
  detail?: string;
  children?: ReactNode;
  announce?: boolean;
};

export function DataState({ state, title, detail, children, announce = true }: DataStateProps): React.ReactElement {
  const headingId = useId();

  return (
    <section className="data-state" data-state={state} aria-labelledby={headingId}>
      <p className="data-state__label" role={announce ? "status" : undefined} aria-label={announce ? dataStateLabels[state] : undefined}>
        {dataStateLabels[state]}
      </p>
      <h2 id={headingId}>{title}</h2>
      {detail ? <p className="data-state__detail">{detail}</p> : null}
      {children}
    </section>
  );
}
