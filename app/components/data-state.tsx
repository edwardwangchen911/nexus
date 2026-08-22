import type React from "react";

export type DataStateKind =
  | "loading"
  | "empty"
  | "ready"
  | "partial"
  | "stale"
  | "error"
  | "forbidden";

const stateLabels: Record<DataStateKind, string> = {
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
  children?: React.ReactNode;
};

export function DataState({ state, title, detail, children }: DataStateProps): React.ReactElement {
  return (
    <section className="data-state" data-state={state} aria-labelledby="data-state-title">
      <p className="data-state__label" role="status" aria-label={stateLabels[state]}>
        {stateLabels[state]}
      </p>
      <h2 id="data-state-title">{title}</h2>
      {detail ? <p className="data-state__detail">{detail}</p> : null}
      {children}
    </section>
  );
}
