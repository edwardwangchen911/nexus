"use client";

import { useEffect, useRef } from "react";

type DecisionStage = "research" | "diagnose" | "what-if" | "paper" | "review";

const stages: Array<{ id: DecisionStage; label: string; detail: string }> = [
  { id: "research", label: "研究证据", detail: "冻结时点" },
  { id: "diagnose", label: "组合诊断", detail: "确定性规则" },
  { id: "what-if", label: "情景试算", detail: "不持久化" },
  { id: "paper", label: "内部模拟", detail: "账本事实" },
  { id: "review", label: "离线审核", detail: "外部断开" },
];

export function DecisionPath({ current }: { current: DecisionStage }): React.ReactElement {
  const currentIndex = stages.findIndex((stage) => stage.id === current);
  const currentRef = useRef<HTMLLIElement>(null);

  useEffect(() => {
    currentRef.current?.scrollIntoView?.({ block: "nearest", inline: "center" });
  }, [current]);

  return (
    // The bounded evidence chain must remain keyboard-scrollable on narrow screens.
    // eslint-disable-next-line jsx-a11y/no-noninteractive-tabindex
    <div className="decision-path" role="region" aria-label="投资决策证据链，可横向滚动" tabIndex={0}>
      <ol>
        {stages.map((stage, index) => (
          <li
            key={stage.id}
            ref={index === currentIndex ? currentRef : undefined}
            data-state={index < currentIndex ? "previous" : index === currentIndex ? "current" : "upcoming"}
            aria-current={index === currentIndex ? "step" : undefined}
          >
            <span className="decision-path__index">{String(index + 1).padStart(2, "0")}</span>
            <span className="decision-path__copy"><strong>{stage.label}</strong><small>{stage.detail}</small></span>
          </li>
        ))}
      </ol>
    </div>
  );
}
