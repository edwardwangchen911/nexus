import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import BrokerSimulationPage from "../../../app/(protected)/broker-simulation/page";
import { IntegrationsPanel } from "../../../app/(protected)/integrations/page";
import LiveApprovalsPage from "../../../app/(protected)/live-approvals/page";
import {
  GlobalSystemStatusView,
  type IntegrationStatus,
} from "../../../app/components/global-system-status";
import type { DataStateKind } from "../../../app/components/data-state";

const fosunFixture: IntegrationStatus = {
  provider: "fosun",
  mode: "fixture",
  quality: "PASS",
  as_of: "2026-08-22T12:00:00Z",
  capability_expires_at: null,
  contract_hash: null,
  next_action: "Read-only admission is required.",
  version: 1,
};

const stateLabels: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"],
  ["empty", "暂无数据"],
  ["ready", "数据已就绪"],
  ["partial", "数据不完整"],
  ["stale", "数据已过期"],
  ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

describe("integrations vertical slice", () => {
  it.each(stateLabels)("renders the visible %s data state", (state, label) => {
    render(
      <IntegrationsPanel
        state={state}
        integrations={state === "ready" || state === "partial" || state === "stale" ? [fosunFixture] : []}
        pendingAction={null}
        notice={null}
        onTest={vi.fn()}
        onModeChange={vi.fn()}
      />,
    );

    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "数据与能力状态页面" })).toHaveAttribute(
      "data-state",
      state,
    );
  });

  it("renders only the generated safe fields and labels fixture as read-only, not disabled", () => {
    const poisoned = {
      ...fosunFixture,
      api_key: "ak_must_never_render",
      account_id: "account-must-never-render",
      endpoint: "https://internal.invalid",
    } as IntegrationStatus;

    render(
      <IntegrationsPanel
        state="ready"
        integrations={[poisoned]}
        pendingAction={null}
        notice={null}
        onTest={vi.fn()}
        onModeChange={vi.fn()}
      />,
    );

    const card = screen.getByRole("region", { name: "fosun 集成（只读）" });
    expect(card).toHaveAttribute("data-read-only", "true");
    expect(card).not.toHaveAttribute("disabled");
    expect(screen.getByText("FIXTURE")).toBeVisible();
    expect(screen.queryByText(/ak_must|account-must|internal\.invalid/)).not.toBeInTheDocument();
  });

  it("disables only the pending action and shows an inline timeout recovery path", () => {
    render(
      <IntegrationsPanel
        state="ready"
        integrations={[fosunFixture]}
        pendingAction="fosun:test"
        notice={{ kind: "error", text: "连接测试超时。未更改 Provider 模式，请重试。" }}
        onTest={vi.fn()}
        onModeChange={vi.fn()}
      />,
    );

    expect(screen.getByRole("button", { name: "正在测试复星非交易连接" })).toBeDisabled();
    expect(screen.getByText("连接测试超时。未更改 Provider 模式，请重试。")).toBeVisible();
    expect(screen.getByRole("button", { name: "禁用复星 Provider" })).not.toBeDisabled();
  });

  it("uses one atomic polite live region for global refresh without moving focus", () => {
    render(
      <GlobalSystemStatusView
        state="partial"
        integrations={[fosunFixture]}
        refreshing={false}
        onRefresh={vi.fn()}
      />,
    );

    const liveRegion = screen.getByRole("status", { name: "全局系统状态" });
    expect(liveRegion).toHaveAttribute("aria-live", "polite");
    expect(liveRegion).toHaveAttribute("aria-atomic", "true");
    expect(screen.getAllByRole("status")).toHaveLength(1);
    expect(screen.getByText("FOSUN · FIXTURE")).toBeVisible();
  });

  it("keeps broker simulation and live approval routes visibly locked with no submit surface", () => {
    const { rerender } = render(<BrokerSimulationPage />);

    expect(screen.getByText("阶段 3 前锁定")).toBeVisible();
    expect(screen.queryByRole("form")).not.toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();

    rerender(<LiveApprovalsPage />);
    expect(screen.getByText("阶段 4 前锁定")).toBeVisible();
    expect(screen.queryByRole("form")).not.toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
});
