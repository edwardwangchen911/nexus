import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import IntegrationsPage, { IntegrationsPanel } from "../../../app/(protected)/integrations/page";
import LiveApprovalsPage from "../../../app/(protected)/live-approvals/page";
import { GlobalSystemStatus, GlobalSystemStatusView,
  type IntegrationStatus,
} from "../../../app/components/global-system-status";
import type { DataStateKind } from "../../../app/components/data-state";
import { IntegrationStatusProvider } from "../../../app/lib/integrations/browser";

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

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("integrations vertical slice", () => {
  it.each([
    ["A successful read-only connectivity check is required.", "需先通过只读连接检查"],
    ["A valid capability admission is required.", "需先完成有效的能力准入核验"],
    ["Read-only admission is required.", "需先完成只读准入核验"],
    [null, "无需操作"],
    ["future_action", "尚无中文说明（future_action）"],
  ])("explains the next action %s for the real read-only mode", (next_action, label) => {
    const status = { ...fosunFixture, mode: "read_only" as const, next_action };
    render(<IntegrationsPanel state="ready" integrations={[status]} pendingAction={null} notice={null} onTest={vi.fn()} onModeChange={vi.fn()} />);
    expect(screen.getByText("只读")).toBeVisible();
    expect(screen.getByText(label)).toBeVisible();
    expect(screen.queryByRole("button", { name: "测试非交易连接" })).not.toBeInTheDocument();
  });
  it("distinguishes stale data-source evidence from the displayed snapshot", () => {
    render(<GlobalSystemStatusView state="stale" integrations={[fosunFixture]} refreshing={false} onRefresh={vi.fn()} />);
    expect(screen.getByText("数据源：数据已过期")).toBeVisible();
    expect(screen.getByText("数据源证据已过期；页面仍可展示已存快照，不代表当前决策条件已满足。")).toBeVisible();
  });
  it("uses the same next-step wording in the shared system evidence", () => {
    render(<GlobalSystemStatusView state="ready" integrations={[fosunFixture]} refreshing={false} onRefresh={vi.fn()} />);
    fireEvent.click(screen.getByText("查看系统证据"));
    expect(screen.getByText("下一步：需先完成只读准入核验")).toBeVisible();
    expect(screen.queryByText(/Read-only admission/)).not.toBeInTheDocument();
  });
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
    expect(screen.getByText("系统能力 · 集成状态")).toBeVisible();
    expect(card).toHaveAttribute("data-read-only", "true");
    expect(card).not.toHaveAttribute("disabled");
    expect(screen.getByText("离线示例")).toBeVisible();
    expect(screen.queryByText(/ak_must|account-must|internal\.invalid/)).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "复星模拟盘验证" })).toHaveClass("button");
    expect(screen.getByRole("link", { name: "实盘逐笔批准" })).toHaveClass("button");
  });

  it("locks the provider while reconciling an unknown timeout result", () => {
    render(
      <IntegrationsPanel
        state="ready"
        integrations={[fosunFixture]}
        pendingAction="fosun:reconcile"
        notice={{ kind: "error", text: "请求结果仍未知。当前快照仅供参考，相关操作保持锁定；请使用同一幂等键继续确认。" }}
        unresolvedProvider="fosun"
        onTest={vi.fn()}
        onModeChange={vi.fn()}
      />,
    );

    expect(screen.getByRole("button", { name: "正在核对连接状态" })).toBeDisabled();
    expect(screen.getByText("请求结果仍未知。当前快照仅供参考，相关操作保持锁定；请使用同一幂等键继续确认。")).toBeVisible();
    expect(screen.getByRole("button", { name: "禁用复星数据源" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "使用同一幂等键继续确认" })).toBeEnabled();
  });

  it("shares one integration source so a mutation updates the global status and page together", async () => {
    const disabled = { ...fosunFixture, mode: "disabled" as const, quality: "PARTIAL" as const, version: 2 };
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) =>
      init?.method === "POST"
        ? Response.json({ data: disabled, meta: responseMeta() })
        : Response.json({ data: [fosunFixture], meta: responseMeta() }));
    vi.stubGlobal("fetch", fetchMock);

    render(
      <IntegrationStatusProvider>
        <GlobalSystemStatus />
        <IntegrationsPage />
      </IntegrationStatusProvider>,
    );

    expect(await screen.findByText("FOSUN · FIXTURE")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "禁用复星数据源" }));
    expect(await screen.findByText("FOSUN · DISABLED")).toBeVisible();
    expect(screen.getByRole("region", { name: "fosun 集成（已禁用）" })).toBeVisible();
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method !== "POST")).toHaveLength(1);
  });

  it("treats an aborted mutation as unknown and reconciles before enabling another action", async () => {
    const disabled = { ...fosunFixture, mode: "disabled" as const, quality: "PARTIAL" as const, version: 2 };
    let getCount = 0;
    let postCount = 0;
    const idempotencyKeys: Array<string | null> = [];
    const fetchMock = vi.fn((_: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      if (init?.method !== "POST") {
        getCount += 1;
        return Promise.resolve(Response.json({
          data: [fosunFixture],
          meta: responseMeta(),
        }));
      }
      postCount += 1;
      idempotencyKeys.push(new Headers(init.headers).get("idempotency-key"));
      if (postCount > 2) return Promise.resolve(Response.json({ data: disabled, meta: responseMeta() }));
      return new Promise((_resolve, reject) => {
        init.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")));
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<IntegrationStatusProvider><IntegrationsPage /></IntegrationStatusProvider>);
    expect(await screen.findByRole("button", { name: "禁用复星数据源" })).toBeVisible();
    vi.useFakeTimers();
    fireEvent.click(screen.getByRole("button", { name: "禁用复星数据源" }));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(70_000);
    });

    expect(screen.getByText("请求结果仍未知。当前快照仅供参考，相关操作保持锁定；请使用同一幂等键继续确认。")).toBeVisible();
    expect(screen.getByRole("region", { name: "fosun 集成（只读）" })).toBeVisible();
    expect(screen.getByRole("button", { name: "正在核对连接状态" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "禁用复星数据源" })).toBeDisabled();
    expect(postCount).toBe(2);
    expect(getCount).toBe(2);
    expect(new Set(idempotencyKeys).size).toBe(1);

    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "使用同一幂等键继续确认" }));
      await Promise.resolve();
    });

    expect(screen.getByRole("region", { name: "fosun 集成（已禁用）" })).toBeVisible();
    expect(postCount).toBe(3);
    expect(new Set(idempotencyKeys).size).toBe(1);
    expect(screen.queryByText(/未更改 Provider 模式/)).not.toBeInTheDocument();
  });

  it("treats a BFF INTERNAL_ERROR as unknown and confirms with the same idempotency key", async () => {
    const disabled = { ...fosunFixture, mode: "disabled" as const, quality: "PARTIAL" as const, version: 2 };
    const idempotencyKeys: Array<string | null> = [];
    let postCount = 0;
    const fetchMock = vi.fn((_input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      if (init?.method !== "POST") {
        return Promise.resolve(Response.json({ data: [fosunFixture], meta: responseMeta() }));
      }
      postCount += 1;
      idempotencyKeys.push(new Headers(init.headers).get("idempotency-key"));
      return Promise.resolve(postCount === 1
        ? Response.json({
          error: {
            code: "INTERNAL_ERROR",
            message: "The request could not be completed.",
            request_id: responseMeta().request_id,
            details: {},
          },
        }, { status: 500 })
        : Response.json({ data: disabled, meta: responseMeta() }));
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<IntegrationStatusProvider><IntegrationsPage /></IntegrationStatusProvider>);
    fireEvent.click(await screen.findByRole("button", { name: "禁用复星数据源" }));
    expect(await screen.findByText("首次结果未知；已使用同一幂等键确认最终结果。")).toBeVisible();

    expect(screen.getByRole("region", { name: "fosun 集成（已禁用）" })).toBeVisible();
    expect(postCount).toBe(2);
    expect(new Set(idempotencyKeys).size).toBe(1);
  });

  it("uses one atomic polite live region for global refresh without moving focus", () => {
    const onRefresh = vi.fn();
    render(
      <GlobalSystemStatusView
        state="partial"
        integrations={[fosunFixture]}
        refreshing={false}
        onRefresh={onRefresh}
      />,
    );

    const liveRegion = screen.getByRole("status", { name: "全局系统状态" });
    expect(liveRegion).toHaveAttribute("aria-live", "polite");
    expect(liveRegion).toHaveAttribute("aria-atomic", "true");
    expect(screen.getAllByRole("status")).toHaveLength(1);
    expect(screen.getByText("FOSUN · FIXTURE")).toBeVisible();
    expect(screen.getByText("复星 · 演示数据")).toBeVisible();
    expect(screen.getByText("查看系统证据")).toBeVisible();
    const details = liveRegion.querySelector("details")!;
    expect(details).not.toHaveAttribute("open");
    details.open = true;
    fireEvent(details, new Event("toggle"));
    expect(details).toHaveAttribute("open");
    fireEvent.click(screen.getByRole("button", { name: "刷新状态" }));
    expect(onRefresh).toHaveBeenCalledOnce();
  });

  it.each([
    ["error", null],
    ["forbidden", null],
    ["partial", "状态证据需要人工核对。"],
  ] as const)("keeps %s evidence expanded when the system requires attention", (state, notice) => {
    render(
      <GlobalSystemStatusView
        state={state}
        integrations={[fosunFixture]}
        refreshing
        onRefresh={vi.fn()}
        notice={notice}
      />,
    );

    expect(screen.getByRole("status", { name: "全局系统状态" }).querySelector("details")).toHaveAttribute("open");
    expect(screen.getByRole("button", { name: "正在刷新" })).toBeDisabled();
  });

  it("keeps the live approval route visibly locked with no submit surface", () => {
    render(<LiveApprovalsPage />);
    expect(screen.getByText("实盘能力 · 逐笔批准")).toBeVisible();
    expect(screen.getByText("实盘交易未启用")).toBeVisible();
    expect(screen.queryByRole("form")).not.toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    expect(screen.getByRole("link", { name: "返回集成状态" })).toHaveClass("button");
  });
});

function responseMeta() {
  return { request_id: "0198f7a0-1111-7777-8111-111111111114", schema_version: "1", as_of: "2026-08-22T12:00:00Z" };
}
