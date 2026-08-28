import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import BrokerSimulationPage, {
  BrokerSimulationPanel,
  type BrokerWorkbenchState,
} from "../../../app/(protected)/broker-simulation/page";
import { envelope, intentId, readyWorkbench } from "./fixtures";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const states: Array<[BrokerWorkbenchState, string]> = [
  ["loading", "正在加载"],
  ["empty", "暂无待处理批次"],
  ["ready", "证据已就绪"],
  ["partial", "权威事实不完整"],
  ["stale", "权威事实已过期"],
  ["error", "加载失败"],
  ["forbidden", "无权访问"],
  ["reconciling", "正在对账"],
  ["unknown", "提交结果未知"],
];

describe("broker simulation workbench", () => {
  it.each(states)("renders a fail-closed %s state", (state, label) => {
    const hasFacts = ["ready", "partial", "stale", "reconciling", "unknown"].includes(state);
    render(<BrokerSimulationPanel
      state={state}
      workbench={hasFacts ? readyWorkbench : null}
      notice={null}
      busy={false}
      onRefresh={vi.fn()}
      onApprove={vi.fn()}
      onReject={vi.fn()}
    />);

    expect(screen.getByRole("region", { name: "复星模拟盘审核工作台" })).toHaveAttribute("data-state", state);
    expect(screen.getAllByText("实盘关闭").length).toBeGreaterThan(0);
    expect(screen.getByText(label)).toBeVisible();
    expect(screen.queryByRole("button", { name: /派发|修改订单|取消订单/ })).not.toBeInTheDocument();
    if (!hasFacts) expect(screen.queryByText("USD 1,200.00")).not.toBeInTheDocument();
  });

  it("renders the execution circuit, exact server batch and redacted authority", () => {
    render(<BrokerSimulationPanel state="ready" workbench={readyWorkbench} notice={null} busy={false}
      onRefresh={vi.fn()} onApprove={vi.fn()} onReject={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "复星模拟盘验证" })).toBeVisible();
    const circuit = screen.getByRole("region", { name: "执行电路" });
    for (const step of ["策略提案", "确定性风控", "人工批准", "外部激活", "派发与对账"]) {
      expect(within(circuit).getByText(step)).toBeVisible();
    }
    expect(screen.getByText("FOSUN SIM ··· 8421")).toBeVisible();
    expect(screen.getByText("USD 1,200.00")).toBeVisible();
    expect(screen.getByRole("table", { name: "服务端订单批次" })).toHaveTextContent("USD 200.00");
    expect(screen.getByRole("list", { name: "券商订单事件时间线" })).toHaveTextContent("E01");
    expect(screen.getByText(/external_activation_blocked/)).toBeVisible();
    expect(document.body).not.toHaveTextContent(/account_id|provider_id|client_order_id|api[_-]?key/i);
  });

  it("loads current server facts and keeps an atomic polite live region", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json(envelope())));
    render(<BrokerSimulationPage />);

    expect(await screen.findByText("USD 1,200.00")).toBeVisible();
    expect(screen.getByRole("region", { name: "复星模拟盘审核工作台" })).toHaveAttribute("data-state", "ready");
    const status = screen.getByRole("status", { name: "模拟盘工作台状态" });
    expect(status).toHaveAttribute("aria-live", "polite");
    expect(status).toHaveAttribute("aria-atomic", "true");
  });

  it("confirms only the immutable local approval and rereads authority", async () => {
    const calls: Array<{ path: string; method: string; body?: string; ifMatch?: string | null; key?: string | null }> = [];
    let reads = 0;
    vi.spyOn(crypto, "randomUUID").mockReturnValue("0198f7a0-1111-7777-8111-111111111219");
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (!init?.method || init.method === "GET") {
        reads += 1;
        calls.push({ path, method: "GET" });
        return Response.json(envelope());
      }
      calls.push({
        path,
        method: init.method,
        body: String(init.body),
        ifMatch: new Headers(init.headers).get("if-match"),
        key: new Headers(init.headers).get("idempotency-key"),
      });
      return Response.json({
        data: { resource_id: intentId, action: "approve", status: "approved", version: 2, replayed: false, dispatch_created: false },
        meta: envelope().meta,
      });
    }));

    render(<BrokerSimulationPage />);
    const approve = await screen.findByRole("button", { name: "审阅并批准离线批次" });
    fireEvent.click(approve);
    const dialog = screen.getByRole("dialog", { name: "确认批准离线模拟批次" });
    expect(dialog).toHaveTextContent("批准不会派发任何订单");
    expect(within(dialog).getByRole("button", { name: "返回审阅" })).toHaveFocus();
    fireEvent.click(within(dialog).getByRole("button", { name: "确认离线批准" }));

    await waitFor(() => expect(reads).toBeGreaterThanOrEqual(2));
    expect(calls.find((call) => call.method === "POST")).toEqual({
      path: `/api/v1/broker-simulation/intents/${intentId}/approve`,
      method: "POST",
      body: '{"version":1}',
      ifMatch: "a".repeat(64),
      key: "broker-approve-0198f7a0-1111-7777-8111-111111111219",
    });
    expect(await screen.findByText("离线批次已批准；未创建派发任务。")).toBeVisible();
  });

  it("clears all broker facts when access is forbidden", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ error: { code: "FORBIDDEN" } }, { status: 403 })));
    render(<BrokerSimulationPage />);
    expect(await screen.findByText("无权访问")).toBeVisible();
    expect(screen.queryByText("FOSUN SIM ··· 8421")).not.toBeInTheDocument();
  });
});
