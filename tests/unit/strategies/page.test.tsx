import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import StrategiesPage, { StrategiesPanel } from "../../../app/(protected)/strategies/page";
import type { DataStateKind } from "../../../app/components/data-state";
import { actionEnvelope, detail, detailEnvelope, overview, overviewEnvelope } from "../strategy-evolution/fixtures";

const states: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"],
  ["empty", "暂无数据"],
  ["ready", "数据已就绪"],
  ["partial", "数据不完整"],
  ["stale", "数据已过期"],
  ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

const callbacks = {
  onRefresh: vi.fn(),
  onSelectExperiment: vi.fn(),
  onStartExperiment: vi.fn(),
  onControlRegistry: vi.fn(),
  onVetoExperiment: vi.fn(),
  onRollbackRegistry: vi.fn(),
  onRetryUnknown: vi.fn(),
};

function renderPanel(state: DataStateKind, overrides = {}) {
  return render(<StrategiesPanel
    state={state}
    overview={["ready", "partial", "stale"].includes(state) ? overview : null}
    detail={["ready", "partial", "stale"].includes(state) ? detail : null}
    etag={state === "ready" ? '"g1-v1"' : null}
    busyAction={null}
    errorMessage={null}
    successMessage={null}
    unknownResult={false}
    {...callbacks}
    {...overrides}
  />);
}

describe("strategy evolution workbench", () => {
  afterEach(() => {
    vi.clearAllMocks();
    vi.unstubAllGlobals();
  });

  it.each(states)("renders the visible %s state and the immutable live boundary", (state, label) => {
    renderPanel(state);
    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "策略进化工作台" })).toHaveAttribute("data-state", state);
    expect(screen.getByText("实盘关闭")).toBeVisible();
  });

  it("renders the complete evidence lifecycle without a trade surface", () => {
    renderPanel("ready");

    expect(screen.getByRole("heading", { name: "baseline_v1" })).toBeVisible();
    expect(screen.getByText("提高流动性门槛可减少成本侵蚀，同时保留中期动量溢价。")).toBeVisible();
    expect(screen.getByText("ADV20 最低成交额 · 3 个候选值")).toBeVisible();
    expect(screen.getByRole("heading", { name: "证据比较" })).toBeVisible();
    expect(screen.getByText("SPY 比较完整")).toBeVisible();
    expect(screen.getByText("等权白名单比较完整")).toBeVisible();
    expect(screen.getByText("21 / 68 sessions")).toBeVisible();
    expect(screen.getByText("检查点 5 · 已完成")).toBeVisible();
    expect(screen.getByText("检查点 21 · 已完成")).toBeVisible();
    expect(screen.getByText("检查点 63 · 等待")).toBeVisible();
    expect(screen.getByRole("heading", { name: "晋级解释" })).toBeVisible();
    expect(screen.getAllByText("通过").length).toBeGreaterThan(0);
    expect(screen.getByRole("heading", { name: "冠军历史" })).toBeVisible();
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出/ })).not.toBeInTheDocument();
  });

  it.each(["partial", "stale"] as const)("disables every mutation when evidence is %s", (state) => {
    renderPanel(state);
    for (const button of screen.getAllByRole("button")) {
      if (button.getAttribute("aria-label") === "刷新策略进化证据") expect(button).toBeEnabled();
      else expect(button).toBeDisabled();
    }
  });

  it("uses an accessible, fixed-copy rollback confirmation", () => {
    renderPanel("ready");
    fireEvent.click(screen.getByRole("button", { name: "回滚上一个冠军" }));

    const dialog = screen.getByRole("dialog", { name: "确认回滚策略冠军" });
    expect(dialog).toHaveTextContent("只影响未来内部模拟决策");
    expect(dialog).toHaveTextContent("不会撤销历史成交");
    expect(dialog).toHaveTextContent("不会创建任何真实订单");
    fireEvent.click(screen.getByRole("button", { name: "确认回滚" }));
    expect(callbacks.onRollbackRegistry).toHaveBeenCalledOnce();
  });

  it("focuses the safe error summary and exposes a polite success status", async () => {
    renderPanel("ready", { errorMessage: "资源已变化，请刷新后重试。", successMessage: "已暂停自动晋级。" });
    await waitFor(() => expect(screen.getByRole("alert")).toHaveFocus());
    expect(screen.getByRole("status")).toHaveTextContent("已暂停自动晋级。");
  });

  it("closes new mutations while an unknown result can only replay the original command", () => {
    renderPanel("ready", {
      unknownResult: true,
      errorMessage: "操作结果未知。",
    });

    expect(screen.getByRole("region", { name: "未知操作恢复" })).toHaveTextContent("路径、命令、权威版本和幂等键均保持不变");
    expect(screen.getByRole("button", { name: "暂停自动晋级" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "否决本次晋级" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "使用同一幂等键继续确认" }));
    expect(callbacks.onRetryUnknown).toHaveBeenCalledOnce();
  });

  it("refetches after a 409 and clears all protected evidence after authorization is revoked", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: { code: "VERSION_CONFLICT" } }), { status: 409 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v2"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: { etag: '"g1-v2"' } }))
      .mockResolvedValueOnce(new Response(null, { status: 403 }));
    vi.stubGlobal("fetch", fetchMock);
    render(<StrategiesPage />);

    expect(await screen.findByRole("heading", { name: "baseline_v1" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "暂停自动晋级" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(5));
    expect(screen.getByRole("alert")).toHaveTextContent("资源已变化");

    fireEvent.click(screen.getByRole("button", { name: "刷新策略进化证据" }));
    expect(await screen.findByRole("status", { name: "无权访问" })).toBeVisible();
    expect(screen.queryByRole("heading", { name: "baseline_v1" })).not.toBeInTheDocument();
  });

  it("keeps accepted actions pending until the refreshed authority confirms them", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(actionEnvelope("rollback_registry")), { status: 202, headers: { location: "/api/v1/strategy-evolution" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v2"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: { etag: '"g1-v2"' } }));
    vi.stubGlobal("fetch", fetchMock);
    render(<StrategiesPage />);

    expect(await screen.findByRole("heading", { name: "baseline_v1" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "回滚上一个冠军" }));
    fireEvent.click(screen.getByRole("button", { name: "确认回滚" }));
    expect(await screen.findByText("回滚已接受，正在等待确定性边界生效。")).toBeVisible();
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(5));
  });

  it("fails closed when experiment evidence cannot be bound to the overview authority", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: { code: "AUTHORITY_UNAVAILABLE" } }), { status: 503 }));
    vi.stubGlobal("fetch", fetchMock);
    render(<StrategiesPage />);

    expect(await screen.findByText("数据不完整")).toBeVisible();
    expect(screen.getByRole("heading", { name: "baseline_v1" })).toBeVisible();
    for (const button of screen.getAllByRole("button")) {
      if (button.getAttribute("aria-label") !== "刷新策略进化证据") expect(button).toBeDisabled();
    }
  });
});
