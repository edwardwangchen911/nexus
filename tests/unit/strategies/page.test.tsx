import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import StrategiesPage, { StrategiesPanel } from "../../../app/(protected)/strategies/page";
import type { DataStateKind } from "../../../app/components/data-state";

const baseline = {
  strategy_version: "baseline_v1" as const,
  champion: true as const,
  evolution_status: "locked" as const,
  rebalance_frequency: "weekly" as const,
  historical_agent_overlay: false as const,
  live_execution_enabled: false as const,
  latest_backtest: null,
};

const states: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"],
  ["empty", "暂无数据"],
  ["ready", "数据已就绪"],
  ["partial", "数据不完整"],
  ["stale", "数据已过期"],
  ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

describe("strategy governance page", () => {
  afterEach(() => vi.unstubAllGlobals());

  it.each(states)("renders the visible %s state", (state, label) => {
    render(
      <StrategiesPanel
        state={state}
        strategies={["ready", "partial", "stale"].includes(state) ? [baseline] : []}
        onRefresh={vi.fn()}
      />,
    );

    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "策略与实验页面" })).toHaveAttribute("data-state", state);
  });

  it("shows the immutable baseline and every execution boundary without a trade surface", () => {
    render(<StrategiesPanel state="ready" strategies={[baseline]} onRefresh={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "baseline_v1" })).toBeVisible();
    expect(screen.getByText("当前唯一冠军")).toBeVisible();
    expect(screen.getByText("策略进化未启用")).toBeVisible();
    expect(screen.getByText("历史回测不含 Agent overlay")).toBeVisible();
    expect(screen.getByText("实盘执行关闭")).toBeVisible();
    expect(screen.getByText("暂无已持久化的回测结果")).toBeVisible();
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出|晋级/ })).not.toBeInTheDocument();
  });

  it("renders only server-confirmed backtest evidence and benchmark completeness", () => {
    render(<StrategiesPanel state="ready" strategies={[{
      ...baseline,
      latest_backtest: {
        result_hash: "a".repeat(64),
        data_hash: "b".repeat(64),
        code_hash: "c".repeat(64),
        start_session: "2018-01-02",
        end_session: "2026-08-25",
        oos_window_count: 8,
        comparison_keys: ["spy", "equal_weight", "champion"],
        benchmark_comparison_complete: true,
        data_mode: "fixture" as const,
        data_sources: ["fixture:test:fixture"],
        completed_at: "2026-08-26T00:00:00Z",
      },
    }]} onRefresh={vi.fn()} />);

    expect(screen.getByRole("heading", { name: "最新回测证据" })).toBeVisible();
    expect(screen.getByText("基准比较完整")).toBeVisible();
    expect(screen.getByText("8 个样本外窗口")).toBeVisible();
    expect(screen.getByText("FIXTURE / 合成数据")).toBeVisible();
    expect(screen.getByText(/仅用于验证流程/)).toBeVisible();
    expect(screen.queryByText("暂无已持久化的回测结果")).not.toBeInTheDocument();
  });

  it("warns when mixed provenance contains any fixture authority domain", () => {
    render(<StrategiesPanel state="ready" strategies={[{
      ...baseline,
      latest_backtest: {
        result_hash: "a".repeat(64),
        data_hash: "b".repeat(64),
        code_hash: "c".repeat(64),
        start_session: "2018-01-02",
        end_session: "2026-08-25",
        oos_window_count: 8,
        comparison_keys: ["spy", "equal_weight", "champion"],
        benchmark_comparison_complete: true,
        data_mode: "mixed" as const,
        data_sources: ["price:alpaca:sip:real_time", "sector:fixture"],
        completed_at: "2026-08-26T00:00:00Z",
      },
    }]} onRefresh={vi.fn()} />);

    expect(screen.getByText("MIXED / 混合来源")).toBeVisible();
    expect(screen.getByText(/含 fixture 证据/)).toBeVisible();
  });

  it("never restores protected strategy data after authorization is revoked", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ data: [baseline] }), { status: 200 }))
      .mockResolvedValueOnce(new Response(null, { status: 403 }))
      .mockRejectedValueOnce(new TypeError("network unavailable"));
    vi.stubGlobal("fetch", fetchMock);
    render(<StrategiesPage />);

    expect(await screen.findByRole("heading", { name: "baseline_v1" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "刷新策略" }));
    expect(await screen.findByRole("status", { name: "无权访问" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "刷新策略" }));

    await waitFor(() =>
      expect(screen.getByRole("status", { name: "加载失败" })).toBeVisible(),
    );
    expect(screen.queryByRole("heading", { name: "baseline_v1" })).not.toBeInTheDocument();
  });
});
