import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ShadowPerformance } from "../../../app/components/shadow-performance";
import { shadowActive, shadowExpired, shadowValidated } from "./fixtures";

describe("Shadow forward-performance evidence", () => {
  it("renders line semantics, the NAV anchor, exact ledger, and permanent safety boundary", () => {
    const shadow = shadowActive();
    const { container } = render(<ShadowPerformance shadow={shadow} />);

    expect(screen.getByRole("heading", { name: "前向影子绩效" })).toBeVisible();
    expect(screen.getByText("前向证据 · 63 个会话")).toBeVisible();
    expect(screen.getByText("检查点 · 5 / 21 / 63")).toBeVisible();
    expect(screen.getByText("首会话 · 模拟结果")).toBeVisible();
    expect(screen.getByText("NAV 100 基准")).toBeVisible();
    const legend = screen.getByRole("list", { name: "三方案线型图例" });
    expect(within(legend).getByText("保持现状 · 实线")).toBeVisible();
    expect(within(legend).getByText("最小调整 · 虚线")).toBeVisible();
    expect(within(legend).getByText("策略目标 · 点线")).toBeVisible();
    expect(container.querySelectorAll(".shadow-line-key--solid, .shadow-line-key--dashed, .shadow-line-key--dotted")).toHaveLength(3);
    expect(container.querySelector('.shadow-performance__canvas[aria-hidden="true"]')).toBeTruthy();

    const navTable = screen.getByRole("table", { name: "三方案逐会话精确 NAV" });
    expect(within(navTable).getAllByRole("row")).toHaveLength(shadow.coverage.completed + 1);
    expect(within(navTable).getByText(shadow.portfolios[2].nav[4].nav)).toBeVisible();
    expect(within(navTable).getAllByRole("columnheader").map((cell) => cell.textContent)).toEqual([
      "交易日", "保持现状", "最小调整", "策略目标",
    ]);

    expect(screen.getByText("NAV 指数点")).toBeVisible();
    expect(screen.getAllByText("-0.021000000000").length).toBeGreaterThan(0);
    expect(screen.getByText(/不会创建订单，也不需要交易审批/)).toBeVisible();
    expect(screen.getByText(/fosun · price_bars · delayed/)).toBeVisible();
    expect(screen.getAllByText(/filled at target-session open/)).toHaveLength(3);
    expect(screen.queryByRole("button", { name: /下单|交易审批|买入|卖出|验证通过|晋级|自进化/ })).not.toBeInTheDocument();
  });

  it("moves the semantic selected-session detail with Home, End, and arrow keys", () => {
    const shadow = shadowActive();
    render(<ShadowPerformance shadow={shadow} />);
    const navigator = screen.getByRole("group", { name: "选择影子会话" });
    const detail = screen.getByRole("status", { name: "当前影子会话详情" });

    expect(detail).toHaveTextContent(shadow.portfolios[0].nav[4].session);
    fireEvent.keyDown(navigator, { key: "Home" });
    expect(detail).toHaveTextContent(shadow.portfolios[0].nav[0].session);
    fireEvent.keyDown(navigator, { key: "ArrowRight" });
    expect(detail).toHaveTextContent(shadow.portfolios[0].nav[1].session);
    fireEvent.keyDown(navigator, { key: "End" });
    expect(detail).toHaveTextContent(shadow.portfolios[0].nav[4].session);
    fireEvent.keyDown(navigator, { key: "ArrowLeft" });
    expect(detail).toHaveTextContent(shadow.portfolios[0].nav[3].session);
  });

  it("shows unknown checkpoint reasons instead of fabricating metrics", () => {
    render(<ShadowPerformance shadow={shadowExpired()} />);

    expect(screen.getAllByText(/未知 · SHADOW_DATA_INCOMPLETE/).length).toBeGreaterThan(0);
    expect(screen.getByText(/最终状态：已过期/)).toBeVisible();
  });

  it("renders all 63 validated sessions without hiding exact rows", () => {
    const shadow = shadowValidated();
    render(<ShadowPerformance shadow={shadow} />);

    const navTable = screen.getByRole("table", { name: "三方案逐会话精确 NAV" });
    expect(within(navTable).getAllByRole("row")).toHaveLength(64);
    expect(screen.getAllByText("63 / 63")).toHaveLength(2);
    expect(screen.getByText(/最终状态：已验证/)).toBeVisible();
  });
});
