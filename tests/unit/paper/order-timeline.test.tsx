import { fireEvent, render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { OrderTimeline } from "../../../app/components/order-timeline";
import { cancellableOrder, filledOrder } from "./fixtures";

describe("OrderTimeline", () => {
  it("renders the append-only server sequence and exact fill authority", () => {
    const order = structuredClone(cancellableOrder);
    order.events[0].occurred_at = "2026-08-26T20:02:00Z";
    render(<OrderTimeline order={order} onCancel={vi.fn()} />);

    const timeline = screen.getByRole("list", { name: "AAPL 订单事件时间线" });
    const events = within(timeline).getAllByRole("listitem");
    expect(events.map((item) => item.getAttribute("data-event-seq"))).toEqual(["1", "2", "3", "4"]);
    expect(events[0]).toHaveTextContent("paper_order_proposed");
    expect(events[3]).toHaveTextContent("submitted → partially_filled");

    const fills = screen.getByRole("table", { name: "AAPL 模拟成交事实" });
    expect(fills).toHaveTextContent("alpaca / iex");
    expect(fills).toHaveTextContent("delayed");
    expect(fills).toHaveTextContent("2026-08-26 19:59:00 UTC");
    expect(fills).toHaveTextContent("10 bps");
  });

  it("shows a cancel control only when the server marks the order cancellable", () => {
    const onCancel = vi.fn();
    const { rerender } = render(<OrderTimeline order={cancellableOrder} onCancel={onCancel} />);
    fireEvent.click(screen.getByRole("button", { name: "取消 AAPL 模拟订单" }));
    expect(onCancel).toHaveBeenCalledWith(cancellableOrder, expect.any(HTMLButtonElement));

    rerender(<OrderTimeline order={filledOrder} onCancel={onCancel} />);
    expect(screen.queryByRole("button", { name: /取消 AAPL 模拟订单/ })).not.toBeInTheDocument();
    expect(screen.getAllByText("已成交").length).toBeGreaterThan(0);
  });

  it("keeps a server-cancellable order read-only when the page authority is degraded", () => {
    render(<OrderTimeline order={cancellableOrder} allowCancel={false} onCancel={vi.fn()} />);

    expect(screen.queryByRole("button", { name: "取消 AAPL 模拟订单" })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /AAPL/ })).toBeVisible();
  });
});
