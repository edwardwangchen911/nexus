import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import PortfolioPage, { type PortfolioItem } from "../../../app/(protected)/portfolio/page";
import { portfolio, responseMeta, snapshot } from "./fixtures";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("portfolio pagination boundaries", () => {
  it.each([
    ["empty", ""],
    ["overlong", "x".repeat(2049)],
  ])("rejects an %s cursor", async (_label, badCursor) => {
    stubPortfolioPages(() => page([portfolio], badCursor));
    render(<PortfolioPage />);

    expect(await screen.findByText("无法读取组合事实。当前状态保持只读。")).toBeVisible();
    expect(screen.queryByRole("region", { name: "Fosun read-only HKD 组合摘要" })).not.toBeInTheDocument();
  });

  it("rejects a repeated cursor", async () => {
    stubPortfolioPages((call) => page(call === 1 ? [portfolio] : [], "same-cursor"));
    render(<PortfolioPage />);

    expect(await screen.findByText("无法读取组合事实。当前状态保持只读。")).toBeVisible();
  });

  it("rejects a portfolio ID repeated across pages", async () => {
    stubPortfolioPages((call) => call === 1 ? page([portfolio], "next") : page([portfolio], null));
    render(<PortfolioPage />);

    expect(await screen.findByText("无法读取组合事实。当前状态保持只读。")).toBeVisible();
  });

  it("accepts a terminal tenth page", async () => {
    let calls = 0;
    stubPortfolioPages((call) => {
      calls = call;
      const start = (call - 1) * 50;
      return page(Array.from({ length: 50 }, (_, offset) => item(start + offset + 1)), call === 10 ? null : `cursor-${call}`);
    });
    render(<PortfolioPage />);

    expect(await screen.findByRole("region", { name: "Portfolio 1 HKD 组合摘要" })).toBeVisible();
    expect(screen.getByRole("button", { name: /Portfolio 500/ })).toBeVisible();
    expect(calls).toBe(10);
  }, 15_000);

  it("rejects a tenth page that still advertises another page", async () => {
    let calls = 0;
    stubPortfolioPages((call) => {
      calls = call;
      return page([item(call)], `cursor-${call}`);
    });
    render(<PortfolioPage />);

    expect(await screen.findByText("无法读取组合事实。当前状态保持只读。")).toBeVisible();
    expect(calls).toBe(10);
  });

  it("rejects a page containing more than the admitted 50 portfolios", async () => {
    stubPortfolioPages(() => page(Array.from({ length: 51 }, (_, index) => item(index + 1)), null));
    render(<PortfolioPage />);

    expect(await screen.findByText("无法读取组合事实。当前状态保持只读。")).toBeVisible();
  });

  it("preserves the last frozen facts when a later refresh page fails", async () => {
    let listCalls = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) {
        listCalls += 1;
        if (listCalls === 1) return page([portfolio], null);
        if (listCalls === 2) return page([portfolio], "later-page");
        return Response.json({ error: { code: "INTERNAL_ERROR" } }, { status: 500 });
      }
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    expect(await screen.findByRole("region", { name: "Fosun read-only HKD 组合摘要" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "刷新页面数据" }));

    expect(await screen.findByText("无法读取组合事实。当前状态保持只读。")).toBeVisible();
    expect(screen.getByRole("region", { name: "Fosun read-only HKD 组合摘要" })).toBeVisible();
    expect(screen.getByRole("region", { name: "组合事实页面" })).toHaveAttribute("data-state", "stale");
  });
});

function stubPortfolioPages(respond: (call: number) => Response): void {
  let calls = 0;
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.startsWith("/api/v1/portfolios?")) return respond(++calls);
    if (url.endsWith("/snapshots/latest")) return Response.json({ data: { ...snapshot, portfolio_id: item(1).portfolio_id }, meta: responseMeta() });
    throw new Error(`unexpected request: ${url}`);
  }));
}

function page(items: PortfolioItem[], nextCursor: string | null): Response {
  return Response.json({ data: { items, next_cursor: nextCursor }, meta: responseMeta() });
}

function item(index: number): PortfolioItem {
  return { ...portfolio, portfolio_id: `018f3f4e-8e8f-7f5b-9f26-${String(index).padStart(12, "0")}`, name: `Portfolio ${index}` };
}
