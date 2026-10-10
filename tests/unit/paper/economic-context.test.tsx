import { act, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import type { components } from "../../../app/lib/generated/schema";
import { EconomicContextBoundary } from "../../../app/components/paper/economic-context";

afterEach(() => { vi.unstubAllGlobals(); window.history.replaceState({}, "", "/"); });
it("invalid economic identity never mounts the ordinary account or calls it", async () => {
  window.history.replaceState({}, "", "/paper?pair_slot_id=invalid&lane=incumbent");
  const fetch = vi.fn(); vi.stubGlobal("fetch", fetch);
  render(<EconomicContextBoundary view="paper"><p>ordinary account</p></EconomicContextBoundary>);
  await screen.findByText("配对上下文无效");
  expect(screen.queryByText("ordinary account")).toBeNull();
  expect(fetch).not.toHaveBeenCalled();
});
it("a default page mounts its existing normal behavior", async () => {
  render(<EconomicContextBoundary view="workspace"><p>ordinary account</p></EconomicContextBoundary>);
  await screen.findByText("ordinary account");
});
it("unknown lane data stays unknown and never falls back to ordinary account", async () => {
  window.history.replaceState({}, "", "/portfolio?pair_slot_id=0198f7a0-1111-7777-8111-111111111110&lane=challenger");
  const fetch = vi.fn(async (path: string) => { expect(path).toContain("/lanes/challenger"); return new Response("", { status: 409 }); }); vi.stubGlobal("fetch", fetch);
  render(<EconomicContextBoundary view="portfolio"><p>ordinary account</p></EconomicContextBoundary>);
  await screen.findByText("配对事实暂不可用");
  expect(screen.queryByText("ordinary account")).toBeNull();
  await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
  expect(fetch.mock.calls[0][0]).toContain("/lanes/challenger");
});


import WorkspacePage from "../../../app/(protected)/workspace/page";
import DecisionsPage from "../../../app/(protected)/decisions/page";
import PaperPage from "../../../app/(protected)/paper/page";
import PortfolioPage from "../../../app/(protected)/portfolio/page";
import { managementDetail } from "./management-fixtures";
import { laneSample } from "./economic-fixtures";
for (const [page, Component] of [["workspace", WorkspacePage], ["decisions", DecisionsPage], ["paper", PaperPage], ["portfolio", PortfolioPage]] as const) {
  it(`reads the same pair lane in the actual ${page} page, without ordinary account reads`, async () => {
    const value = laneSample();
    window.history.replaceState({}, "", `/${page}?pair_slot_id=${value.data.pair.pair_slot_id}&lane=incumbent`);
    const fetch = vi.fn(async (path: string) => { expect(path).toBe(`/api/v1/paper/economic-pairs/${value.data.pair.pair_slot_id}/lanes/incumbent`); return Response.json(value); });
    vi.stubGlobal("fetch", fetch);
    render(<Component />);
    await screen.findByRole("heading", { name: "基线侧 incumbent" });
    expect(screen.getByText(/计划槽位 1 \/ 24/)).toBeVisible();
    expect(screen.getByRole("navigation", { name: "配对事实页面" }).querySelectorAll('a[href*="lane=incumbent"]')).toHaveLength(4);
    expect(fetch).toHaveBeenCalledTimes(1);
  });
}
it("unknown current cash and holdings render as unknown", async () => {
  const value = laneSample();
  Object.assign(value.data.actual, { data_state: "UNKNOWN", as_of: null, available_cash: null, reserved_cash: null, nav: null, positions: null });
  window.history.replaceState({}, "", `/portfolio?pair_slot_id=${value.data.pair.pair_slot_id}&lane=incumbent`);
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(value)));
  render(<PortfolioPage />);
  await screen.findByText("持仓未知");
  expect(screen.getAllByText("未知")).toHaveLength(3);
  expect(screen.queryByText("已核验无持仓")).toBeNull();
});
it("router query changes switch lane and restore the ordinary page", async () => {
  const value = laneSample();
  window.history.replaceState({}, "", `/paper?pair_slot_id=${value.data.pair.pair_slot_id}&lane=incumbent`);
  vi.stubGlobal("fetch", vi.fn(async (path: string) => {
    const next = structuredClone(value);
    next.data.lane = path.endsWith("challenger") ? "challenger" : "incumbent";
    if (next.data.lane === "challenger") {
      next.data.detail!.cycle.cycle_id = next.data.pair.lanes[1].cycle_id;
      next.data.detail!.cycle.status = "researching";
    }
    return Response.json(next);
  }));
  const rendered = render(<EconomicContextBoundary view="paper"><p>ordinary account</p></EconomicContextBoundary>);
  await screen.findByRole("heading", { name: "基线侧 incumbent" });
  act(() => window.history.pushState({}, "", `/paper?pair_slot_id=${value.data.pair.pair_slot_id}&lane=challenger`));
  rendered.rerender(<EconomicContextBoundary view="paper"><p>ordinary account</p></EconomicContextBoundary>);
  await screen.findByRole("heading", { name: "挑战侧 challenger" });
  expect(screen.queryByRole("heading", { name: "基线侧 incumbent" })).toBeNull();
  act(() => window.history.pushState({}, "", "/paper"));
  rendered.rerender(<EconomicContextBoundary view="paper"><p>ordinary account</p></EconomicContextBoundary>);
  await screen.findByText("ordinary account");
  expect(screen.queryByRole("heading", { name: "挑战侧 challenger" })).toBeNull();
  act(() => window.history.pushState({}, "", `/paper?pair_slot_id=${value.data.pair.pair_slot_id}&lane=incumbent`));
  await screen.findByRole("heading", { name: "基线侧 incumbent" });
  expect(screen.queryByText("ordinary account")).toBeNull();
});
it("economic detail plan and unknown-result links retain pair and lane", async () => {
  const value = laneSample();
  value.data.detail!.cycle.status = "partially_filled";
  value.data.detail!.cycle.result_code = "PAPER_RESULT_INCOMPLETE";
  value.data.detail!.orders = [];
  (value.data.detail as components["schemas"]["AgentPaperManagementCycleResponse"]).blocking_plan = structuredClone(managementDetail.blocking_plan);
  value.data.pair.lanes[0].cycle_status = "partially_filled";
  window.history.replaceState({}, "", `/paper?pair_slot_id=${value.data.pair.pair_slot_id}&lane=incumbent`);
  vi.stubGlobal("fetch", vi.fn(async () => Response.json(value)));
  render(<EconomicContextBoundary view="detail"><p>ordinary account</p></EconomicContextBoundary>);
  const link = await screen.findByRole("link", { name: "核对原操作结果" });
  expect(link).toHaveAttribute("href", `/paper?pair_slot_id=${value.data.pair.pair_slot_id}&lane=incumbent`);
  expect(screen.getByRole("link", { name: "查看调整计划" })).toHaveAttribute("href", `/paper?pair_slot_id=${value.data.pair.pair_slot_id}&lane=incumbent`);
});
