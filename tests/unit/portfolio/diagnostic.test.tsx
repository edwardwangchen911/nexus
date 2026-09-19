import { render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";

import { PortfolioDiagnostic } from "../../../app/components/portfolio-diagnostic";
import PortfolioPage from "../../../app/(protected)/portfolio/page";
import { diagnosticReport } from "../optimize/fixtures";
import { portfolio, portfolioId, snapshot, snapshotId } from "./fixtures";

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });

it("loads a diagnostic through the real portfolio page without starting a provider refresh", async () => {
  const report = { ...diagnosticReport("RESEARCH_REQUIRED"), portfolio_snapshot_id: snapshotId, snapshot_as_of: snapshot.as_of! };
  const read = vi.fn(async (input: RequestInfo | URL) => {
    const path = String(input);
    if (path.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null } });
    if (path.endsWith("/snapshots/latest")) return Response.json({ data: snapshot });
    if (path.endsWith("/diagnostics/latest")) return Response.json({ data: report });
    throw new Error("unexpected request");
  });
  vi.stubGlobal("fetch", read);
  render(<PortfolioPage />);
  expect(await screen.findByRole("region", { name: "当前快照诊断" })).toBeVisible();
  expect(screen.getByRole("button", { name: "生成调整情景" })).toBeEnabled();
  expect(screen.getByRole("region", { name: "Fosun read-only HKD 组合摘要" })).toBeVisible();
  expect(read).toHaveBeenCalledTimes(3);
  const calls = read.mock.calls as unknown as Array<[RequestInfo | URL, RequestInit?]>;
  expect(calls.every((call) => (call[1]?.method ?? "GET") === "GET")).toBe(true);
});

it("shows a snapshot-bound preliminary diagnosis without suggesting execution", async () => {
  const report = diagnosticReport("RESEARCH_REQUIRED");
  report.portfolio_snapshot_id = snapshotId;
  report.items.push({
    ...report.items[0], diagnostic_code: "PRELIMINARY_DIAGNOSTIC",
    metric_value: null, reference_limit: null, headroom: null,
    headroom_reason: "FORMAL_DIAGNOSTIC_PENDING", unknown_reason: "FORMAL_DIAGNOSTIC_PENDING",
    actionability: "blocked",
  });
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json({ data: report })));
  render(<PortfolioDiagnostic portfolioId={portfolioId} snapshotId={snapshotId} />);
  expect(await screen.findByRole("heading", { name: "只读初步诊断" })).toBeVisible();
  expect(screen.getByText(/不是正式决策诊断/)).toBeVisible();
  expect(screen.getByRole("listitem", { name: /CONCENTRATION_TOP1/ })).toBeVisible();
  expect(screen.getByRole("heading", { name: "单一持仓集中度" })).toBeVisible();
  expect(screen.queryByRole("button", { name: /下单|执行|优化/ })).not.toBeInTheDocument();
});

it("rejects a report from a different snapshot instead of mixing portfolio facts", async () => {
  const onReport = vi.fn();
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json({ data: diagnosticReport() })));
  render(<PortfolioDiagnostic portfolioId={portfolioId} snapshotId={snapshotId} onReport={onReport} />);
  expect(await screen.findByText(/诊断暂不可用/)).toBeVisible();
  expect(onReport).toHaveBeenLastCalledWith(null);
  expect(screen.queryByRole("listitem", { name: /CONCENTRATION_TOP1/ })).not.toBeInTheDocument();
});

it("reports only a formally identified snapshot-bound diagnosis to its parent", async () => {
  const report = { ...diagnosticReport(), portfolio_snapshot_id: snapshotId };
  const onReport = vi.fn();
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(Response.json({ data: report })));

  render(<PortfolioDiagnostic portfolioId={portfolioId} snapshotId={snapshotId} onReport={onReport} />);

  await screen.findByRole("region", { name: "当前快照诊断" });
  expect(onReport).toHaveBeenLastCalledWith(report);
});

it("keeps diagnosis read failures local without hiding account facts or retrying", async () => {
  const read = vi.fn().mockResolvedValue(new Response(null, { status: 503 }));
  vi.stubGlobal("fetch", read);
  render(<PortfolioDiagnostic portfolioId={portfolioId} snapshotId={snapshotId} />);
  expect(await screen.findByText(/诊断暂不可用/)).toBeVisible();
  expect(read).toHaveBeenCalledTimes(1);
  expect(read.mock.calls[0][0]).toBe(`/api/v1/portfolios/${portfolioId}/diagnostics/latest`);
});
