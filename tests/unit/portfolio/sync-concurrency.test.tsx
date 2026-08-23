import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import PortfolioPage, { type BrokerSyncRun } from "../../../app/(protected)/portfolio/page";
import { portfolio, portfolioId, responseMeta, snapshot } from "./fixtures";

const replacementId = "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e799";
const replacement = { ...portfolio, portfolio_id: replacementId, name: "Replacement portfolio" };
const replacementSnapshot = { ...snapshot, portfolio_id: replacementId };

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("portfolio sync concurrency", () => {
  it("does not attach a late portfolio A admission to selected portfolio B", async () => {
    const runA = syncRun("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e710", portfolioId, "2026-08-23T08:01:00Z");
    const runB = syncRun("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e711", replacementId, "2026-08-23T08:03:00Z");
    let releaseA: ((value: { data: BrokerSyncRun }) => void) | undefined;
    const posted: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio, replacement], next_cursor: null }, meta: responseMeta() });
      if (url.includes(replacementId)) return Response.json({ data: replacementSnapshot, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") {
        posted.push(String(init?.body));
        if (posted.length === 1) {
          const body = new Promise<{ data: BrokerSyncRun }>((resolve) => { releaseA = resolve; });
          return { ok: true, status: 202, headers: new Headers({ location: `/api/v1/broker-sync-runs/${runA.broker_sync_run_id}` }), json: () => body } as Response;
        }
        return acceptedResponse(runB);
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));
    await waitFor(() => expect(releaseA).toBeTypeOf("function"));
    fireEvent.click(screen.getByRole("button", { name: /Replacement portfolio/ }));
    expect(await screen.findByRole("region", { name: "Replacement portfolio HKD 组合摘要" })).toBeVisible();
    await act(async () => { releaseA?.({ data: runA }); await Promise.resolve(); });

    expect(screen.queryByRole("heading", { name: "同步详情" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "同步复星只读事实" }));
    expect(await screen.findByText("2026-08-23 08:03:00 UTC")).toBeVisible();
    expect(posted.at(-1)).toBe(JSON.stringify({ portfolio_id: replacementId }));
  });

  it("ignores a portfolio A lookup that resolves after portfolio B becomes current", async () => {
    const runA = syncRun("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e712", portfolioId, "2026-08-23T08:01:00Z");
    const runB = syncRun("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e713", replacementId, "2026-08-23T08:03:00Z");
    let releaseLookupA: ((value: { data: BrokerSyncRun }) => void) | undefined;
    let admissions = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio, replacement], next_cursor: null }, meta: responseMeta() });
      if (url.includes(replacementId) && url.endsWith("/snapshots/latest")) return Response.json({ data: replacementSnapshot, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") return acceptedResponse(admissions++ === 0 ? runA : runB);
      if (url === `/api/v1/broker-sync-runs/${runA.broker_sync_run_id}`) {
        const body = new Promise<{ data: BrokerSyncRun }>((resolve) => { releaseLookupA = resolve; });
        return { ok: true, status: 200, json: () => body } as Response;
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));
    fireEvent.click(await screen.findByRole("button", { name: "刷新同步状态" }));
    await waitFor(() => expect(releaseLookupA).toBeTypeOf("function"));
    fireEvent.click(screen.getByRole("button", { name: /Replacement portfolio/ }));
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));
    expect(await screen.findByText("2026-08-23 08:03:00 UTC")).toBeVisible();
    await act(async () => { releaseLookupA?.({ data: { ...runA, status: "READY", completed_at: "2026-08-23T08:04:00Z", cash_flow_quality: "PASS" } }); await Promise.resolve(); });

    expect(screen.getByText("2026-08-23 08:03:00 UTC")).toBeVisible();
    expect(screen.queryByText("2026-08-23 08:01:00 UTC")).not.toBeInTheDocument();
  });

  it("keeps one lookup in flight for a sync run", async () => {
    vi.useFakeTimers();
    const run = syncRun("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e714", portfolioId, "2026-08-23T08:01:00Z");
    let releaseLookup: ((value: { data: BrokerSyncRun }) => void) | undefined;
    let lookups = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") return acceptedResponse(run);
      if (url === `/api/v1/broker-sync-runs/${run.broker_sync_run_id}`) {
        lookups += 1;
        const body = new Promise<{ data: BrokerSyncRun }>((resolve) => { releaseLookup = resolve; });
        return { ok: true, status: 200, json: () => body } as Response;
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "同步复星只读事实" })); await Promise.resolve(); });
    fireEvent.click(screen.getByRole("button", { name: "刷新同步状态" }));
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000); });
    expect(lookups).toBe(1);
    await act(async () => { releaseLookup?.({ data: { ...run, status: "READY", completed_at: "2026-08-23T08:02:00Z", cash_flow_quality: "PASS" } }); await Promise.resolve(); });
    expect(screen.getByText("PASS")).toBeVisible();
  });

  it("never regresses a terminal run back to ACTIVE", async () => {
    const run = syncRun("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e715", portfolioId, "2026-08-23T08:01:00Z");
    let lookups = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") return acceptedResponse(run);
      if (url === `/api/v1/broker-sync-runs/${run.broker_sync_run_id}`) {
        lookups += 1;
        return Response.json({ data: lookups === 1
          ? { ...run, status: "READY", completed_at: "2026-08-23T08:02:00Z", cash_flow_quality: "PASS" }
          : run, meta: responseMeta() });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    fireEvent.click(await screen.findByRole("button", { name: "同步复星只读事实" }));
    fireEvent.click(await screen.findByRole("button", { name: "刷新同步状态" }));
    expect(await screen.findByText("PASS")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "刷新同步状态" }));
    await waitFor(() => expect(lookups).toBe(2));

    expect(screen.getByText("PASS")).toBeVisible();
    expect(screen.queryByText("NOT_RUN")).not.toBeInTheDocument();
  });

  it("re-arms polling when an old lookup finishes after switching away and back", async () => {
    vi.useFakeTimers();
    const run = syncRun("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e716", portfolioId, "2026-08-23T08:01:00Z");
    let releaseOldLookup: ((value: { data: BrokerSyncRun }) => void) | undefined;
    let lookups = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio, replacement], next_cursor: null }, meta: responseMeta() });
      if (url.includes(replacementId) && url.endsWith("/snapshots/latest")) return Response.json({ data: replacementSnapshot, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") return acceptedResponse(run);
      if (url === `/api/v1/broker-sync-runs/${run.broker_sync_run_id}`) {
        lookups += 1;
        if (lookups === 1) {
          const body = new Promise<{ data: BrokerSyncRun }>((resolve) => { releaseOldLookup = resolve; });
          return { ok: true, status: 200, json: () => body } as Response;
        }
        return Response.json({ data: { ...run, status: "READY", completed_at: "2026-08-23T08:02:00Z", cash_flow_quality: "PASS" }, meta: responseMeta() });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "同步复星只读事实" })); await Promise.resolve(); });
    fireEvent.click(screen.getByRole("button", { name: "刷新同步状态" }));
    await act(async () => { await Promise.resolve(); await Promise.resolve(); });
    expect(releaseOldLookup).toBeTypeOf("function");
    fireEvent.click(screen.getByRole("button", { name: /Replacement portfolio/ }));
    fireEvent.click(screen.getByRole("button", { name: /Fosun read-only/ }));
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000); });
    expect(lookups).toBe(1);
    await act(async () => { releaseOldLookup?.({ data: run }); await Promise.resolve(); });
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000); });

    expect(screen.getByText("PASS")).toBeVisible();
    expect(lookups).toBe(2);
  });

  it("retries a current run lookup after a mismatched response", async () => {
    vi.useFakeTimers();
    const run = syncRun("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e717", portfolioId, "2026-08-23T08:01:00Z");
    let lookups = 0;
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/portfolios?")) return Response.json({ data: { items: [portfolio], next_cursor: null }, meta: responseMeta() });
      if (url.endsWith("/snapshots/latest")) return Response.json({ data: snapshot, meta: responseMeta() });
      if (url === "/api/v1/broker-sync-runs") return acceptedResponse(run);
      if (url === `/api/v1/broker-sync-runs/${run.broker_sync_run_id}`) {
        lookups += 1;
        return Response.json({ data: lookups === 1
          ? { ...run, broker_sync_run_id: "018f3f4e-8e8f-7f5b-9f26-a3cc4da1e718" }
          : { ...run, status: "READY", completed_at: "2026-08-23T08:02:00Z", cash_flow_quality: "PASS" }, meta: responseMeta() });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<PortfolioPage />);
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: "同步复星只读事实" })); await Promise.resolve(); });
    fireEvent.click(screen.getByRole("button", { name: "刷新同步状态" }));
    await act(async () => { await Promise.resolve(); });
    expect(screen.getByText("无法刷新同步详情。冻结组合快照保持不变。")).toBeVisible();
    await act(async () => { await vi.advanceTimersByTimeAsync(5_000); });
    expect(screen.getByText("PASS")).toBeVisible();
    expect(lookups).toBe(2);
  });
});

function syncRun(id: string, owner: string, acceptedAt: string): BrokerSyncRun {
  return { broker_sync_run_id: id, portfolio_id: owner, status: "ACTIVE", trigger_kind: "MANUAL", accepted_at: acceptedAt, completed_at: null, attempt_count: 0, cash_flow_quality: "NOT_RUN", failure_category: null };
}

function acceptedResponse(run: BrokerSyncRun): Response {
  return Response.json({ data: run, meta: responseMeta() }, { status: 202, headers: { location: `/api/v1/broker-sync-runs/${run.broker_sync_run_id}` } });
}
