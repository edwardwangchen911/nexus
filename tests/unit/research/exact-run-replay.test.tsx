import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { BriefPanel } from "../../../app/(protected)/briefs/[sessionDate]/page";
import ResearchPage from "../../../app/(protected)/research/[instrumentId]/page";
import { briefData, decisionTime, instrumentId, researchAdmissionEnvelope, researchEnvelope, runId } from "./fixtures";

let query = new URLSearchParams();
vi.mock("next/navigation", () => ({
  useParams: () => ({ instrumentId }),
  useSearchParams: () => query,
}));
const otherRun = "0198d600-1000-7000-8000-000000001533";

beforeEach(() => { query = new URLSearchParams({ run_id: runId }); });
afterEach(() => vi.unstubAllGlobals());

describe("brief links preserve the exact saved run", () => {
  it("binds all six sections to run_id, not the later brief read time", () => {
    const brief = briefData();
    brief.holding_risks = brief.candidates;
    brief.rejected = brief.candidates;
    brief.budget_exhausted_watch = brief.candidates;
    render(<BriefPanel state="ready" brief={brief} onRefresh={vi.fn()} />);
    const links = screen.getAllByRole("link", { name: instrumentId });
    expect(links).toHaveLength(6);
    for (const link of links) expect(link).toHaveAttribute("href", `/research/${instrumentId}?run_id=${runId}`);
  });

  it("reads only the selected run even when as_of points at a newer run, and stays read-only", async () => {
    query.set("as_of", "2026-09-04T00:00:00Z");
    const fetch = vi.fn<(url: string) => Promise<Response>>(async () => Response.json(researchEnvelope()));
    vi.stubGlobal("fetch", fetch);
    render(<ResearchPage />);
    await screen.findByRole("article");
    fireEvent.click(screen.getByRole("button", { name: "重新读取" }));
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
    for (const [url] of fetch.mock.calls) expect(url).toBe(`/api/v1/research-runs/${runId}`);
    expect(screen.queryByRole("button", { name: /运行冻结研究/ })).not.toBeInTheDocument();
  });

  it.each(["instrument", "run", "missing", "forbidden", "empty-selector"])("does not fall back for %s", async (scenario) => {
    query.set("as_of", decisionTime);
    if (scenario === "empty-selector") query.set("run_id", "");
    const data = researchEnvelope();
    if (scenario === "instrument") data.data.instrument_id = otherRun;
    if (scenario === "run") data.data.run_id = otherRun;
    const fetch = vi.fn<(url: string) => Promise<Response>>(async () => scenario === "missing" || scenario === "forbidden"
      ? new Response(null, { status: scenario === "missing" ? 404 : 403 }) : Response.json(data));
    vi.stubGlobal("fetch", fetch);
    render(<ResearchPage />);
    await waitFor(() => expect(screen.getByRole("region", { name: "个股研究证据页" })).not.toHaveAttribute("data-state", "loading"));
    expect(screen.queryByRole("article")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /运行冻结研究/ })).not.toBeInTheDocument();
    for (const [url] of fetch.mock.calls) expect(url).toBe(`/api/v1/research-runs/${runId}`);
    expect(fetch).toHaveBeenCalledTimes(scenario === "empty-selector" ? 0 : 1);
  });

  it("drops previous-run content on navigation and ignores a late response from that run", async () => {
    let resolveOld!: (response: Response) => void;
    const oldResponse = new Promise<Response>((resolve) => { resolveOld = resolve; });
    const fetch = vi.fn(async (url: string) => url.endsWith(runId) ? oldResponse : new Response(null, { status: 404 }));
    vi.stubGlobal("fetch", fetch);
    const view = render(<ResearchPage />);
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    query = new URLSearchParams({ run_id: otherRun });
    view.rerender(<ResearchPage />);
    await waitFor(() => expect(fetch).toHaveBeenCalledTimes(2));
    await act(async () => resolveOld(Response.json(researchEnvelope())));
    expect(screen.queryByRole("article")).not.toBeInTheDocument();
    expect(screen.getByRole("region", { name: "个股研究证据页" })).toHaveAttribute("data-state", "empty");
  });

  it("keeps the existing as_of entry and admission behavior", async () => {
    query = new URLSearchParams({ as_of: decisionTime });
    const fetch = vi.fn(async (url: string) => Response.json(url.includes("research-admission") ? researchAdmissionEnvelope() : researchEnvelope()));
    vi.stubGlobal("fetch", fetch);
    render(<ResearchPage />);
    await screen.findByRole("button", { name: "重新运行冻结研究" });
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      `/api/v1/instruments/${instrumentId}/research?as_of=${encodeURIComponent(decisionTime)}`,
      `/api/v1/instruments/${instrumentId}/research-admission?as_of=${encodeURIComponent(decisionTime)}`,
    ]);
  });
});
