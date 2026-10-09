import { render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CurrentCash } from "../../../app/components/workbench/current-cash";
import { managementPlan } from "../paper/management-fixtures";

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); });
const ready = managementPlan.actual;
describe("current internal ledger cash", () => {
  it("reads cash and cutoff independently of performance, using only GET", async () => {
    const fetcher = vi.fn(async () => Response.json({ data: ready }));
    vi.stubGlobal("fetch", fetcher);
    render(<CurrentCash readVersion={{}} />);
    expect(await screen.findByText("USD 3,500")).toBeVisible();
    expect(screen.getByText("USD 500")).toBeVisible();
    expect(screen.getByText(/^读取时点 /)).toHaveTextContent("2026-09-18");
    expect(screen.getByText(/不代表实时净值或绩效/)).toBeVisible();
    expect(fetcher).toHaveBeenCalledWith("/api/v1/paper/actual", expect.objectContaining({ method: "GET", cache: "no-store" }));
  });
  it("preserves confirmed zero cash as a fact", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ data: { ...ready, available_cash: "0", reserved_cash: "0" } })));
    render(<CurrentCash readVersion={{}} />);
    await waitFor(() => expect(screen.getAllByText("USD 0")).toHaveLength(2));
  });
  it.each([
    { data_state: "UNKNOWN", as_of: null, available_cash: null, reserved_cash: null, nav: null, positions: null },
    { ...ready, available_cash: null },
    { ...ready, available_cash: "NaN" },
    { ...ready, as_of: null },
  ])("does not fabricate cash for unknown or malformed facts", async (data) => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({ data })));
    render(<CurrentCash readVersion={{}} />);
    expect(await screen.findByText(/当前账本现金无法核定/)).toBeVisible();
    expect(screen.queryByText(/USD/)).not.toBeInTheDocument();
  });
  it("clears prior cash while refreshing and after read failure", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce(Response.json({ data: ready })).mockResolvedValueOnce(new Response(null, { status: 403 })));
    const view = render(<CurrentCash readVersion={{}} />);
    await screen.findByText("USD 3,500");
    view.rerender(<CurrentCash readVersion={{}} />);
    expect(screen.queryByText("USD 3,500")).not.toBeInTheDocument();
    await screen.findByText(/当前账本现金无法核定/);
  });
  it("aborts the read when access to the enclosing workspace facts is cleared", async () => {
    const fetcher = vi.fn(() => new Promise<Response>(() => {}));
    vi.stubGlobal("fetch", fetcher);
    const view = render(<CurrentCash readVersion={{}} />);
    await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));
    const signal = (fetcher.mock.calls[0] as unknown as [string, RequestInit])[1].signal!;
    view.unmount();
    expect(signal.aborted).toBe(true);
  });
});
