import { render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ query: "", load: vi.fn() }));
vi.mock("next/navigation", () => ({ useSearchParams: () => new URLSearchParams(state.query) }));
vi.mock("../../../app/components/paper/economic-context", () => {
  state.load();
  return { EconomicContextBoundary: ({ children }: { children: React.ReactNode }) => <>{children}</>, EconomicLanePage: () => <p>paired body</p> };
});
import WorkspacePage from "../../../app/(protected)/workspace/page";
import { EconomicContextBoundary } from "../../../app/components/paper/economic-context-boundary";

beforeEach(() => { state.query = ""; vi.stubGlobal("fetch", vi.fn(() => new Promise(() => {}))); });
afterEach(() => vi.unstubAllGlobals());
it("ordinary workbench does not load the unrelated economic page graph", () => {
  render(<WorkspacePage />);
  expect(screen.getByRole("heading", { name: "交易工作台" })).toBeVisible();
  expect(state.load).not.toHaveBeenCalled();
});
it("invalid context remains rejected without loading economic content", () => {
  state.query = "lane=incumbent";
  render(<EconomicContextBoundary view="workspace"><p>ordinary workbench</p></EconomicContextBoundary>);
  expect(screen.getByText("配对上下文无效")).toBeVisible();
  expect(screen.queryByText("ordinary workbench")).not.toBeInTheDocument();
  expect(state.load).not.toHaveBeenCalled();
});
it("valid context still loads the original economic page", async () => {
  state.query = "pair_slot_id=123e4567-e89b-42d3-a456-426614174000&lane=incumbent";
  render(<EconomicContextBoundary view="workspace"><p>ordinary workbench</p></EconomicContextBoundary>);
  expect(await screen.findByText("paired body")).toBeVisible();
  expect(state.load).toHaveBeenCalledOnce();
});
