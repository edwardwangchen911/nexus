import { fireEvent, render, screen, waitFor, cleanup } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ManualPaperStart } from "../../../app/components/workbench/manual-paper-start";
import { independentUnknownReceipt, researchReceipt } from "./research-fixture";
const id = "0198f7a0-1111-7777-8111-111111111111";
const receipt = { request_id: id, mode: "research_only", requested_at: "2026-10-09T14:00:00Z", status: "queued", code: "MANUAL_REQUEST_QUEUED", cycle_id: null, execution_locked: true };
const fetchMock = vi.fn();
beforeEach(() => { sessionStorage.clear(); fetchMock.mockReset(); vi.stubGlobal("fetch", fetchMock); });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
it("mounts without writes, starts only the explicit mode, and prevents duplicate clicks", async () => {
  fetchMock.mockResolvedValue(Response.json({ data: receipt }));
  render(<ManualPaperStart />);
  await waitFor(() => expect(screen.getByRole("button", { name: "仅研究" })).toBeEnabled());
  expect(fetchMock).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "仅研究" }));
  fireEvent.click(screen.getByRole("button", { name: "仅研究" }));
  await screen.findByText("已登记，等待宿主处理");
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: "POST", body: '{"mode":"research_only"}' });
  expect(screen.getByRole("button", { name: "研究并内部模拟" })).toBeDisabled();
});
it("keeps an uncertain key across refresh and only queries it, without retrying POST", async () => {
  fetchMock.mockRejectedValueOnce(new Error("lost"));
  const view = render(<ManualPaperStart />);
  await waitFor(() => expect(screen.getByRole("button", { name: "仅研究" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "仅研究" }));
  await screen.findByText(/提交结果尚未确认/);
  const key = fetchMock.mock.calls[0][1].headers["Idempotency-Key"];
  view.unmount();
  fetchMock.mockResolvedValue(Response.json({ data: receipt }));
  render(<ManualPaperStart />);
  await screen.findByText("已登记，等待宿主处理");
  expect(fetchMock.mock.calls[1][1]).toMatchObject({ method: "GET", headers: { "Idempotency-Key": key } });
  expect(fetchMock.mock.calls.filter((c) => c[1].method === "POST")).toHaveLength(1);
});
it("does not unlock new starts when the original lookup is missing or malformed", async () => {
  fetchMock.mockRejectedValueOnce(new Error("lost"));
  render(<ManualPaperStart />);
  await waitFor(() => expect(screen.getByRole("button", { name: "仅研究" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "仅研究" }));
  await screen.findByText(/提交结果尚未确认/);
  fetchMock.mockResolvedValue(Response.json({ data: null }));
  fireEvent.click(screen.getByRole("button", { name: "核对原请求" }));
  await screen.findByText(/未读取到持久化回执/);
  expect(screen.getByRole("button", { name: "仅研究" })).toBeDisabled();
  expect(fetchMock.mock.calls.filter((c) => c[1].method === "POST")).toHaveLength(1);
});
it("rejects mismatched mode and ignores untrusted raw errors", async () => {
  fetchMock.mockResolvedValue(Response.json({ data: { ...receipt, mode: "research_and_paper" } }));
  render(<ManualPaperStart />);
  await waitFor(() => expect(screen.getByRole("button", { name: "仅研究" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "仅研究" }));
  await waitFor(() => expect(screen.getByText(/提交结果尚未确认/)).toBeVisible());
  expect(screen.queryByText("已登记，等待宿主处理")).not.toBeInTheDocument();
});
it("submits internal paper explicitly and allows a new request only after a confirmed terminal receipt", async () => {
  const cycle = "0198f7a0-2222-7777-8111-111111111111";
  fetchMock.mockResolvedValue(Response.json({ data: { ...receipt, mode: "research_and_paper", status: "completed", code: "MANUAL_PAPER_COMPLETED", cycle_id: cycle } }));
  render(<ManualPaperStart />);
  await waitFor(() => expect(screen.getByRole("button", { name: "研究并内部模拟" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "研究并内部模拟" }));
  await screen.findByText("周期已完成");
  expect(fetchMock.mock.calls[0][1].body).toBe('{"mode":"research_and_paper"}');
  expect(screen.getByRole("link", { name: "查看周期决策" })).toHaveAttribute("href", `/decisions/${cycle}`);
  fireEvent.click(screen.getByRole("button", { name: "开始新的请求" }));
  expect(screen.getByRole("button", { name: "仅研究" })).toBeEnabled();
  expect(fetchMock).toHaveBeenCalledTimes(1);
});
it("allows an explicit new selection after a known refusal but hides raw error material", async () => {
  fetchMock.mockResolvedValue(Response.json({ error: { message: "private", details: { reason_code: "MANUAL_HOST_NOT_READY" } } }, { status: 409 }));
  render(<ManualPaperStart />);
  await waitFor(() => expect(screen.getByRole("button", { name: "仅研究" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "仅研究" }));
  await screen.findByText("同版本宿主尚未就绪。");
  expect(screen.queryByText("private")).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "开始新的请求" }));
  expect(screen.getByRole("button", { name: "仅研究" })).toBeEnabled();
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("keeps acknowledged unknown visible and resets only local reference without replay", async () => {
  sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "old-unknown", mode: "research_only" }));
  fetchMock.mockResolvedValue(Response.json({ data: { ...receipt, status: "unknown", code: "MANUAL_RESULT_UNKNOWN", preparation_disposition: "unknown_acknowledged" } }));
  render(<ManualPaperStart />);
  await screen.findByText("周期结果尚未确认");
  expect(screen.getByText(/历史调用、费用和预算保持原样/)).toBeVisible();
  expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: "GET", headers: { "Idempotency-Key": "old-unknown" } });
  fireEvent.click(screen.getByRole("button", { name: "开始新的请求" }));
  expect(sessionStorage.getItem("nexus-manual-paper-attempt-v1")).toBeNull();
  expect(screen.getByRole("button", { name: "仅研究" })).toBeEnabled();
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it("does not clear an unacknowledged unknown or keep a stale acknowledgement after failed GET", async () => {
  sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "old-unknown", mode: "research_only" }));
  fetchMock.mockResolvedValueOnce(Response.json({ data: { ...receipt, status: "unknown", code: "MANUAL_RESULT_UNKNOWN" } }));
  render(<ManualPaperStart />);
  await screen.findByText("周期结果尚未确认");
  expect(screen.queryByRole("button", { name: "开始新的请求" })).not.toBeInTheDocument();
  fetchMock.mockResolvedValueOnce(Response.json({ data: { ...receipt, status: "unknown", code: "MANUAL_RESULT_UNKNOWN", preparation_disposition: "unknown_acknowledged" } }));
  fireEvent.click(screen.getByRole("button", { name: "核对原请求" }));
  await screen.findByRole("button", { name: "开始新的请求" });
  fetchMock.mockRejectedValueOnce(new Error("audit unavailable"));
  fireEvent.click(screen.getByRole("button", { name: "核对原请求" }));
  await screen.findByText(/提交结果尚未确认/);
  expect(screen.queryByRole("button", { name: "开始新的请求" })).not.toBeInTheDocument();
  expect(sessionStorage.getItem("nexus-manual-paper-attempt-v1")).not.toBeNull();
  expect(fetchMock.mock.calls.every((call) => call[1].method === "GET")).toBe(true);
});

it("shows verified current preparation cancellation, and revokes it when readback fails", async () => {
  sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "interrupted", mode: "research_only" }));
  fetchMock.mockResolvedValueOnce(Response.json({ data: { ...receipt, status: "unavailable", code: "MANUAL_PREPARATION_CANCELLED", preparation_recovery: { disposition: "cancelled_before_call", external_effects: "not_started", cycle_id: null } } }));
  render(<ManualPaperStart />);
  await screen.findByText(/已确认未开始外部操作/);
  expect(screen.getByRole("button", { name: "开始新的请求" })).toBeEnabled();
  fetchMock.mockRejectedValueOnce(new Error("recovery proof unavailable"));
  fireEvent.click(screen.getByRole("button", { name: "核对原请求" }));
  await screen.findByText(/提交结果尚未确认/);
  expect(screen.queryByText(/已确认未开始外部操作/)).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "开始新的请求" })).not.toBeInTheDocument();
  expect(fetchMock.mock.calls.every((call) => call[1].method === "GET")).toBe(true);
});

it("keeps uncertain preparation blocked after maintenance and links a reconciled original cycle", async () => {
  sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "uncertain", mode: "research_only" }));
  fetchMock.mockResolvedValueOnce(Response.json({ data: { ...receipt, status: "unknown", code: "MANUAL_RESULT_UNKNOWN", preparation_recovery: { disposition: "unknown_preserved", external_effects: "unknown", cycle_id: null } } }));
  render(<ManualPaperStart />);
  await screen.findByText(/费用和预算继续保留/);
  expect(screen.queryByRole("button", { name: "开始新的请求" })).not.toBeInTheDocument();
});

it("shows completed processing separately from per-stock quality and frozen research limitations", async () => {
  sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "pure-research", mode: "research_only" }));
  fetchMock.mockImplementation(async () => Response.json({ data: researchReceipt }));
  render(<ManualPaperStart />);
  await screen.findByText("研究处理已完成");
  expect(screen.getAllByText("内容受限")).toHaveLength(2);
  expect(screen.getByText(/历史观察资料/)).toBeVisible();
  expect(screen.getByText(/远端最新状态未核实/)).toBeVisible();
  expect(screen.getByText(/不构成正式决策/)).toBeVisible();
  expect(screen.getByText(researchReceipt.research.research_as_of)).toHaveAttribute("dateTime", researchReceipt.research.research_as_of);
  const links = screen.getAllByRole("link", { name: /查看第.*只证券研究/ });
  expect(links).toHaveLength(3);
  for (const [index, member] of researchReceipt.research.members.entries()) {
    expect(links[index]).toHaveAttribute("href", `/research/${member.instrument_id}?run_id=${member.run_id}`);
  }
  expect(screen.queryByRole("link", { name: /周期决策/ })).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "核对原请求" }));
  await screen.findByText("研究处理已完成");
  expect(fetchMock.mock.calls.every((call) => call[1].method === "GET")).toBe(true);
});

it("retains unknown usage and blocks a new start after pure research closeout", async () => {
  sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "pure-unknown", mode: "research_only" }));
  fetchMock.mockResolvedValue(Response.json({ data: {
    ...researchReceipt, status: "unknown", code: "MANUAL_RESULT_UNKNOWN",
    research: { ...researchReceipt.research, members: researchReceipt.research.members.map((member, index) => index === 1 ? { ...member, status: "failed", quality: null, usage_known: false } : member) },
  } }));
  render(<ManualPaperStart />);
  await screen.findByText("研究结果尚未确认");
  expect(screen.getByText("用量未知，保留预算占用")).toBeVisible();
  expect(screen.getByText("内容质量未知")).toBeVisible();
  expect(screen.queryByRole("button", { name: "开始新的请求" })).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "仅研究" })).toBeDisabled();
  expect(fetchMock.mock.calls[0][1].method).toBe("GET");
});

it("does not present request time as the frozen cutoff while pure research is queued", async () => {
  sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "pure-queued", mode: "research_only" }));
  fetchMock.mockResolvedValue(Response.json({ data: {
    ...researchReceipt, status: "queued", code: "MANUAL_REQUEST_QUEUED", research: null,
  } }));
  render(<ManualPaperStart />);
  await screen.findByText("已登记，等待宿主处理");
  expect(screen.getByText(/研究资料尚未冻结/)).toBeVisible();
  expect(screen.queryByText(researchReceipt.research.research_as_of)).not.toBeInTheDocument();
  expect(screen.queryByRole("link")).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "开始新的请求" })).not.toBeInTheDocument();
  expect(fetchMock.mock.calls[0][1].method).toBe("GET");
});

it("renders unrecognized member state and quality as unknown without exposing raw strings", async () => {
  sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "pure-active", mode: "research_only" }));
  fetchMock.mockResolvedValue(Response.json({ data: {
    ...researchReceipt, status: "active", code: "MANUAL_RESEARCH_STARTED",
    research: { ...researchReceipt.research, members: [{ ...researchReceipt.research.members[0], status: "private-status", quality: "private-quality" }] },
  } }));
  render(<ManualPaperStart />);
  await screen.findByText("研究正在处理");
  expect(screen.getByText("处理状态未知")).toBeVisible();
  expect(screen.getByText("内容质量未知")).toBeVisible();
  expect(screen.queryByText(/private/)).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "开始新的请求" })).not.toBeInTheDocument();
});

it("starts independent research once and retains unknown identity across refresh", async () => {
  const independent = { ...researchReceipt, mode: "research_and_monitor", research_contract: "independent_monitor_research_v1", status: "unknown", code: "MANUAL_RESULT_UNKNOWN" };
  fetchMock.mockResolvedValue(Response.json({ data: independent }));
  const view = render(<ManualPaperStart />);
  await waitFor(() => expect(screen.getByRole("button", { name: "研究并持续监控" })).toBeEnabled());
  expect(fetchMock).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "研究并持续监控" }));
  await screen.findByText(/通过审计的条件可进入持续监控/);
  expect(fetchMock.mock.calls[0][1].body).toBe('{"mode":"research_and_monitor"}');
  expect(screen.queryByRole("button", { name: "开始新的请求" })).not.toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "查看周期决策" })).not.toBeInTheDocument();
  view.unmount(); render(<ManualPaperStart />);
  await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
  expect(fetchMock.mock.calls[1][1].method).toBe("GET");
  expect(screen.getByRole("button", { name: "研究并持续监控" })).toBeDisabled();
});

it.each(["unknown_acknowledged", "recheck_required"])("shows independent disposition %s without releasing an unknown attempt", async (independent_disposition) => {
  sessionStorage.setItem("nexus-manual-paper-attempt-v1", JSON.stringify({ key: "independent-unknown", mode: "research_and_monitor" }));
  fetchMock.mockResolvedValue(Response.json({ data: { ...independentUnknownReceipt, independent_disposition } }));
  render(<ManualPaperStart />);
  await screen.findByText(independent_disposition === "unknown_acknowledged" ? /独立研究未知影响已明确接受/ : /独立研究的接受记录需要重新核验/);
  expect(screen.getByText("研究结果尚未确认")).toBeVisible();
  expect(screen.getByRole("button", { name: "研究并持续监控" })).toBeDisabled();
  expect(fetchMock).toHaveBeenCalledTimes(1);
  expect(fetchMock.mock.calls[0][1].method).toBe("GET");
  if (independent_disposition === "unknown_acknowledged") {
    fireEvent.click(screen.getByRole("button", { name: "开始新的请求" }));
    expect(sessionStorage.getItem("nexus-manual-paper-attempt-v1")).toBeNull();
    expect(screen.getByRole("button", { name: "研究并持续监控" })).toBeEnabled();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  } else {
    expect(screen.queryByRole("button", { name: "开始新的请求" })).toBeNull();
  }
});
