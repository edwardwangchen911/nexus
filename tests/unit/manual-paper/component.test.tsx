import { fireEvent, render, screen, waitFor, cleanup } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ManualPaperStart } from "../../../app/components/workbench/manual-paper-start";
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
