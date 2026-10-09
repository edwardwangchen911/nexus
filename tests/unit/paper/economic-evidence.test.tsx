import { render, screen, waitFor, within } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { EconomicEvidenceLoader } from "../../../app/components/paper/economic-evidence";

afterEach(() => { vi.unstubAllGlobals(); });

const report = {
  as_of: "2026-10-08T09:00:00Z", execution_locked: true,
  protocol_status: "not_implemented", economic_status: "not_evaluated",
  planned_slot_count: null, economic_pair_count: null,
  source_status: "ready", source_reason: null,
  ordinary_samples: [{ cycle_id: "019abcde-1234-7123-8123-123456789abc", decision_slot_at: "2026-10-07T14:00:00Z", execution_state: "no_order", performance_state: "account_observed" }],
};

it("shows an ordinary no-order observation without inventing economic pairs or returns", async () => {
  const fetcher = vi.fn(async () => Response.json({ data: report }));
  vi.stubGlobal("fetch", fetcher);
  render(<EconomicEvidenceLoader />);
  await screen.findByText("普通组合复盘样本：1（最多读取 20 条）");
  expect(screen.getByText("经济配对与计划槽位数量：尚未具备权威，无法统计")).toBeVisible();
  expect(screen.getByText("经济结果尚未评估，不能证明策略有效或样本外通过。")).toBeVisible();
  expect(screen.getByRole("link", { name: "查看关联决策" })).toHaveAttribute("href", `/decisions/${report.ordinary_samples[0].cycle_id}`);
  expect(screen.queryByText("PASS")).not.toBeInTheDocument();
  expect(fetcher).toHaveBeenCalledTimes(1);
  expect(fetcher).toHaveBeenCalledWith("/api/v1/paper/economic-evidence", expect.objectContaining({ cache: "no-store" }));
});

it("keeps unavailable reads unknown instead of presenting zero samples", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json({}, { status: 503 })));
  render(<EconomicEvidenceLoader />);
  await screen.findByText("组合经济证据暂不可读；样本数量与评价状态未知。");
  expect(screen.queryByText(/普通组合复盘样本：0/)).not.toBeInTheDocument();
});

it("rejects a response claiming economic PASS through this evidence-only interface", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json({ data: { ...report, economic_status: "PASS" } })));
  render(<EconomicEvidenceLoader />);
  await waitFor(() => expect(screen.getByText("组合经济证据暂不可读；样本数量与评价状态未知。")).toBeVisible());
});

it("reads a registered blocked protocol without suggesting activation", async () => {
  const slots = Array.from({ length: 24 }, (_, i) => ({ ordinal: i + 1, phase: i < 4 ? "training" : "oos", decision_at: new Date(Date.UTC(2027, 0, 4 + i, 15)).toISOString() }));
  vi.stubGlobal("fetch", vi.fn(async () => Response.json({ data: { ...report,
    protocol_status: "registered_not_activated", planned_slot_count: 24,
    protocol: { protocol_hash: "a".repeat(64), registered_at: report.as_of,
      daily_model_jobs: 6, daily_sdk_calls: 14, slots,
      daily_model_input_cap: 576000, daily_wire_input_cap: 512000,
      daily_output_cap: 49152, serial_invocation_ceiling_seconds: 570,
      activation_blockers: ["FIXED_STRATEGY_BASELINE_UNIMPLEMENTED", "COMMON_EXECUTION_WINDOW_UNVERIFIED"],
    },
  } })));
  render(<EconomicEvidenceLoader />);
  await screen.findByText("评价协议已登记，尚未激活。");
  expect(screen.getByText("计划槽位：24（训练 4 / 样本外 20）；经济配对数量仍未知。")).toBeVisible();
  expect(screen.getByText("固定策略基线尚未实现")).toBeVisible();
  expect(within(screen.getByRole("table", { name: "经济评价计划槽位" })).getAllByRole("row")).toHaveLength(25);
  expect(screen.queryByRole("button", { name: /激活|开始|运行/ })).not.toBeInTheDocument();
});

it("reports corrupted registration separately from absent registration", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => Response.json({ data: { ...report, protocol_status: "unavailable", protocol: null } })));
  render(<EconomicEvidenceLoader />);
  await screen.findByText("评价协议登记证据不可核实，计划数量未知。");
  expect(screen.queryByText("多股票经济评价协议与双账户执行入口尚未实现。")).not.toBeInTheDocument();
});
