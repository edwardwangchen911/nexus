import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ResearchPanel } from "../../../app/(protected)/research/[instrumentId]/page";
import { dataSnapshotId, navHistoryInputs, researchAdmissionData, researchData } from "./fixtures";

const newerSnapshot = "0198d600-1000-7000-8000-000000001588";

describe("research and visible inputs have separate frozen identities", () => {
  it.each(["failed", "completed"])("explains different snapshots without altering a %s run", (status) => {
    render(<ResearchPanel state="partial" research={researchData({
      status, intent: null, quality_status: status === "failed" ? null : "partial",
    })} admission={researchAdmissionData({ inputs: navHistoryInputs(), data_snapshot_id: newerSnapshot })}
    submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.getByText(/当前资料与下方研究使用不同冻结快照/)).toBeVisible();
    expect(screen.getByText(/不能把当前资料视为下方研究已经采用的完整输入/)).toBeVisible();
    expect(screen.getByRole("region", { name: "发行商净值历史资料" })).toBeVisible();
    expect(screen.getByRole("region", { name: "个股研究证据页" })).toHaveAttribute("data-state", "partial");
    expect(screen.queryByRole("button", { name: /运行冻结研究/ })).not.toBeInTheDocument();
  });

  it.each(["same", "no-inputs", "no-run", "forbidden", "loading"])("does not infer a mismatch for %s", (scenario) => {
    render(<ResearchPanel state={scenario === "forbidden" ? "forbidden" : scenario === "loading" ? "loading" : "partial"}
      research={scenario === "no-run" ? null : researchData()}
      admission={researchAdmissionData({
        inputs: scenario === "no-inputs" ? null : navHistoryInputs(),
        data_snapshot_id: scenario === "same" ? dataSnapshotId : newerSnapshot,
      })} submitting={false} notice={null} onRefresh={vi.fn()} onRun={vi.fn()} />);
    expect(screen.queryByText(/当前资料与下方研究使用不同冻结快照/)).not.toBeInTheDocument();
  });
});
