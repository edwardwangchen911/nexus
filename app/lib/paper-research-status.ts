import type { components } from "./generated/schema";

type Cycle = components["schemas"]["AgentPaperCycleResponse"];
type Candidate = Cycle["candidates"][number];

export function researchJobLabel(candidate: Candidate, cycle: Cycle): string {
  switch (candidate.research_job_status) {
    case "queued":
      return cycle.status === "failed" || cycle.status === "unavailable" ? "未开始" : "待运行";
    case "running": return "运行中";
    case "succeeded": return "任务成功";
    case "failed":
    case "dead_letter": return "任务失败";
    case "skipped": return "已跳过";
    default: return "未知";
  }
}

export function researchAdmissionLabel(candidate: Candidate, cycle: Cycle): string {
  if (candidate.research_status === "completed") return "已纳入决策";
  if (candidate.research_status === "unavailable") return "未通过准入";
  if (cycle.status === "failed" || cycle.status === "unavailable") return "未纳入决策";
  return "尚未决策";
}

export function hasFailedResearchJob(cycle: Cycle): boolean {
  return cycle.candidates.some((candidate) =>
    candidate.research_job_status === "failed" || candidate.research_job_status === "dead_letter",
  );
}

export function researchExecutionFailureExplanation(cycle: Cycle): string | null {
  return cycle.status === "unavailable"
    && cycle.result_code === "RESEARCH_OUTPUT_NOT_ACTIONABLE"
    && hasFailedResearchJob(cycle)
    ? "研究任务运行失败，未形成可核验的研究结果；本周期未创建模拟订单。"
    : null;
}
