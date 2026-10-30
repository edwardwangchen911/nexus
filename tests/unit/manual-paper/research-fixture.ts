export const researchReceipt = {
  request_id: "0198f7a0-1111-7777-8111-111111111111",
  mode: "research_only",
  requested_at: "2026-10-21T23:50:00Z",
  status: "completed",
  code: "MANUAL_RESEARCH_FINISHED",
  cycle_id: null,
  execution_locked: true,
  research_contract: "research_only_v2",
  research: {
    manifest_id: "0198f7a0-2222-7777-8111-111111111111",
    research_as_of: "2026-10-22T00:10:00Z",
    manifest_hash: "a".repeat(64),
    remote_latest_verified: false,
    execution_locked: true,
    members: [
      { instrument_id: "0198f7a0-3333-7777-8111-111111111111", run_id: "0198f7a0-4444-7777-8111-111111111111", job_id: "0198f7a0-5555-7777-8111-111111111111", status: "completed", quality: "partial", usage_known: true },
      { instrument_id: "0198f7a0-3333-7777-8111-222222222222", run_id: "0198f7a0-4444-7777-8111-222222222222", job_id: "0198f7a0-5555-7777-8111-222222222222", status: "completed", quality: "complete", usage_known: true },
      { instrument_id: "0198f7a0-3333-7777-8111-333333333333", run_id: "0198f7a0-4444-7777-8111-333333333333", job_id: "0198f7a0-5555-7777-8111-333333333333", status: "completed", quality: "partial", usage_known: true },
    ],
  },
};

export const independentUnknownReceipt = {
  ...researchReceipt,
  mode: "research_and_monitor",
  research_contract: "independent_monitor_research_v1",
  status: "unknown",
  code: "MANUAL_RESULT_UNKNOWN",
  research: {
    ...researchReceipt.research,
    members: researchReceipt.research.members.map((member, index) => ({
      ...member, status: index === 0 ? "failed" : "abstained", quality: null, usage_known: index !== 0,
    })),
  },
};
