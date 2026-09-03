import { fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import RiskAuditPage, { RiskAuditPanel } from "../../../app/(protected)/risk-audit/page";
import type { DataStateKind } from "../../../app/components/data-state";

const jobId = "0198f7a0-1111-7777-8111-111111111112";
const meta = { request_id: "0198f7a0-1111-7777-8111-111111111111", schema_version: "1", as_of: "2026-08-27T00:00:00Z" };

beforeEach(() => {
  window.history.replaceState({}, "", "/risk-audit?kind=job&from=2026-08-20&to=2026-08-27&status=attention");
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const states: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"], ["empty", "暂无数据"], ["ready", "数据已就绪"],
  ["partial", "数据不完整"], ["stale", "数据已过期"], ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

describe("risk-audit page", () => {
  it.each([["strategy.registry.paused", "自动晋级已暂停", "PAUSED"], ["strategy.registry.resumed", "自动晋级已恢复", "EFFECTIVE"]])("limits %s to automatic promotion without claiming strategy execution stopped", (action, label, status) => {
    const base = auditPage();
    render(<RiskAuditPanel state="ready" readiness={readiness("ready")}
      data={{ ...base, items: [{ ...base.items[0], action, kind: "audit", outcome: { status } }] }}
      kindFilter="" statusFilter="" fromDate="2026-08-20" toDate="2026-08-27" loadingMore={false} notice={null}
      onRefresh={vi.fn()} onFilters={vi.fn()} onLoadMore={vi.fn()} onRetry={vi.fn()} onAcknowledge={vi.fn()} onExport={vi.fn()} />);
    const table = screen.getByRole("table", { name: "脱敏审计事件" });
    expect(table).toHaveTextContent(`${label}（${action}）`);
    expect(table).not.toHaveTextContent(/策略已暂停|策略已恢复|已解除锁定/);
  });
  it("preserves unknown audit codes, hashes and versions without treating them as numbers or dates", () => {
    const base = auditPage();
    const digest = "0123456789abcdef".repeat(4);
    const outcome = { status: "future_state", retryable: false, error_code: "future_error", policy_version: "0.123456789", authority_hash: digest, event_type: "FUTURE_EVENT", result: true, future_field: "2026-09-03" };
    render(<RiskAuditPanel state="ready" readiness={readiness("ready")}
      data={{ ...base, items: [{ ...base.items[0], action: "future.action", actor_kind: "service", outcome }] }}
      kindFilter="" statusFilter="" fromDate="2026-08-20" toDate="2026-08-27" loadingMore={false} notice={null}
      onRefresh={vi.fn()} onFilters={vi.fn()} onLoadMore={vi.fn()} onRetry={vi.fn()} onAcknowledge={vi.fn()} onExport={vi.fn()} />);
    const table = screen.getByRole("table", { name: "脱敏审计事件" });
    for (const label of ["未识别动作（future.action）", "后台服务", "状态：未识别状态（future_state）", "可重试：否", "错误原因：尚无中文说明（future_error）", "政策版本：0.123456789", `证据摘要：${digest}`, "事件类型：FUTURE_EVENT", "结果：是", "未识别字段（future_field）：2026-09-03"]) expect(table).toHaveTextContent(label);
    expect(outcome.policy_version).toBe("0.123456789");
  });
  it.each(states)("renders the visible %s state and closes recovery outside ready", (state, label) => {
    const hasFacts = ["ready", "partial", "stale"].includes(state);
    render(<RiskAuditPanel
      state={state}
      readiness={hasFacts ? readiness("ready") : null}
      data={hasFacts ? auditPage() : null}
      kindFilter="job"
      statusFilter="attention"
      fromDate="2026-08-20"
      toDate="2026-08-27"
      loadingMore={false}
      notice={null}
      onRefresh={vi.fn()}
      onFilters={vi.fn()}
      onLoadMore={vi.fn()}
      onRetry={vi.fn()}
      onAcknowledge={vi.fn()}
      onExport={vi.fn()}
    />);

    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "风险与审计控制台" })).toHaveAttribute("data-state", state);
    if (!hasFacts) expect(screen.queryByText("daily_pipeline")).not.toBeInTheDocument();
    if (state === "ready") {
      expect(screen.getByRole("button", { name: "重试 daily_pipeline" })).toBeEnabled();
      expect(screen.getByText(/质量通过/)).toHaveTextContent("PASS");
      expect(screen.getByText("仅追加 · 已脱敏")).toBeVisible();
    }
    if (state === "partial" || state === "stale") {
      expect(screen.getByRole("button", { name: "重试 daily_pipeline" })).toBeDisabled();
      expect(screen.getByRole("button", { name: "确认解除回撤锁" })).toBeDisabled();
    }
  });

  it("loads closed readiness and audit facts, then filters event outcome locally", async () => {
    const calls: string[] = [];
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input); calls.push(url);
      if (url === "/api/v1/readiness") return Response.json({ data: readiness("ready"), meta });
      if (url.includes("/api/v1/risk-audit?")) return Response.json({ data: auditPage(), meta });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<RiskAuditPage />);
    expect(await screen.findByText("daily_pipeline")).toBeVisible();
    expect(calls).toContain("/api/v1/readiness");
    expect(calls).toContain("/api/v1/risk-audit?from=2026-08-20T00%3A00%3A00Z&kind=job&limit=50&to=2026-08-28T00%3A00%3A00Z");
    expect(screen.getByRole("table", { name: "脱敏审计事件" })).toHaveTextContent("job.failed");
    expect(screen.getByRole("table", { name: "脱敏审计事件" })).toHaveTextContent("任务失败");
    expect(screen.getByRole("table", { name: "脱敏审计事件" })).toHaveTextContent("系统");
    expect(screen.getByRole("table", { name: "脱敏审计事件" })).toHaveTextContent("可重试：是");
    expect(screen.getByRole("table", { name: "脱敏审计事件" })).toHaveTextContent("状态：失败");
    expect(screen.getByRole("table", { name: "脱敏审计事件" })).not.toHaveTextContent(/retryable:|status:|true/);
    expect(document.body).not.toHaveTextContent(/account_id|payload|secret/);

    fireEvent.change(screen.getByRole("combobox", { name: "结果状态" }), { target: { value: "recovered" } });
    expect(screen.queryByText(/任务失败（job\.failed）/)).not.toBeInTheDocument();
    expect(window.location.search).toContain("status=recovered");
  });

  it("confirms one manual-safe retry with the authoritative version and rereads facts", async () => {
    const requests: Array<{ url: string; init?: RequestInit }> = [];
    vi.spyOn(crypto, "randomUUID").mockReturnValue("0198f7a0-1111-7777-8111-111111111130");
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input); requests.push({ url, init });
      if (url === "/api/v1/readiness") return Response.json({ data: readiness("ready"), meta });
      if (url.includes("/api/v1/risk-audit?")) return Response.json({ data: auditPage(), meta });
      if (url.endsWith(`/jobs/${jobId}/retry`)) return Response.json({
        data: { job_id: jobId, status: "queued", version: 8, manual_retry_count: 1, replayed: false }, meta,
      });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<RiskAuditPage />);
    fireEvent.click(await screen.findByRole("button", { name: "重试 daily_pipeline" }));
    const dialog = screen.getByRole("dialog", { name: "确认重试 daily_pipeline" });
    expect(dialog).toHaveTextContent("只重新排队这个允许安全重试的任务");
    expect(within(dialog).getByRole("button", { name: "返回审计" })).toHaveFocus();
    fireEvent.click(within(dialog).getByRole("button", { name: "确认安全重试" }));

    expect(await screen.findByText("任务已重新排队；已更新记录。")).toBeVisible();
    const post = requests.find((request) => request.url.endsWith(`/jobs/${jobId}/retry`));
    expect(post?.init?.method).toBe("POST");
    expect(new Headers(post?.init?.headers).get("if-match")).toBe('"7"');
    expect(new Headers(post?.init?.headers).get("idempotency-key")).toBe("risk-retry-0198f7a0-1111-7777-8111-111111111130");
    expect(post?.init?.body).toBe("{}");
  });

  it("reuses one idempotency key when a retry result is unknown", async () => {
    const keys: Array<string | null> = [];
    let retries = 0;
    vi.spyOn(crypto, "randomUUID").mockReturnValue("0198f7a0-1111-7777-8111-111111111131");
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === "/api/v1/readiness") return Response.json({ data: readiness("ready"), meta });
      if (url.includes("/api/v1/risk-audit?")) return Response.json({ data: auditPage(), meta });
      if (url.endsWith(`/jobs/${jobId}/retry`)) {
        retries += 1;
        keys.push(new Headers(init?.headers).get("idempotency-key"));
        if (retries === 1) throw new TypeError("network result unknown");
        return Response.json({ data: { job_id: jobId, status: "queued", version: 8, manual_retry_count: 1, replayed: true }, meta });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<RiskAuditPage />);
    fireEvent.click(await screen.findByRole("button", { name: "重试 daily_pipeline" }));
    fireEvent.click(screen.getByRole("button", { name: "确认安全重试" }));
    expect(await screen.findByText("恢复结果未知。再次确认会复用原请求；其他恢复操作暂不可用。")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "继续确认原操作结果" }));
    expect(await screen.findByText("任务已重新排队；已更新记录。")).toBeVisible();
    expect(retries).toBe(2);
    expect(new Set(keys).size).toBe(1);
  });

  it("acknowledges only the shown drawdown event sequence", async () => {
    let acknowledgement: { body?: string; version?: string | null } = {};
    vi.spyOn(crypto, "randomUUID").mockReturnValue("0198f7a0-1111-7777-8111-111111111132");
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url === "/api/v1/readiness") return Response.json({ data: readiness("ready"), meta });
      if (url.includes("/api/v1/risk-audit?")) return Response.json({ data: auditPage(), meta });
      if (url.endsWith("/risk/drawdown-lock/acknowledge")) {
        acknowledgement = { body: String(init?.body), version: new Headers(init?.headers).get("if-match") };
        return Response.json({ data: { state: "unlocked", event_seq: 13, replayed: false }, meta });
      }
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<RiskAuditPage />);
    fireEvent.click(await screen.findByRole("button", { name: "确认解除回撤锁" }));
    const dialog = screen.getByRole("dialog", { name: "确认解除回撤锁" });
    expect(dialog).toHaveTextContent("不会创建或提交订单");
    fireEvent.click(within(dialog).getByRole("button", { name: "确认解除" }));
    expect(await screen.findByText("回撤锁已确认解除；已更新记录。")).toBeVisible();
    expect(acknowledgement).toEqual({ body: "{\"expected_event_seq\":12}", version: '"12"' });
  });

  it("treats a 503 readiness envelope as stale and does not expose active recovery", async () => {
    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url === "/api/v1/readiness") return Response.json({ data: readiness("unready"), meta }, { status: 503 });
      if (url.includes("/api/v1/risk-audit?")) return Response.json({ data: auditPage(), meta });
      throw new Error(`unexpected request: ${url}`);
    }));

    render(<RiskAuditPage />);
    expect(await screen.findByText("依赖未就绪；保留最后读取的脱敏事实，恢复动作保持关闭。")).toBeVisible();
    expect(screen.getByRole("region", { name: "风险与审计控制台" })).toHaveAttribute("data-state", "stale");
    expect(screen.getByRole("button", { name: "重试 daily_pipeline" })).toBeDisabled();
  });
});

function readiness(status: "ready" | "unready") {
  return {
    status,
    components: {
      database: status === "ready" ? "ready" as const : "unavailable" as const,
      migration: "ready" as const,
      object_store: "ready" as const,
      worker: "ready" as const,
    },
  };
}

function auditPage() {
  return {
    summary: {
      data_quality: "PASS" as const,
      drawdown_lock: { state: "recovery_eligible" as const, event_seq: 12, acknowledge_allowed: true },
      failed_jobs: {
        count: 1,
        retryable: [{
          job_id: jobId,
          job_type: "daily_pipeline",
          status: "failed" as const,
          version: 7,
          manual_retry_count: 0,
          last_error_code: "PROVIDER_UNAVAILABLE",
          finished_at: "2026-08-26T23:30:00Z",
        }],
      },
    },
    items: [{
      event_seq: 9,
      occurred_at: "2026-08-26T23:30:00Z",
      kind: "job" as const,
      actor_kind: "system" as const,
      action: "job.failed",
      schema_version: "1",
      outcome: { status: "failed", retryable: true, error_code: "PROVIDER_UNAVAILABLE" },
      previous_hash: "a".repeat(64),
      event_hash: "b".repeat(64),
    }],
    next_cursor: null,
  };
}
