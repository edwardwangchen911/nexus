import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import StrategiesPage, { StrategiesPanel } from "../../../app/(protected)/strategies/page";
import type { DataStateKind } from "../../../app/components/data-state";
import { actionEnvelope, detail, detailEnvelope, overview, overviewEnvelope } from "../strategy-evolution/fixtures";

const states: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"],
  ["empty", "暂无数据"],
  ["ready", "数据已就绪"],
  ["partial", "数据不完整"],
  ["stale", "显示上次数据"],
  ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

const callbacks = {
  onRefresh: vi.fn(),
  onSelectExperiment: vi.fn(),
  onStartExperiment: vi.fn(),
  onControlRegistry: vi.fn(),
  onVetoExperiment: vi.fn(),
  onRollbackRegistry: vi.fn(),
  onRetryUnknown: vi.fn(),
};

function renderPanel(state: DataStateKind, overrides = {}) {
  return render(<StrategiesPanel
    state={state}
    overview={["ready", "partial", "stale"].includes(state) ? overview : null}
    detail={["ready", "partial", "stale"].includes(state) ? detail : null}
    etag={state === "ready" ? '"g1-v1"' : null}
    busyAction={null}
    errorMessage={null}
    successMessage={null}
    unknownResult={false}
    {...callbacks}
    {...overrides}
  />);
}

describe("strategy evolution workbench", () => {
  it("keeps gate levels as percent and differences as percentage points", () => {
    const gates = [
      { stage: "shadow", gate: "nonnegative_excess_return", status: "PASS", observed: ["champion=0.05", "challenger=0.07"], thresholds: ["minimum_delta=0"], reason_code: "PASS" },
      { stage: "shadow", gate: "maximum_drawdown_degradation", status: "PASS", observed: ["champion=-0.12", "challenger=-0.13"], thresholds: ["minimum_delta=-0.02"], reason_code: "PASS" },
      { stage: "historical", gate: "double_cost_excess_positive", status: "PASS", observed: ["challenger_minus_champion=0.025"], thresholds: ["minimum_exclusive=0"], reason_code: "PASS" },
    ];
    renderPanel("ready", { detail: { ...detail, gates } });
    expect(screen.getByText("主用策略：5% · 候选策略：7%")).toBeVisible();
    expect(screen.getByText("主用策略：-12% · 候选策略：-13%")).toBeVisible();
    expect(screen.getByText("最低差值：-2 个百分点")).toBeVisible();
    expect(screen.getByText("候选减主用：2.5 个百分点")).toBeVisible();
  });
  afterEach(() => {
    vi.clearAllMocks();
    vi.unstubAllGlobals();
  });

  it.each(states)("renders the visible %s state and the immutable live boundary", (state, label) => {
    renderPanel(state);
    expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    expect(screen.getByRole("region", { name: "策略进化工作台" })).toHaveAttribute("data-state", state);
    expect(screen.getByText("实盘关闭")).toBeVisible();
  });

  it("renders the complete evidence lifecycle without a trade surface", () => {
    renderPanel("ready", { overview: { ...overview, active_family: { ...overview.active_family, next_step: "Accumulate 5/21/63 session checkpoints." } } });

    expect(screen.getByRole("heading", { name: "baseline_v1" })).toBeVisible();
    expect(screen.getByText("策略进化 · 证据账簿")).toBeVisible();
    expect(screen.getByText("01 / 当前主用策略")).toBeVisible();
    expect(screen.getByText("提高流动性门槛可减少成本侵蚀，同时保留中期动量溢价。")).toBeVisible();
    expect(screen.getByText("近 20 个交易日最低平均成交额 · 3 个候选值")).toBeVisible();
    expect(screen.getByRole("heading", { name: "证据比较" })).toBeVisible();
    expect(screen.getByText("SPY 比较完整")).toBeVisible();
    expect(screen.getByText("等权白名单比较完整")).toBeVisible();
    expect(screen.getByText("21 / 63 个交易日")).toBeVisible();
    expect(screen.getByText("宽限期 5 个交易日")).toBeVisible();
    expect(screen.getByText("已等待 0 个交易日")).toBeVisible();
    expect(screen.getByText("检查点 5 · 已完成")).toBeVisible();
    expect(screen.getByText("检查点 21 · 已完成")).toBeVisible();
    expect(screen.getByText("检查点 63 · 等待")).toBeVisible();
    expect(screen.getByRole("heading", { name: "晋级解释" })).toBeVisible();
    expect(screen.getAllByText("通过").length).toBeGreaterThan(0);
    expect(screen.getByRole("heading", { name: "冠军历史" })).toBeVisible();
    expect(screen.queryByRole("button", { name: /下单|交易|买入|卖出/ })).not.toBeInTheDocument();
    expect(document.body).not.toHaveTextContent(/投影版本|权威|门禁|Shadow/);
    expect(screen.getByText("积累 5、21、63 个交易日的观察检查点。")).toBeVisible();
  });

  it.each([
    ["Await deterministic admission.", "等待按固定规则核验准入条件。"],
    ["Run deterministic paired backtests.", "对候选策略与基准执行配对回测。"],
    ["Await point-in-time backtest evidence.", "等待按当时可用信息生成的回测证据。"],
    ["Start isolated Strategy Shadow.", "开始独立的策略前向观察。"],
    ["Await deterministic handover and boundary activation.", "等待按固定规则完成策略交接，并在规定时点生效。"],
    ["Champion transition is recorded.", "主用策略切换已记录。"],
    ["Experiment was vetoed; no promotion is permitted.", "实验已被否决，不允许晋级。"],
    ["Restart against the current champion generation.", "以当前主用策略版本为基准重新开始。"],
    ["No further automatic action is permitted.", "不允许继续自动操作。"],
    ["future_step", "尚无中文说明（future_step）"],
    ["保留原始说明", "保留原始说明"],
  ])("explains the fixed next step %s without changing unknown instructions", (next_step, label) => {
    renderPanel("ready", { overview: { ...overview, active_family: { ...overview.active_family, next_step } } });
    expect(screen.getByText("下一步").nextElementSibling).toHaveTextContent(label);
  });

  it.each([["PROMOTE", "晋级"], ["ROLLBACK", "回滚"]])("explains a pending %s transition without changing its session", (transition, label) => {
    renderPanel("ready", { overview: { ...overview, champion: { ...overview.champion, pending_transition: transition, pending_effective_session: "2026-09-04" } } });
    expect(screen.getByText("待生效切换").nextElementSibling).toHaveTextContent(`${label} · 2026-09-04`);
  });

  it("explains missing strategy evidence without internal terminology", () => {
    renderPanel("empty");
    expect(screen.getByText(/不使用浏览器默认值/)).not.toHaveTextContent(/权威|门禁|Shadow/);
  });

  it("keeps the verified champion and registry history visible before the first experiment", () => {
    renderPanel("empty", {
      overview: {
        ...overview,
        status: "empty",
        active_family: null,
        experiments: [],
        experiment_start_enabled: false,
        experiment_start_reason: "LINEAGE_AUTHORITY_INCOMPLETE",
      },
      detail: null,
      etag: '"g1-v1"',
    });

    expect(screen.getByRole("heading", { name: "baseline_v1" })).toBeVisible();
    expect(screen.getByRole("heading", { name: "冠军历史" })).toBeVisible();
    expect(screen.getByText("当前没有活动实验组；Agent 不会为空证据生成默认候选。")).toBeVisible();
    expect(screen.getByText(
      "暂不能启动实验：缺少已核验的研究版本来源，暂不能生成策略实验（LINEAGE_AUTHORITY_INCOMPLETE）",
    )).toBeVisible();
    expect(screen.queryByRole("heading", { name: "策略进化证据暂不可用" })).not.toBeInTheDocument();

    const start = screen.getByRole("button", { name: "启动实验" });
    const pause = screen.getByRole("button", { name: "暂停自动晋级" });
    const rollback = screen.getByRole("button", { name: "回滚上一个冠军" });
    expect(start).toBeDisabled();
    expect(pause).toBeDisabled();
    expect(rollback).toBeDisabled();
    fireEvent.click(start);
    fireEvent.click(pause);
    fireEvent.click(rollback);
    expect(callbacks.onStartExperiment).not.toHaveBeenCalled();
    expect(callbacks.onControlRegistry).not.toHaveBeenCalled();
    expect(callbacks.onRollbackRegistry).not.toHaveBeenCalled();
  });

  it.each(["partial", "stale"] as const)("disables every mutation when evidence is %s", (state) => {
    renderPanel(state);
    for (const button of screen.getAllByRole("button")) {
      if (button.getAttribute("aria-label") === "刷新策略进化证据") expect(button).toBeEnabled();
      else expect(button).toBeDisabled();
    }
  });

  it("traps and restores focus in the fixed-copy rollback confirmation", async () => {
    renderPanel("ready");
    const rollback = screen.getByRole("button", { name: "回滚上一个冠军" });
    fireEvent.click(rollback);

    const dialog = screen.getByRole("dialog", { name: "确认回滚策略冠军" });
    const cancel = screen.getByRole("button", { name: "取消" });
    const confirm = screen.getByRole("button", { name: "确认回滚" });
    expect(dialog).toHaveTextContent("只影响未来内部模拟决策");
    expect(dialog).toHaveTextContent("不会撤销历史成交");
    expect(dialog).toHaveTextContent("不会创建任何真实订单");
    await waitFor(() => expect(confirm).toHaveFocus());
    fireEvent.keyDown(dialog, { key: "Tab" });
    expect(cancel).toHaveFocus();
    fireEvent.keyDown(dialog, { key: "Tab" });
    expect(confirm).toHaveFocus();
    fireEvent.click(confirm);
    expect(callbacks.onRollbackRegistry).toHaveBeenCalledOnce();
    await waitFor(() => expect(rollback).toHaveFocus());
  });

  it("focuses the safe error summary and exposes a polite success status", async () => {
    renderPanel("ready", { errorMessage: "资源已变化，请刷新后重试。", successMessage: "已暂停自动晋级。" });
    await waitFor(() => expect(screen.getByRole("alert")).toHaveFocus());
    expect(screen.getByRole("status")).toHaveTextContent("已暂停自动晋级。");
  });

  it("closes new mutations while an unknown result can only replay the original command", () => {
    renderPanel("ready", {
      unknownResult: true,
      errorMessage: "操作结果未知。",
    });

    expect(screen.getByRole("region", { name: "未知操作恢复" })).toHaveTextContent("继续确认会复用原请求，不会重复创建操作");
    expect(screen.getByRole("button", { name: "暂停自动晋级" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "否决本次晋级" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "继续确认原操作结果" }));
    expect(callbacks.onRetryUnknown).toHaveBeenCalledOnce();
  });

  it("refetches after a 409 and clears all protected evidence after authorization is revoked", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: { code: "VERSION_CONFLICT" } }), { status: 409 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v2"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: { etag: '"g1-v2"' } }))
      .mockResolvedValueOnce(new Response(null, { status: 403 }));
    vi.stubGlobal("fetch", fetchMock);
    render(<StrategiesPage />);

    expect(await screen.findByRole("heading", { name: "baseline_v1" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "暂停自动晋级" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(5));
    expect(screen.getByRole("alert")).toHaveTextContent("资源已变化");

    fireEvent.click(screen.getByRole("button", { name: "刷新策略进化证据" }));
    expect(await screen.findByRole("status", { name: "无权访问" })).toBeVisible();
    expect(screen.queryByRole("heading", { name: "baseline_v1" })).not.toBeInTheDocument();
  });

  it("keeps accepted actions pending until the refreshed authority confirms them", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(actionEnvelope("rollback_registry")), { status: 202, headers: { location: "/api/v1/strategy-evolution" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v2"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: { etag: '"g1-v2"' } }));
    vi.stubGlobal("fetch", fetchMock);
    render(<StrategiesPage />);

    expect(await screen.findByRole("heading", { name: "baseline_v1" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "回滚上一个冠军" }));
    fireEvent.click(screen.getByRole("button", { name: "确认回滚" }));
    expect(await screen.findByText("回滚已接受，正在等待确定性边界生效。")).toBeVisible();
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(5));
  });

  it("fails closed when experiment evidence cannot be bound to the overview authority", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ error: { code: "AUTHORITY_UNAVAILABLE" } }), { status: 503 }));
    vi.stubGlobal("fetch", fetchMock);
    render(<StrategiesPage />);

    expect(await screen.findByText("数据不完整")).toBeVisible();
    expect(screen.getByRole("heading", { name: "baseline_v1" })).toBeVisible();
    for (const button of screen.getAllByRole("button")) {
      if (button.getAttribute("aria-label") !== "刷新策略进化证据") expect(button).toBeDisabled();
    }
  });

  it.each([
    ["missing overview ETag", null, '"g1-v1"'],
    ["missing detail ETag", '"g1-v1"', null],
    ["mismatched detail ETag", '"g1-v1"', '"g1-v2"'],
  ])("fails closed and clears detail evidence for %s", async (_case, overviewEtag, detailEtag) => {
    const overviewHeaders: HeadersInit = overviewEtag === null ? {} : { etag: overviewEtag };
    const detailHeaders: HeadersInit = detailEtag === null ? {} : { etag: detailEtag };
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(overviewEnvelope()), { status: 200, headers: overviewHeaders }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: detailHeaders }));
    vi.stubGlobal("fetch", fetchMock);
    render(<StrategiesPage />);

    expect(await screen.findByText("数据不完整")).toBeVisible();
    expect(screen.getByRole("heading", { name: "baseline_v1" })).toBeVisible();
    expect(screen.getAllByText("当前没有可验证证据；不使用 0 或浏览器默认值代替。")).toHaveLength(3);
    expect(screen.queryByText("0.142")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "暂停自动晋级" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "回滚上一个冠军" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "否决本次晋级" })).not.toBeInTheDocument();
  });

  it.each(["identity", "etag"] as const)("fails closed on manual experiment-selection %s mismatch", async (mismatch) => {
    const secondExperiment = {
      ...structuredClone(overview.experiments[0]),
      experiment_id: "0198d500-1000-7000-8000-000000002299",
      candidate_ordinal: 2,
      strategy_version: "challenger_liquidity_v2",
    };
    const nextOverview = structuredClone(overview);
    nextOverview.experiments.push(secondExperiment);
    const selectedDetail = structuredClone(detail);
    selectedDetail.experiment = mismatch === "identity"
      ? structuredClone(detail.experiment)
      : structuredClone(secondExperiment);
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ ...overviewEnvelope(), data: nextOverview }), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify(detailEnvelope()), { status: 200, headers: { etag: '"g1-v1"' } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ...detailEnvelope(), data: selectedDetail }), {
        status: 200,
        headers: { etag: mismatch === "etag" ? '"g1-v2"' : '"g1-v1"' },
      }));
    vi.stubGlobal("fetch", fetchMock);
    render(<StrategiesPage />);

    fireEvent.click(await screen.findByRole("button", { name: "2 · challenger_liquidity_v2" }));
    expect(await screen.findByText("数据不完整")).toBeVisible();
    expect(screen.getAllByText("当前没有可验证证据；不使用 0 或浏览器默认值代替。")).toHaveLength(3);
    expect(screen.queryByRole("button", { name: "否决本次晋级" })).not.toBeInTheDocument();
  });
});
