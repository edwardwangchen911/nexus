"use client";

import { LockKeyhole, RefreshCw, ShieldCheck } from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { DataState, dataStateLabels, type DataStateKind } from "../../components/data-state";
import type { components } from "../../lib/generated/schema";

type Strategy = components["schemas"]["StrategySummaryResponse"];
type StrategyEnvelope = components["schemas"]["StrategyListEnvelope"];
type Props = { state: DataStateKind; strategies: Strategy[]; onRefresh: () => void };

export default function StrategiesPage(): React.ReactElement {
  const [state, setState] = useState<DataStateKind>("loading");
  const [strategies, setStrategies] = useState<Strategy[]>([]);
  const strategiesRef = useRef<Strategy[]>([]);
  const authorizedRef = useRef(false);
  const refresh = useCallback(async () => {
    setState("loading");
    try {
      const response = await fetch("/api/v1/strategies", { headers: { accept: "application/json" } });
      if (response.status === 401 || response.status === 403) {
        authorizedRef.current = false;
        strategiesRef.current = [];
        setStrategies([]);
        setState("forbidden");
        return;
      }
      if (!response.ok) { setState(response.status === 409 || response.status === 422 ? "partial" : "error"); return; }
      const data = [...(await response.json() as StrategyEnvelope).data];
      authorizedRef.current = true;
      strategiesRef.current = data;
      setStrategies(data);
      setState(data.length ? "ready" : "empty");
    } catch {
      const safeFallback = authorizedRef.current ? strategiesRef.current : [];
      setStrategies(safeFallback);
      setState(safeFallback.length ? "stale" : "error");
    }
  }, []);
  useEffect(() => {
    const timer = globalThis.setTimeout(() => void refresh(), 0);
    return () => globalThis.clearTimeout(timer);
  }, [refresh]);
  return <StrategiesPanel state={state} strategies={strategies} onRefresh={() => void refresh()} />;
}

export function StrategiesPanel({ state, strategies, onRefresh }: Props): React.ReactElement {
  const baseline = strategies.find((strategy) => strategy.strategy_version === "baseline_v1") ?? null;
  const hasData = baseline !== null && ["ready", "partial", "stale"].includes(state);
  const latest = baseline?.latest_backtest ?? null;
  return (
    <section className="page-stack strategies-page" aria-label="策略与实验页面" data-state={state}>
      <header className="page-heading">
        <div>
          <p className="eyebrow">STRATEGY / GOVERNANCE LEDGER</p>
          <h1>策略与实验</h1>
          <p>先固定可复现基线，再比较挑战者。当前只读，不会因回测或 Agent 结论创建真实订单。</p>
        </div>
        <div className="page-actions">
          <span className="mode-badge">EVOLUTION_LOCKED</span>
          <span className="state-chip" data-state={state}>{dataStateLabels[state]}</span>
          <button className="button button--quiet" type="button" onClick={onRefresh}>
            <RefreshCw aria-hidden="true" size={17} />刷新策略
          </button>
        </div>
      </header>

      {!hasData ? (
        <DataState
          state={state}
          title={state === "loading" ? "正在读取策略注册表" : "策略注册表暂不可用"}
          detail="不使用浏览器默认值补齐冠军或回测结果。"
        />
      ) : (
        <>
          {state === "stale" ? <p className="inline-notice inline-notice--error" role="status">显示最后已知策略边界；当前数据不可用于新实验。</p> : null}
          <section className="strategy-policy-card" aria-labelledby="baseline-title">
            <header className="strategy-policy-card__heading">
              <div>
                <p className="eyebrow">PUBLISHED POLICY / EVIDENCE-GATED</p>
                <h2 id="baseline-title">{baseline.strategy_version}</h2>
              </div>
              <div className="strategy-policy-card__badges" aria-label="策略状态">
                <span className="mode-badge">当前唯一冠军</span>
                <span className="mode-badge">策略进化未启用</span>
              </div>
            </header>

            <dl className="strategy-boundary-ledger numeric-data">
              <div><dt>调仓频率</dt><dd>每周最后一个 session</dd></div>
              <div><dt>成交时点</dt><dd>下一交易日</dd></div>
              <div><dt>最大持仓</dt><dd>6 只 · 单只 12.5%</dd></div>
              <div><dt>最低现金</dt><dd>25%</dd></div>
            </dl>

            <ol className="strategy-rule-flow" aria-label="baseline_v1 确定性规则">
              <li><span>01</span><div><h3>准入政策</h3><p>要求冻结的 PASS 美股权威、ADV20 ≥ 5,000 万美元、收盘价 ≥ 5 美元、高于 SMA200、126 日收益为正。</p></div></li>
              <li><span>02</span><div><h3>排名政策</h3><p>要求同一冻结复权序列计算 momentum_126_skip5；缺失权威时不生成可持久化证据。</p></div></li>
              <li><span>03</span><div><h3>组合约束</h3><p>ETF 最多 2 只，同行业股票最多 2 只；ETF 按冻结行业暴露穿透，行业总权重不超过 30%，空缺保持现金。</p></div></li>
              <li><span>04</span><div><h3>退出边界</h3><p>历史基线处理趋势与排名退出；硬风控退出属于后续前向执行管道，当前未启用。</p></div></li>
            </ol>
          </section>

          <section className="strategy-guardrails" aria-label="策略执行边界">
            <div><ShieldCheck aria-hidden="true" size={20} /><p><strong>历史回测不含 Agent overlay</strong><span>LLM 不在逐 bar 循环内；Agent 只能做前向影子比较。</span></p></div>
            <div><LockKeyhole aria-hidden="true" size={20} /><p><strong>实盘执行关闭</strong><span>回测或模拟盘表现不会自动开启实盘。</span></p></div>
          </section>

          {latest ? (
            <section className="strategy-results-ready" aria-labelledby="backtest-results-title">
              <header>
                <div><p className="eyebrow">BACKTEST EVIDENCE</p><h2 id="backtest-results-title">最新回测证据</h2></div>
                <div className="strategy-policy-card__badges">
                  <span className="mode-badge">{dataModeLabel(latest.data_mode)}</span>
                  <span className="mode-badge">{latest.benchmark_comparison_complete ? "基准比较完整" : "基准比较不完整"}</span>
                </div>
              </header>
              {containsFixture(latest.data_sources) ? <p className="inline-notice inline-notice--error" role="status">含 fixture 证据：仅用于验证流程，不代表真实市场数据表现。</p> : null}
              <dl className="strategy-result-ledger numeric-data">
                <div><dt>区间</dt><dd>{latest.start_session} → {latest.end_session}</dd></div>
                <div><dt>样本外</dt><dd>{latest.oos_window_count} 个样本外窗口</dd></div>
                <div><dt>结果哈希</dt><dd title={latest.result_hash}>{shortHash(latest.result_hash)}</dd></div>
                <div><dt>数据哈希</dt><dd title={latest.data_hash}>{shortHash(latest.data_hash)}</dd></div>
                <div><dt>代码哈希</dt><dd title={latest.code_hash}>{shortHash(latest.code_hash)}</dd></div>
                <div><dt>数据来源</dt><dd>{latest.data_sources.join(" · ")}</dd></div>
                <div><dt>完成时间</dt><dd>{formatUtc(latest.completed_at)}</dd></div>
              </dl>
              <div className="strategy-comparison-keys" aria-label="已完成的比较基准">
                {latest.comparison_keys.map((key) => <span className="state-chip" key={key}>{comparisonLabel(key)}</span>)}
              </div>
            </section>
          ) : (
            <section className="strategy-results-empty" aria-labelledby="backtest-results-title">
              <div><p className="eyebrow">BACKTEST EVIDENCE</p><h2 id="backtest-results-title">暂无已持久化的回测结果</h2></div>
              <p>完成冻结数据集运行后，此处将以同一起止日期和成本口径比较 SPY、等权白名单与当前冠军；不使用演示数字。</p>
            </section>
          )}
        </>
      )}
    </section>
  );
}

function shortHash(value: string): string { return `${value.slice(0, 10)}…${value.slice(-8)}`; }
function formatUtc(value: string): string { return value.replace("T", " ").replace("Z", " UTC"); }
function comparisonLabel(key: "spy" | "equal_weight" | "champion"): string {
  if (key === "spy") return "SPY 买入持有";
  if (key === "equal_weight") return "等权白名单";
  return "当前冠军";
}
function dataModeLabel(mode: "fixture" | "delayed" | "real_time" | "mixed"): string {
  if (mode === "fixture") return "FIXTURE / 合成数据";
  if (mode === "delayed") return "DELAYED / 延迟数据";
  if (mode === "real_time") return "REAL-TIME / 实时数据";
  return "MIXED / 混合来源";
}
function containsFixture(sources: string[]): boolean {
  return sources.some((source) => source.split(":").includes("fixture"));
}
