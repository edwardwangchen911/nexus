import { formatPercent } from "../../lib/display-format";
import type { components } from "../../lib/generated/schema";
type Detail = components["schemas"]["AgentPaperManagementCycleResponse"];
export function instrumentLabel(detail: Detail, id: string): string {
  return detail.cycle.candidates.find((item) => item.instrument_id === id)?.symbol ?? `标的 …${id.slice(-4)}`;
}
export function TargetComparison({ detail }: { detail: Detail }): React.ReactElement {
  const { active_target: active, proposal } = detail;
  const ids = [...new Set([...(active?.targets ?? []).map((row) => row.instrument_id), ...(proposal?.targets ?? []).map((row) => row.instrument_id)])];
  return <section className="pm-panel" aria-labelledby="target-comparison-title">
    <header className="pm-panel__heading"><h2 id="target-comparison-title">当前目标与提案对照</h2><p>以下权重来自确定性配置，不是 Agent 输出。</p></header>
    <table className="pm-table" aria-label="确定性目标对照"><thead><tr><th scope="col">标的</th><th scope="col">当前生效目标{active ? ` V${active.version_seq}` : ""}</th><th scope="col">本轮提案</th><th scope="col">配置依据</th></tr></thead><tbody>
      {ids.map((id) => {
        const current = active?.targets.find((row) => row.instrument_id === id);
        const next = proposal?.targets.find((row) => row.instrument_id === id);
        const inherited = next?.reason_code === "INHERITED_TARGET";
        return <tr key={id}><th scope="row" className="numeric-data" title={id}>{instrumentLabel(detail, id)}</th><td data-label={active ? `目标 V${active.version_seq} · 生效` : "尚未建立目标"} className="numeric-data">{current ? formatPercent(current.target_weight) : "未列入目标"}</td><td data-label="本轮提案 · 确定性配置" className="numeric-data">{next ? formatPercent(next.target_weight) : "未提供"}</td><td data-label="配置依据">{inherited ? "继承现有目标；本轮未研究，不更新研究有效期" : next?.exit ? "明确提出退出；不代表已退出" : next ? `确定性配置 · ${next.reason_code}` : "保留原目标事实"}</td></tr>;
      })}
      <tr><th scope="row">现金</th><td data-label="生效目标现金" className="numeric-data">{active ? formatPercent(active.cash_weight) : "尚未建立目标"}</td><td data-label="提案现金" className="numeric-data">{proposal ? formatPercent(proposal.cash_weight) : "暂不可用"}</td><td data-label="配置依据">目标现金是合法配置；实际现金独立核对</td></tr>
    </tbody></table>
    <p className="pm-footnote">权重分母：对应目标的完整配置。实际权重及金额未提供，暂不可用。</p>
  </section>;
}
