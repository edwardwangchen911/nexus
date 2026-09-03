import { LockKeyhole } from "lucide-react";

export default function LiveApprovalsPage(): React.ReactElement {
  return (
    <section className="locked-page" aria-labelledby="live-approvals-title">
      <LockKeyhole aria-hidden="true" size={28} />
      <p className="eyebrow">实盘能力 · 逐笔批准</p>
      <h1 id="live-approvals-title">实盘逐笔批准</h1>
      <p className="state-chip">实盘交易未启用</p>
      <p>实盘写能力未启用。未来也只能逐笔人工批准，并需另行完成安全设计、恢复演练与小额限额验证。</p>
      <a className="button button--quiet" href="/integrations">返回集成状态</a>
    </section>
  );
}
