import { LockKeyhole } from "lucide-react";

export default function LiveApprovalsPage(): React.ReactElement {
  return (
    <section className="locked-page" aria-labelledby="live-approvals-title">
      <LockKeyhole aria-hidden="true" size={28} />
      <p className="eyebrow">LIVE APPROVALS</p>
      <h1 id="live-approvals-title">实盘逐笔批准</h1>
      <p className="state-chip">阶段 4 前锁定</p>
      <p>实盘写能力未启用。未来也只能逐笔人工批准，并需另行完成安全设计、恢复演练与小额限额验证。</p>
      <a href="/integrations">返回集成状态</a>
    </section>
  );
}
