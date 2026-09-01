import { LockKeyhole, ShieldCheck } from "lucide-react";

import type { components } from "../../lib/generated/schema";
import { formatPaperTime } from "../order-timeline";

type Workbench = components["schemas"]["BrokerSimulationResponse"];

export function CapabilityBanner({ workbench }: { workbench: Workbench }): React.ReactElement {
  const { capability, account } = workbench;
  return (
    <section className="broker-capability" aria-labelledby="broker-capability-title">
      <div className="broker-capability__lock">
        <LockKeyhole aria-hidden="true" size={20} />
        <div>
          <p className="eyebrow">执行能力边界</p>
          <h2 id="broker-capability-title">实盘关闭</h2>
        </div>
      </div>
      <dl className="broker-capability__facts">
        <div><dt>运行能力</dt><dd>{capability.status}</dd></div>
        <div><dt>外部激活</dt><dd>{capability.external_activation}</dd></div>
        <div><dt>模拟写入</dt><dd>{capability.simulation_write_enabled ? "已激活" : "关闭"}</dd></div>
        <div><dt>账户边界</dt><dd>{account?.redacted_label ?? "未绑定外部模拟账户"}</dd></div>
      </dl>
      <p className="broker-capability__note">
        <ShieldCheck aria-hidden="true" size={17} />
        {account
          ? `能力到期 ${formatPaperTime(account.capability_expires_at)}；仅展示脱敏标识。`
          : "Phase 3A 仅验证离线权威链，未连接真实账户。"}
      </p>
    </section>
  );
}
