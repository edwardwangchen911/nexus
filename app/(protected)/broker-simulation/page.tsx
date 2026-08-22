import { LockKeyhole } from "lucide-react";

export default function BrokerSimulationPage(): React.ReactElement {
  return (
    <section className="locked-page" aria-labelledby="broker-simulation-title">
      <LockKeyhole aria-hidden="true" size={28} />
      <p className="eyebrow">FOSUN SIMULATION</p>
      <h1 id="broker-simulation-title">复星模拟盘验证</h1>
      <p className="state-chip">阶段 3 前锁定</p>
      <p>当前只提供内部美股模拟盘。复星模拟账户、批次确认和订单接口尚未编入本阶段能力。</p>
      <a href="/integrations">返回集成状态</a>
    </section>
  );
}
