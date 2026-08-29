import { qualityReasonLabel } from "../lib/market-evidence-labels";

export function MarketQualityReasons({ reasons }: { reasons: string[] }): React.ReactElement {
  return (
    <ul>
      {reasons.map((reason) => (
        <li key={reason}>
          <span>{qualityReasonLabel(reason)}</span>
          <code className="quality-reason__code">{reason}</code>
        </li>
      ))}
    </ul>
  );
}
