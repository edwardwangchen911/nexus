import { ArrowRight, WalletCards } from "lucide-react";

import { formatDisplayDecimal } from "../../lib/display-decimal";
import { formatDisplayTime } from "../../lib/display-format";
import type { components } from "../../lib/generated/schema";

type Account = components["schemas"]["WorkbenchAccountSummaryResponse"];

function money(value: string | null, currency: Account["currency"]): string {
  return value === null ? "未知" : `${currency} ${formatDisplayDecimal(value, { group: true })}`;
}

export function AccountSummary({ account }: { account: Account | null }): React.ReactElement {
  return (
    <section className="workbench-panel workbench-account" aria-label="账户概况（内部模拟）">
      <header className="workbench-panel__heading">
        <div><p className="eyebrow">INTERNAL PAPER ACCOUNT</p><h2>账户概况（内部模拟）</h2></div>
        <WalletCards aria-hidden="true" size={22} />
      </header>
      {account ? <>
        <dl>
          <div className="workbench-account__primary"><dt>净值</dt><dd className="numeric-data">{money(account.nav, account.currency)}</dd></div>
          <div><dt>可用现金</dt><dd className="numeric-data">{money(account.available_cash, account.currency)}</dd></div>
          <div><dt>持仓市值</dt><dd className="numeric-data">{money(account.market_value, account.currency)}</dd></div>
          <div><dt>已实现盈亏</dt><dd className="numeric-data">{money(account.realized_pnl, account.currency)}</dd></div>
          <div><dt>未实现盈亏</dt><dd className="numeric-data">{money(account.unrealized_pnl, account.currency)}</dd></div>
          <div><dt>数据时间</dt><dd className="numeric-data">{formatDisplayTime(account.as_of)}</dd></div>
        </dl>
        <a className="button button--quiet" href="/portfolio">查看账户与持仓<ArrowRight aria-hidden="true" size={17} /></a>
      </> : <p className="workbench-muted">尚无内部模拟账户汇总；未用其他账户事实补填。</p>}
    </section>
  );
}
