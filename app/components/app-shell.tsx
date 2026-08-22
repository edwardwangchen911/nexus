"use client";

import { useEffect } from "react";
import { SkipLink } from "./skip-link";

const primaryNavigation = [
  ["概览", "/overview"],
  ["组合", "/portfolio"],
  ["优化", "/optimize"],
  ["市场", "/market"],
  ["模拟盘", "/paper"],
  ["风险与审计", "/risk-audit"],
] as const;

function PrimaryNavigation({ mobile = false }: { mobile?: boolean }) {
  return (
    <nav aria-label={mobile ? "移动主导航" : "主导航"}>
      <ul className="app-shell__nav-list">
        {primaryNavigation.map(([label, href]) => (
          <li key={href}>
            <a href={href}>{label}</a>
          </li>
        ))}
      </ul>
    </nav>
  );
}

export function AppShell({ children }: { children: React.ReactNode }): React.ReactElement {
  useEffect(() => {
    document.getElementById("main-content")?.focus();
  }, []);

  return (
    <div className="app-shell">
      <SkipLink />
      <aside className="app-shell__sidebar">
        <p className="app-shell__brand">NEXUS</p>
        <PrimaryNavigation />
        <nav aria-label="系统">
          <ul className="app-shell__system-list">
            <li><a href="/settings">系统设置</a></li>
          </ul>
        </nav>
      </aside>
      <details className="app-shell__drawer">
        <summary>导航</summary>
        <PrimaryNavigation mobile />
        <nav aria-label="移动系统">
          <ul className="app-shell__system-list">
            <li><a href="/settings">系统设置</a></li>
          </ul>
        </nav>
      </details>
      <main id="main-content" tabIndex={-1}>{children}</main>
    </div>
  );
}
