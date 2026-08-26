"use client";

import { useEffect, useRef, type MouseEvent } from "react";
import { usePathname, useRouter } from "next/navigation";
import { SkipLink } from "./skip-link";

const primaryNavigation = [
  ["概览", "/overview"],
  ["组合", "/portfolio"],
  ["优化", "/optimize"],
  ["市场", "/market"],
  ["模拟盘", "/paper"],
  ["风险与审计", "/risk-audit"],
] as const;

const systemNavigation = [
  ["策略", "/strategies"],
  ["集成", "/integrations"],
] as const;

function PrimaryNavigation({ mobile = false }: { mobile?: boolean }) {
  return (
    <nav aria-label={mobile ? "移动主导航" : "主导航"}>
      <ul className="app-shell__nav-list">
        {primaryNavigation.map(([label, href]) => (
          <li key={href}>
            <NavigationLink href={href}>{label}</NavigationLink>
          </li>
        ))}
      </ul>
    </nav>
  );
}

function NavigationGroups({ mobile = false }: { mobile?: boolean }) {
  return (
    <div className="app-shell__navigation-groups">
      <PrimaryNavigation mobile={mobile} />
      <nav aria-label={mobile ? "系统（移动）" : "系统"}>
        <ul className="app-shell__system-list">
          {systemNavigation.map(([label, href]) => (
            <li key={href}>
              <NavigationLink href={href}>{label}</NavigationLink>
            </li>
          ))}
        </ul>
      </nav>
    </div>
  );
}

function NavigationLink({ href, children }: { href: string; children: React.ReactNode }): React.ReactElement {
  const router = useRouter();
  function navigate(event: MouseEvent<HTMLAnchorElement>): void {
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    router.push(href);
  }
  return <a className="app-shell__navigation-control" href={href} onClick={navigate}>{children}</a>;
}

export function AppShell({ children }: { children: React.ReactNode }): React.ReactElement {
  const pathname = usePathname();
  const previousPathname = useRef(pathname);

  useEffect(() => {
    if (previousPathname.current !== pathname) {
      document.getElementById("main-content")?.focus();
    }
    previousPathname.current = pathname;
  }, [pathname]);

  return (
    <div className="app-shell">
      <SkipLink />
      <aside className="app-shell__sidebar">
        <p className="app-shell__brand">NEXUS</p>
        <NavigationGroups />
      </aside>
      <details className="app-shell__drawer">
        <summary className="app-shell__navigation-control">导航</summary>
        <NavigationGroups mobile />
      </details>
      <main id="main-content" tabIndex={-1}>{children}</main>
    </div>
  );
}
