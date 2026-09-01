"use client";

import { useEffect, useRef, type ComponentType, type MouseEvent } from "react";
import { usePathname, useRouter } from "next/navigation";
import {
  ChartCandlestick,
  FlaskConical,
  LayoutDashboard,
  Menu,
  PlugZap,
  ScrollText,
  ShieldCheck,
  SlidersHorizontal,
  WalletCards,
} from "lucide-react";
import { SkipLink } from "./skip-link";

type NavigationItem = {
  label: string;
  href: string;
  icon: ComponentType<{ "aria-hidden": true; size: number; strokeWidth: number }>;
};

const primaryNavigation = [
  { label: "概览", href: "/overview", icon: LayoutDashboard },
  { label: "组合", href: "/portfolio", icon: WalletCards },
  { label: "优化", href: "/optimize", icon: SlidersHorizontal },
  { label: "市场", href: "/market", icon: ChartCandlestick },
  { label: "模拟盘", href: "/paper", icon: FlaskConical },
  { label: "风险与审计", href: "/risk-audit", icon: ShieldCheck },
] satisfies NavigationItem[];

const systemNavigation = [
  { label: "策略", href: "/strategies", icon: ScrollText },
  { label: "集成", href: "/integrations", icon: PlugZap },
] satisfies NavigationItem[];

function PrimaryNavigation({ mobile = false }: { mobile?: boolean }) {
  return (
    <nav aria-label={mobile ? "移动主导航" : "主导航"}>
      <p className="app-shell__nav-heading">日常决策</p>
      <ul className="app-shell__nav-list">
        {primaryNavigation.map(({ label, href, icon }) => (
          <li key={href}>
            <NavigationLink href={href} icon={icon}>{label}</NavigationLink>
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
        <p className="app-shell__nav-heading">系统管理</p>
        <ul className="app-shell__system-list">
          {systemNavigation.map(({ label, href, icon }) => (
            <li key={href}>
              <NavigationLink href={href} icon={icon}>{label}</NavigationLink>
            </li>
          ))}
        </ul>
      </nav>
    </div>
  );
}

function NavigationLink({ href, icon: Icon, children }: {
  href: string;
  icon: NavigationItem["icon"];
  children: React.ReactNode;
}): React.ReactElement {
  const router = useRouter();
  const pathname = usePathname();
  const active = pathname === href || pathname.startsWith(`${href}/`);
  function navigate(event: MouseEvent<HTMLAnchorElement>): void {
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    router.push(href);
  }
  return (
    <a
      className="app-shell__navigation-control"
      href={href}
      onClick={navigate}
      aria-current={active ? "page" : undefined}
    >
      <Icon aria-hidden={true} size={18} strokeWidth={1.8} />
      <span>{children}</span>
    </a>
  );
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
        <div className="app-shell__brand">
          <span>NEXUS</span>
          <small>私人投资工作台</small>
        </div>
        <NavigationGroups />
      </aside>
      <details className="app-shell__drawer">
        <summary className="app-shell__navigation-control">
          <Menu aria-hidden="true" size={18} strokeWidth={1.8} />
          导航
        </summary>
        <NavigationGroups mobile />
      </details>
      <main id="main-content" tabIndex={-1}>{children}</main>
    </div>
  );
}
