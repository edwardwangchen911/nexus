"use client";

import { useEffect, useRef, type ComponentType, type MouseEvent } from "react";
import { usePathname, useRouter } from "next/navigation";
import {
  Bot,
  FlaskConical,
  LayoutDashboard,
  Menu,
  LockKeyhole,
  PlugZap,
  ScrollText,
  WalletCards,
} from "lucide-react";
import { SkipLink } from "./skip-link";

type NavigationItem = {
  label: string;
  href: string;
  activePaths: string[];
  icon: ComponentType<{ "aria-hidden": true; size: number; strokeWidth: number }>;
};

const primaryNavigation = [
  { label: "交易工作台", href: "/workspace", activePaths: ["/workspace"], icon: LayoutDashboard },
  { label: "决策记录", href: "/decisions", activePaths: ["/decisions"], icon: ScrollText },
  { label: "账户与交易", href: "/portfolio", activePaths: ["/portfolio"], icon: WalletCards },
  { label: "复盘", href: "/paper#paper-comparison-title", activePaths: ["/paper"], icon: FlaskConical },
] satisfies NavigationItem[];

const systemNavigation = [
  { label: "设置与运行", href: "/integrations", activePaths: ["/integrations"], icon: PlugZap },
] satisfies NavigationItem[];

function PrimaryNavigation({ mobile = false, onNavigate }: { mobile?: boolean; onNavigate?: () => void }) {
  return (
    <nav aria-label={mobile ? "移动主导航" : "主导航"}>
      <p className="app-shell__nav-heading">日常决策</p>
      <ul className="app-shell__nav-list">
        {primaryNavigation.map(({ label, href, activePaths, icon }) => (
          <li key={href}>
            <NavigationLink href={href} activePaths={activePaths} icon={icon} onNavigate={onNavigate}>{label}</NavigationLink>
          </li>
        ))}
      </ul>
    </nav>
  );
}

function NavigationGroups({ mobile = false, onNavigate }: { mobile?: boolean; onNavigate?: () => void }) {
  return (
    <div className="app-shell__navigation-groups">
      <PrimaryNavigation mobile={mobile} onNavigate={onNavigate} />
      <nav aria-label={mobile ? "系统（移动）" : "系统"}>
        <p className="app-shell__nav-heading">系统管理</p>
        <ul className="app-shell__system-list">
          {systemNavigation.map(({ label, href, activePaths, icon }) => (
            <li key={href}>
              <NavigationLink href={href} activePaths={activePaths} icon={icon} onNavigate={onNavigate}>{label}</NavigationLink>
            </li>
          ))}
        </ul>
      </nav>
    </div>
  );
}

function NavigationLink({ href, activePaths, icon: Icon, onNavigate, children }: {
  href: string;
  activePaths: string[];
  icon: NavigationItem["icon"];
  onNavigate?: () => void;
  children: React.ReactNode;
}): React.ReactElement {
  const router = useRouter();
  const pathname = usePathname();
  const active = activePaths.some((path) => pathname === path || pathname.startsWith(`${path}/`));
  function navigate(event: MouseEvent<HTMLAnchorElement>): void {
    if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    router.push(href);
    onNavigate?.();
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
  const drawerRef = useRef<HTMLDetailsElement>(null);

  function onMobileNavigate(): void {
    if (drawerRef.current) drawerRef.current.open = false;
  }

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
          <span className="app-shell__brand-mark" aria-hidden="true">N</span>
          <span className="app-shell__brand-copy"><strong>NEXUS</strong><small>INVESTMENT INTELLIGENCE</small></span>
        </div>
        <NavigationGroups />
        <footer className="app-shell__boundary">
          <p><Bot aria-hidden="true" size={15} />AI 研究辅助</p>
          <p><LockKeyhole aria-hidden="true" size={15} />实盘执行已锁定</p>
        </footer>
      </aside>
      <details className="app-shell__drawer" ref={drawerRef} suppressHydrationWarning>
        <summary className="app-shell__navigation-control">
          <span className="app-shell__mobile-brand"><span aria-hidden="true">N</span><strong>NEXUS</strong></span>
          <span><Menu aria-hidden="true" size={18} strokeWidth={1.8} />导航</span>
        </summary>
        <NavigationGroups mobile onNavigate={onMobileNavigate} />
      </details>
      <main id="main-content" tabIndex={-1}>{children}</main>
    </div>
  );
}
