import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import "../../../app/styles/tokens.css";
import { AppShell } from "../../../app/components/app-shell";

const route = vi.hoisted(() => ({ pathname: "/overview" }));
const navigation = vi.hoisted(() => ({ push: vi.fn() }));

vi.mock("next/navigation", () => ({
  usePathname: () => route.pathname,
  useRouter: () => navigation,
}));

const primaryNavigation = [
  ["交易工作台", "/workspace"],
  ["决策记录", "/decisions"],
  ["账户与交易", "/portfolio"],
  ["复盘", "/paper#paper-comparison-title"],
];
const systemNavigation = [["设置与运行", "/integrations"]];

describe("AppShell", () => {
  beforeEach(() => {
    route.pathname = "/overview";
    navigation.push.mockClear();
  });

  it("provides ordered primary and secondary navigation in desktop and mobile views", () => {
    const { container } = render(
      <AppShell>
        <p>内容</p>
      </AppShell>,
    );

    const skipLink = screen.getByText("跳到主要内容");
    const desktopPrimary = screen.getByRole("navigation", { name: "主导航" });
    const desktopSystem = screen.getByRole("navigation", { name: "系统" });
    const mobilePrimary = screen.getByRole("navigation", { name: "移动主导航" });
    const mobileSystem = screen.getByRole("navigation", { name: "系统（移动）" });

    expect(container.querySelector(".app-shell")?.firstElementChild).toBe(skipLink);
    expect(skipLink).toHaveAttribute("href", "#main-content");
    expect([...desktopPrimary.querySelectorAll("a")].map((link) => [link.textContent, link.getAttribute("href")])).toEqual(primaryNavigation);
    expect([...mobilePrimary.querySelectorAll("a")].map((link) => [link.textContent, link.getAttribute("href")])).toEqual(primaryNavigation);
    expect([...desktopSystem.querySelectorAll("a")].map((link) => [link.textContent, link.getAttribute("href")])).toEqual(systemNavigation);
    expect([...mobileSystem.querySelectorAll("a")].map((link) => [link.textContent, link.getAttribute("href")])).toEqual(systemNavigation);
    expect(screen.getByRole("main")).toHaveAttribute("id", "main-content");
    expect(desktopPrimary.parentElement).toHaveClass("app-shell__navigation-groups");
    expect(desktopPrimary.querySelector("a")).toHaveClass("app-shell__navigation-control");
    expect(container.querySelectorAll(".app-shell__navigation-control")).toHaveLength(11);
  });

  it("marks the current protected route in both navigation surfaces", () => {
    route.pathname = "/portfolio/positions";
    const { rerender } = render(<AppShell><p>内容</p></AppShell>);

    const currentLinks = screen.getAllByRole("link", { name: "账户与交易" });
    expect(currentLinks).toHaveLength(2);
    expect(currentLinks.every((link) => link.getAttribute("aria-current") === "page")).toBe(true);
    expect(screen.getAllByRole("link", { name: "交易工作台" }).every((link) => !link.hasAttribute("aria-current"))).toBe(true);

    route.pathname = "/decisions/cycle-1";
    rerender(<AppShell><p>内容</p></AppShell>);
    expect(screen.getAllByRole("link", { name: "决策记录" }).every((link) => link.getAttribute("aria-current") === "page")).toBe(true);

    route.pathname = "/paper";
    rerender(<AppShell><p>内容</p></AppShell>);
    expect(screen.getAllByRole("link", { name: "复盘" }).every((link) => link.getAttribute("aria-current") === "page")).toBe(true);
  });

  it("moves focus to main after a client-side protected-route change without stealing initial focus", () => {
    const { rerender } = render(
      <AppShell>
        <p>内容</p>
      </AppShell>,
    );

    const main = screen.getByRole("main");
    expect(document.activeElement).not.toBe(main);

    route.pathname = "/portfolio";
    rerender(
      <AppShell>
        <p>内容</p>
      </AppShell>,
    );

    expect(document.activeElement).toBe(main);
  });

  it("keeps unmodified navigation inside the client router", () => {
    render(<AppShell><p>内容</p></AppShell>);

    fireEvent.click(screen.getByRole("navigation", { name: "主导航" }).querySelector('a[href="/workspace"]')!);

    expect(navigation.push).toHaveBeenCalledWith("/workspace");
  });

  it("closes the mobile drawer after an unmodified client navigation", () => {
    const { container } = render(<AppShell><p>内容</p></AppShell>);
    const drawer = container.querySelector<HTMLDetailsElement>(".app-shell__drawer")!;
    drawer.open = true;

    fireEvent.click(screen.getByRole("navigation", { name: "移动主导航" }).querySelector('a[href="/decisions"]')!);

    expect(navigation.push).toHaveBeenCalledWith("/decisions");
    expect(drawer.open).toBe(false);
  });
});
