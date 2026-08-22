import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import "../../../app/styles/tokens.css";
import { AppShell } from "../../../app/components/app-shell";

const route = vi.hoisted(() => ({ pathname: "/overview" }));

vi.mock("next/navigation", () => ({
  usePathname: () => route.pathname,
}));

const primaryHrefs = [
  "/overview",
  "/portfolio",
  "/optimize",
  "/market",
  "/paper",
  "/risk-audit",
];
const systemHrefs = ["/strategies", "/integrations"];

describe("AppShell", () => {
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
    expect([...desktopPrimary.querySelectorAll("a")].map((link) => link.getAttribute("href"))).toEqual(primaryHrefs);
    expect([...mobilePrimary.querySelectorAll("a")].map((link) => link.getAttribute("href"))).toEqual(primaryHrefs);
    expect([...desktopSystem.querySelectorAll("a")].map((link) => link.getAttribute("href"))).toEqual(systemHrefs);
    expect([...mobileSystem.querySelectorAll("a")].map((link) => link.getAttribute("href"))).toEqual(systemHrefs);
    expect(screen.getByRole("main")).toHaveAttribute("id", "main-content");
    expect(desktopPrimary.parentElement).toHaveClass("app-shell__navigation-groups");
    expect(desktopPrimary.querySelector("a")).toHaveClass("app-shell__navigation-control");
    expect(container.querySelectorAll(".app-shell__navigation-control")).toHaveLength(17);
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
});
