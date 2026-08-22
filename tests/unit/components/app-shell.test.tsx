import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { AppShell } from "../../../app/components/app-shell";

describe("AppShell", () => {
  it("provides the shared navigation hierarchy and a main landmark", () => {
    render(
      <AppShell>
        <p>内容</p>
      </AppShell>,
    );

    expect(screen.getByText("跳到主要内容")).toHaveAttribute("href", "#main-content");
    expect(screen.getByRole("navigation", { name: "主导航" }).querySelectorAll("a")).toHaveLength(6);
    expect(screen.getByRole("main")).toHaveAttribute("id", "main-content");
    expect(screen.getByRole("navigation", { name: "系统" })).toBeVisible();
  });
});
