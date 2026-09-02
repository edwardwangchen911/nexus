import { fireEvent, render, screen } from "@testing-library/react";
import { createRef } from "react";
import { describe, expect, it, vi } from "vitest";

import { Badge } from "../../../app/components/ui/badge";
import { Button } from "../../../app/components/ui/button";

describe("source-adapted UI primitives", () => {
  it("defaults to a non-submitting native button and forwards focus refs", () => {
    const ref = createRef<HTMLButtonElement>();
    const onClick = vi.fn();
    render(<Button ref={ref} variant="outline" size="sm" onClick={onClick}>核验来源</Button>);
    const button = screen.getByRole("button", { name: "核验来源" });
    expect(button).toHaveAttribute("type", "button");
    expect(button).toHaveClass("nexus-button--outline", "nexus-button--sm");
    fireEvent.click(button);
    expect(onClick).toHaveBeenCalledOnce();
    ref.current?.focus();
    expect(button).toHaveFocus();
  });

  it("keeps busy buttons disabled without swallowing native or aria props", () => {
    const onClick = vi.fn();
    render(<Button disabled aria-busy="true" aria-describedby="explanation" onClick={onClick}>正在登记研究</Button>);
    const button = screen.getByRole("button", { name: "正在登记研究" });
    fireEvent.click(button);
    expect(button).toBeDisabled();
    expect(button).toHaveAttribute("aria-busy", "true");
    expect(button).toHaveAttribute("aria-describedby", "explanation");
    expect(onClick).not.toHaveBeenCalled();
  });

  it("preserves explicit submit semantics where requested", () => {
    render(<Button type="submit">提交</Button>);
    expect(screen.getByRole("button")).toHaveAttribute("type", "submit");
  });

  it("does not infer a decision, truncate a label or make a badge interactive", () => {
    render(<Badge data-state="partial" variant="secondary">证据不完整 · 不代表可以决策</Badge>);
    const badge = screen.getByText("证据不完整 · 不代表可以决策");
    expect(badge).toHaveAttribute("data-state", "partial");
    expect(badge).not.toHaveAttribute("tabindex");
    expect(badge).not.toHaveAttribute("role");
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
  });
});
