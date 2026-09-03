import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DataState, type DataStateKind } from "../../../app/components/data-state";

const cases: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"],
  ["empty", "暂无数据"],
  ["ready", "数据已就绪"],
  ["partial", "数据不完整"],
  ["stale", "显示上次数据"],
  ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

describe("DataState", () => {
  it.each(cases)("renders a visible %s status label", (state, label) => {
    render(<DataState state={state} title="组合快照" detail="说明" />);

    if (state === "error") expect(screen.getByRole("alert")).toHaveTextContent(label);
    else expect(screen.getByRole("status", { name: label })).toBeVisible();
    expect(screen.getByText("组合快照")).toBeVisible();
    expect(screen.getByText("说明")).toBeVisible();
  });

  it("gives multiple instances distinct heading IDs and relationships", () => {
    const { container } = render(
      <>
        <DataState state="empty" title="组合快照" />
        <DataState state="error" title="市场快照" />
      </>,
    );

    const sections = [...container.querySelectorAll(".data-state")];
    const headings = screen.getAllByRole("heading");

    expect(headings.map((heading) => heading.id)).toEqual(expect.not.arrayContaining([""]));
    expect(new Set(headings.map((heading) => heading.id)).size).toBe(2);
    expect(sections.map((section) => section.getAttribute("aria-labelledby"))).toEqual(headings.map((heading) => heading.id));
  });

  it("moves focus to a newly rendered error summary", () => {
    render(<DataState state="error" title="市场快照" detail="读取失败" />);

    const summary = screen.getByRole("alert");
    expect(summary).toHaveFocus();
    expect(summary).toHaveAttribute("tabindex", "-1");
  });
});
