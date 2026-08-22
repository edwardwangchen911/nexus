import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { DataState, type DataStateKind } from "../../../app/components/data-state";

const cases: Array<[DataStateKind, string]> = [
  ["loading", "正在加载"],
  ["empty", "暂无数据"],
  ["ready", "数据已就绪"],
  ["partial", "数据不完整"],
  ["stale", "数据已过期"],
  ["error", "加载失败"],
  ["forbidden", "无权访问"],
];

describe("DataState", () => {
  it.each(cases)("renders a visible %s status label", (state, label) => {
    render(<DataState state={state} title="组合快照" detail="说明" />);

    expect(screen.getByRole("status", { name: label })).toBeVisible();
    expect(screen.getByText("组合快照")).toBeVisible();
    expect(screen.getByText("说明")).toBeVisible();
  });
});
