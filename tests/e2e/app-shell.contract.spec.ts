import { expect, test } from "@playwright/test";

test("mobile drawer keeps both navigation groups nested and enforces the hit-area contract", async ({ page }) => {
  await page.goto("/overview");

  const drawer = page.locator(".app-shell__drawer");
  const navigationGroups = drawer.locator(":scope > .app-shell__navigation-groups");
  const primaryNavigation = drawer.locator('nav[aria-label="移动主导航"]');
  const systemNavigation = drawer.locator('nav[aria-label="系统（移动）"]');

  await expect(drawer).toBeVisible();
  await expect(navigationGroups).toHaveCount(1);
  await expect(primaryNavigation).toHaveCount(1);
  await expect(systemNavigation).toHaveCount(1);
  await expect(primaryNavigation.locator("..")).toHaveClass("app-shell__navigation-groups");
  await expect(systemNavigation.locator("..")).toHaveClass("app-shell__navigation-groups");

  await drawer.locator("summary").click();
  await expect(primaryNavigation).toBeVisible();
  await expect(systemNavigation).toBeVisible();

  const geometry = await navigationGroups.evaluate((group) => ({
    groupGap: getComputedStyle(group).gap,
    controls: [...group.querySelectorAll<HTMLElement>(".app-shell__navigation-control")].map((control) => {
      const style = getComputedStyle(control);
      const rectangle = control.getBoundingClientRect();
      return {
        minBlockSize: style.minBlockSize,
        minInlineSize: style.minInlineSize,
        height: rectangle.height,
        width: rectangle.width,
      };
    }),
  }));

  expect(geometry.groupGap).toBe("8px");
  expect(geometry.controls).toHaveLength(8);
  for (const control of geometry.controls) {
    expect(control.minBlockSize).toBe("44px");
    expect(control.minInlineSize).toBe("44px");
    expect(control.height).toBeGreaterThanOrEqual(44);
    expect(control.width).toBeGreaterThanOrEqual(44);
  }
});
