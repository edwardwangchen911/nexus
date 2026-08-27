import AxeBuilder from "@axe-core/playwright";
import { expect, type Page } from "@playwright/test";

export const releaseViewports = [
  { name: "mobile-375", width: 375, height: 812 },
  { name: "mobile-390", width: 390, height: 844 },
  { name: "tablet-768", width: 768, height: 1024 },
  { name: "desktop-1024", width: 1024, height: 768 },
  { name: "desktop-1440", width: 1440, height: 1000 },
  { name: "mobile-landscape", width: 844, height: 390 },
] as const;

export async function assertReleaseAccessibility(page: Page): Promise<void> {
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= document.documentElement.clientWidth)).toBe(true);

  const targetSizes = await page.locator([
    ".button:visible",
    "button:visible",
    "select:visible",
    "input:not([type='checkbox']):not([type='radio']):visible",
    "summary:visible",
    ".app-shell__navigation-control:visible",
  ].join(", ")).evaluateAll((items) => items.map((item) => {
    const rect = item.getBoundingClientRect();
    return { label: item.getAttribute("aria-label") ?? item.textContent?.trim().slice(0, 40) ?? item.tagName, width: rect.width, height: rect.height };
  }));
  for (const target of targetSizes) {
    expect(target.width, `${target.label} width`).toBeGreaterThanOrEqual(44);
    expect(target.height, `${target.label} height`).toBeGreaterThanOrEqual(44);
  }

  const spacingFailures = await page.locator([
    ".page-actions",
    ".overview-links",
    ".market-evidence-actions",
    ".price-chart__controls",
    ".confirmation-dialog__actions",
    ".evolution-actions",
    ".app-shell__nav-list",
    ".app-shell__system-list",
  ].join(", ")).evaluateAll((groups) => groups.flatMap((group) => {
    const controls = [...group.querySelectorAll<HTMLElement>("a, button, input, select, summary")]
      .filter((control) => {
        const style = getComputedStyle(control);
        const rect = control.getBoundingClientRect();
        return style.visibility !== "hidden" && style.display !== "none" && rect.width > 0 && rect.height > 0;
      });
    const failures: string[] = [];
    for (let leftIndex = 0; leftIndex < controls.length; leftIndex += 1) {
      for (let rightIndex = leftIndex + 1; rightIndex < controls.length; rightIndex += 1) {
        const left = controls[leftIndex].getBoundingClientRect();
        const right = controls[rightIndex].getBoundingClientRect();
        const xGap = Math.max(0, left.left - right.right, right.left - left.right);
        const yGap = Math.max(0, left.top - right.bottom, right.top - left.bottom);
        if (xGap < 7.5 && yGap < 7.5) {
          failures.push(`${controls[leftIndex].textContent?.trim()} <> ${controls[rightIndex].textContent?.trim()}`);
        }
      }
    }
    return failures;
  }));
  expect(spacingFailures).toEqual([]);

  const motionFailures = await page.locator("body *").evaluateAll((items) => {
    const durationMs = (value: string): number => {
      const trimmed = value.trim();
      if (trimmed.endsWith("ms")) return Number.parseFloat(trimmed);
      if (trimmed.endsWith("s")) return Number.parseFloat(trimmed) * 1000;
      return 0;
    };
    return items.flatMap((item) => {
      const style = getComputedStyle(item);
      const maximum = Math.max(...[style.animationDuration, style.transitionDuration].flatMap((value) => value.split(",").map(durationMs)));
      return maximum > 0.02 ? [`${item.tagName}.${item.className}: ${maximum}ms`] : [];
    });
  });
  expect(motionFailures).toEqual([]);

  const serious = (await new AxeBuilder({ page }).analyze()).violations.filter(
    (violation) => violation.impact === "serious" || violation.impact === "critical",
  );
  expect(serious).toEqual([]);
}
