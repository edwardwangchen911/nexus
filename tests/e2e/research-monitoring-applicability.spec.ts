import { readFileSync } from "node:fs";
import { expect, test } from "@playwright/test";
import { researchAdmissionData, researchAdmissionEnvelope, researchData, researchEnvelope } from "../unit/research/fixtures";

test("a frozen legacy research page explains both monitoring dimensions", async ({ page }) => {
  const path = process.env.NEXUS_TEST_FROZEN_RESEARCH_RESPONSE;
  const data = path ? JSON.parse(readFileSync(path, "utf8")) : {
    ...researchData(), monitoring: null, disclosure_monitoring: null,
    monitoring_applicability: { price: "not_applicable", disclosure: "not_applicable" },
  };
  await page.route(`**/api/v1/research-runs/${data.run_id}`, (route) => route.fulfill({ json: researchEnvelope(data) }));
  await page.route(`**/api/v1/instruments/${data.instrument_id}/research-admission?**`, (route) =>
    route.fulfill({ json: researchAdmissionEnvelope(researchAdmissionData({ instrument_id: data.instrument_id })) }));
  await page.goto(`/research/${data.instrument_id}?run_id=${data.run_id}`);
  await expect(page.getByText("价格监控 · 此研究版本未启用这项监控。")).toBeVisible();
  await expect(page.getByText("SEC 公告监控 · 此研究版本未启用这项监控。")).toBeVisible();
  if (path) {
    await expect(page.getByText(/AVGO · 已冻结研究资料/)).toBeVisible();
    await page.screenshot({ path: path + ".png", fullPage: true });
  }
});
