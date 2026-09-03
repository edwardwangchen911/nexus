import { formatDisplayDecimal } from "./display-decimal";

type DisplayValue = string | number | null | undefined;

export function snapshotTimeLabel(basis: string | null | undefined): string {
  if (basis === "gateway_observed") return "本机读取时间";
  if (basis === "provider_event") return "数据源时间";
  return "快照记录时间（含义未确认）";
}

// Presentation only. Keep API values, trading dates and editable inputs unchanged.
export function formatDisplayTime(value: string | null | undefined): string {
  if (!value) return "未知";
  const match = /^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.\d+)?(Z|[+-]\d{2}:\d{2})?$/.exec(value);
  if (!match) return "无效时间";
  if (!match[3]) return "时区未提供";
  const calendar = new Date(`${match[1]}T${match[2]}Z`);
  const date = new Date(value);
  if (!Number.isFinite(date.valueOf()) || !Number.isFinite(calendar.valueOf())
    || calendar.toISOString().slice(0, 19) !== `${match[1]}T${match[2]}`) return "无效时间";
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23",
  }).formatToParts(date);
  const part = (type: Intl.DateTimeFormatPartTypes) => parts.find((item) => item.type === type)!.value;
  return `${part("year")}-${part("month")}-${part("day")} ${part("hour")}:${part("minute")}:${part("second")} 北京时间`;
}

/** Decimal point shifting avoids binary rounding and also handles integer ratios. */
function scaled(value: DisplayValue, places: number): string {
  if (value === null || value === undefined) return "未知";
  if (typeof value === "number") return formatDisplayDecimal(value * 10 ** places);
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(value);
  if (!match) return "无效数值";
  const fraction = match[3] ?? "";
  let digits = match[2] + fraction;
  const scale = fraction.length - places;
  if (scale <= 0) digits += "0".repeat(-scale);
  else {
    digits = digits.padStart(scale + 1, "0");
    digits = `${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
  }
  return formatDisplayDecimal(`${match[1]}${digits}`);
}

function withUnit(value: string, unit: string): string {
  return value === "未知" || value === "无效数值" ? value : `${value}${unit}`;
}

export function formatPercent(value: DisplayValue, difference = false): string {
  return withUnit(scaled(value, 2), difference ? " 个百分点" : "%");
}

export function formatMetric(value: DisplayValue, unit: string, metric = "", difference = false): string {
  const code = metric.split(":")[0].toLowerCase();
  const coefficient = ["hhi", "concentration_hhi", "correlation", "correlation_126", "sharpe", "beta", "beta_126"].includes(code);
  if (unit === "ratio" || unit === "annualized_ratio") return coefficient ? formatDisplayDecimal(value) : formatPercent(value, difference);
  if (unit === "basis_points") return coefficient ? scaled(value, -4) : withUnit(scaled(value, -2), difference ? " 个百分点" : "%");
  if (unit === "parts_per_million") return scaled(value, -6);
  const units: Record<string, string> = {
    count: " 项", trading_days: " 个交易日", days: " 天", factor: " 倍",
    bps: " 基点", percent: "%", percentage_points: " 个百分点", seconds: " 秒",
    boolean: "", state: "", coefficient: "", index_points: " 指数点",
  };
  return withUnit(formatDisplayDecimal(value), units[unit] ?? ` ${unit}`);
}
