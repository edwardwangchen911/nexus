/** Presentation only: never feed rounded strings back into calculations or requests. */
export function formatDisplayDecimal(
  value: string | number | null | undefined,
  options: { group?: boolean; fixedDigits?: number } = {},
): string {
  if (value === null || value === undefined) return "未知";
  const digits = Math.min(3, Math.max(0, options.fixedDigits ?? 3));
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return "无效数值";
    if (value !== 0 && Math.abs(value) < 0.5 * 10 ** -digits) {
      return belowDisplayPrecision(value < 0, digits);
    }
    return new Intl.NumberFormat("en-US", {
      useGrouping: options.group ?? false,
      minimumFractionDigits: options.fixedDigits === undefined ? 0 : digits,
      maximumFractionDigits: digits,
      signDisplay: "auto",
    }).format(value === 0 ? 0 : value);
  }
  const match = /^(-?)(\d+)(?:\.(\d+))?$/.exec(value);
  if (!match) return "无效数值";
  const fraction = match[3] ?? "";
  // Round decimal strings directly, including values beyond Number.MAX_SAFE_INTEGER.
  let units = BigInt(match[2] + fraction.slice(0, digits).padEnd(digits, "0"));
  if ((fraction[digits] ?? "0") >= "5") units += BigInt(1);
  if (units === BigInt(0) && /[1-9]/.test(`${match[2]}${fraction}`)) {
    return belowDisplayPrecision(match[1] === "-", digits);
  }
  const rounded = units.toString().padStart(digits + 1, "0");
  let whole = digits === 0 ? rounded : rounded.slice(0, -digits);
  let decimals = digits === 0 ? "" : rounded.slice(-digits);
  if (options.fixedDigits === undefined) decimals = decimals.replace(/0+$/, "");
  if (options.group) whole = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  return `${match[1] && units !== BigInt(0) ? "-" : ""}${whole}${decimals ? `.${decimals}` : ""}`;
}

function belowDisplayPrecision(negative: boolean, digits: number): string {
  const threshold = digits === 0 ? "1" : `0.${"0".repeat(digits - 1)}1`;
  return negative ? `(-${threshold}, 0)` : `<${threshold}`;
}
