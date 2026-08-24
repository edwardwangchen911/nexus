import { validate as validateUuid } from "uuid";

export function sameUuid(left: unknown, right: unknown): boolean {
  return typeof left === "string"
    && typeof right === "string"
    && validateUuid(left)
    && validateUuid(right)
    && left.toLowerCase() === right.toLowerCase();
}
