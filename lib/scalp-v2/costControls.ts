import {
  clampScalpV1HardCap,
  resolveScalpV1ResearchHardCaps,
} from "../scalp/v1CostBrake";

export function clampScalpV2HardCap(value: number, hardCap: number): number {
  return clampScalpV1HardCap(value, hardCap);
}

export function resolveScalpV2ResearchHardCaps() {
  return resolveScalpV1ResearchHardCaps();
}
