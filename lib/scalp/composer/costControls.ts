import {
  clampScalpV1HardCap,
  resolveScalpV1ResearchHardCaps,
} from "../v1CostBrake";

export function clampScalpComposerHardCap(value: number, hardCap: number): number {
  return clampScalpV1HardCap(value, hardCap);
}

export function resolveScalpComposerResearchHardCaps() {
  return resolveScalpV1ResearchHardCaps();
}
