import { runScalpExecuteCycle } from "../scalp/engine";

type RunScalpExecuteCycleParams = Parameters<typeof runScalpExecuteCycle>[0];

export async function runScalpV2ExecuteCycle(
  params: RunScalpExecuteCycleParams,
) {
  return runScalpExecuteCycle(params);
}
