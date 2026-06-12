import { runScalpExecuteCycle } from "../engine";

type RunScalpExecuteCycleParams = Parameters<typeof runScalpExecuteCycle>[0];

export async function runScalpComposerExecuteCycle(
  params: RunScalpExecuteCycleParams,
) {
  return runScalpExecuteCycle(params);
}
