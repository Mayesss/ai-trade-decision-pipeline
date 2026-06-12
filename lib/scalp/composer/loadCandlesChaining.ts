export function shouldContinueScalpComposerLoadCandles(params: {
  busy: boolean;
  autoContinue: boolean;
  pendingAfter: number;
  selfHop: number;
  selfMaxHops: number;
}): boolean {
  return (
    !params.busy &&
    params.autoContinue &&
    params.pendingAfter > 0 &&
    params.selfHop < params.selfMaxHops
  );
}

export function shouldTriggerScalpComposerLoadCandlesSuccessor(params: {
  ok: boolean;
  busy: boolean;
  autoSuccessor: boolean;
  pendingAfter: number;
}): boolean {
  return (
    params.ok &&
    !params.busy &&
    params.autoSuccessor &&
    params.pendingAfter <= 0
  );
}
