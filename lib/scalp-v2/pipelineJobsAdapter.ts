import { runLoadCandlesPipelineJob } from "../scalp/pipelineJobs";

// Compatibility adapter kept only for Sunday candle-maintenance route.
export async function runScalpV2LoadCandlesPipelineJob(
  params: Parameters<typeof runLoadCandlesPipelineJob>[0],
) {
  return runLoadCandlesPipelineJob({
    ...(params || {}),
    allowNonBitgetSymbols: true,
  });
}
