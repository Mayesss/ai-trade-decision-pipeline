import type { NextApiResponse } from 'next';

export const FOREX_MODE_DEPRECATION_MESSAGE =
  'Forex mode is deprecated. Use swing or scalp modes instead.';

export function respondForexModeDeprecated(res: NextApiResponse): void {
  res.status(410).json({
    error: 'forex_mode_deprecated',
    message: FOREX_MODE_DEPRECATION_MESSAGE,
  });
}
