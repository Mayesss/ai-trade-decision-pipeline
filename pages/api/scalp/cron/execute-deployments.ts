export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { runExecuteDeploymentsPg } from '../../../../lib/scalp/executeDeploymentsPg';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    return runExecuteDeploymentsPg(req, res, {
        strictPgRequired: true,
    });
}
