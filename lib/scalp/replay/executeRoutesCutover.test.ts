import assert from 'node:assert/strict';
import test from 'node:test';

import { runExecuteDeploymentsPg } from '../executeDeploymentsPg';
import executeDeploymentsHandler from '../../../pages/api/scalp/cron/execute-deployments';

type MockReq = {
    method: string;
    url: string;
    headers: Record<string, string>;
    query: Record<string, string | string[] | undefined>;
};

type MockRes = {
    statusCode: number;
    headers: Record<string, string>;
    body: unknown;
    setHeader: (name: string, value: string) => void;
    status: (code: number) => MockRes;
    json: (payload: unknown) => MockRes;
};

function createReq(pathname: string, query: Record<string, string>): MockReq {
    const search = new URLSearchParams(query).toString();
    return {
        method: 'GET',
        url: `${pathname}?${search}`,
        headers: {},
        query,
    };
}

function createRes(): MockRes {
    return {
        statusCode: 200,
        headers: {},
        body: null,
        setHeader(name: string, value: string) {
            this.headers[String(name || '').toLowerCase()] = String(value || '');
        },
        status(code: number) {
            this.statusCode = Math.floor(Number(code) || 200);
            return this;
        },
        json(payload: unknown) {
            this.body = payload;
            return this;
        },
    };
}

function asRecord(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

test('execute deployment routes enforce strict PG requirement in cutover mode', async () => {
    const originalBackend = process.env.SCALP_BACKEND;
    const pgEnvKeys = [
        'SCALP_PG_CONNECTION_STRING',
        'DATABASE_URL',
        'POSTGRES_PRISMA_URL',
        'POSTGRES_URL',
        'PRISMA_CONNECTION_STRING',
        'PRISMA_PG_POSTGRES_URL',
    ] as const;
    const originalPgEnv: Partial<Record<(typeof pgEnvKeys)[number], string>> = {};
    for (const key of pgEnvKeys) {
        const value = process.env[key];
        if (value !== undefined) {
            originalPgEnv[key] = value;
        }
    }
    try {
        process.env.SCALP_BACKEND = 'pg';
        for (const key of pgEnvKeys) {
            delete process.env[key];
        }

        const baseQuery = {
            all: 'true',
            dryRun: 'true',
            requirePromotionEligible: 'true',
            nowMs: '1773179160000',
        };

        const strictReq = createReq('/api/scalp/cron/execute-deployments', baseQuery);
        const strictRes = createRes();
        await runExecuteDeploymentsPg(strictReq as any, strictRes as any, {
            strictPgRequired: true,
        });
        const strictBody = asRecord(strictRes.body);
        assert.equal(strictRes.statusCode, 503);
        assert.equal(strictBody.error, 'execute_deployments_pg_not_configured');
        assert.equal(strictBody.backend, 'pg');
        assert.equal(strictBody.strictPgRequired, true);

        const nonStrictReq = createReq('/api/scalp/cron/execute-deployments', baseQuery);
        const nonStrictRes = createRes();
        await runExecuteDeploymentsPg(nonStrictReq as any, nonStrictRes as any, {
            strictPgRequired: false,
        });
        const nonStrictBody = asRecord(nonStrictRes.body);
        assert.equal(nonStrictRes.statusCode, 200);
        assert.equal(nonStrictBody.ok, true);
        assert.equal(nonStrictBody.backend, 'pg');
        assert.equal(nonStrictBody.strictPgRequired, false);
        assert.equal(nonStrictBody.skipped, true);
        assert.equal(nonStrictBody.reason, 'pg_not_configured');

        const cutoverReq = createReq('/api/scalp/cron/execute-deployments', baseQuery);
        const cutoverRes = createRes();
        await executeDeploymentsHandler(cutoverReq as any, cutoverRes as any);
        const cutoverBody = asRecord(cutoverRes.body);
        assert.equal(cutoverRes.statusCode, 503);
        assert.equal(cutoverBody.error, 'execute_deployments_pg_not_configured');
        assert.equal(cutoverBody.backend, 'pg');
        assert.equal(cutoverBody.strictPgRequired, true);
    } finally {
        if (originalBackend === undefined) delete process.env.SCALP_BACKEND;
        else process.env.SCALP_BACKEND = originalBackend;
        for (const key of pgEnvKeys) {
            const value = originalPgEnv[key];
            if (value === undefined) delete process.env[key];
            else process.env[key] = value;
        }
    }
});
