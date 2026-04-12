import assert from 'node:assert/strict';
import test from 'node:test';

import type { NextApiRequest } from 'next';

import { buildCronAuthHeaders } from './cronChaining';

function withEnv(
    overrides: Record<string, string | undefined>,
    fn: () => void,
): void {
    const previous = new Map<string, string | undefined>();
    for (const [key, value] of Object.entries(overrides)) {
        previous.set(key, process.env[key]);
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
    }
    try {
        fn();
    } finally {
        for (const [key, value] of previous.entries()) {
            if (value === undefined) delete process.env[key];
            else process.env[key] = value;
        }
    }
}

test('buildCronAuthHeaders includes Vercel protection bypass secret for internal cron hops', { concurrency: false }, () => {
    withEnv(
        {
            ADMIN_ACCESS_SECRET: 'admin-secret',
            VERCEL_AUTOMATION_BYPASS_SECRET: 'bypass-secret',
        },
        () => {
            const headers = buildCronAuthHeaders();
            assert.equal(headers['x-admin-access-secret'], 'admin-secret');
            assert.equal(headers['x-vercel-protection-bypass'], 'bypass-secret');
        },
    );
});

test('buildCronAuthHeaders forwards request auth when env admin secret is absent', { concurrency: false }, () => {
    withEnv(
        {
            ADMIN_ACCESS_SECRET: undefined,
            VERCEL_AUTOMATION_BYPASS_SECRET: 'bypass-secret',
        },
        () => {
            const req = {
                headers: {
                    'x-admin-access-secret': 'forwarded-admin',
                    authorization: 'Bearer forwarded-token',
                },
            } as NextApiRequest;
            const headers = buildCronAuthHeaders(req);
            assert.equal(headers['x-admin-access-secret'], 'forwarded-admin');
            assert.equal(headers.authorization, undefined);
            assert.equal(headers['x-vercel-protection-bypass'], 'bypass-secret');
        },
    );
});

test('buildCronAuthHeaders forwards deployment protection context from request when bypass env is absent', { concurrency: false }, () => {
    withEnv(
        {
            ADMIN_ACCESS_SECRET: undefined,
            VERCEL_AUTOMATION_BYPASS_SECRET: undefined,
        },
        () => {
            const req = {
                headers: {
                    authorization: 'Bearer forwarded-token',
                    cookie: '__vercel_jwt=abc123; foo=bar',
                    'x-vercel-protection-bypass': 'forwarded-bypass',
                },
            } as NextApiRequest;
            const headers = buildCronAuthHeaders(req);
            assert.equal(headers.authorization, 'Bearer forwarded-token');
            assert.equal(headers.cookie, '__vercel_jwt=abc123; foo=bar');
            assert.equal(headers['x-vercel-protection-bypass'], 'forwarded-bypass');
        },
    );
});
