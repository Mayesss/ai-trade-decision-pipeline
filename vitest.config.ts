import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        projects: [
            // Pure-kernel tests (the migrated node:test suite). They stub
            // fetch themselves where needed and must keep running with the
            // same env the old runner saw — scrubbed, no boundary world.
            {
                test: {
                    name: 'unit',
                    environment: 'node',
                    include: ['test/unit/**/*.test.ts'],
                    setupFiles: ['./test/harness/setup-env.ts'],
                },
            },
            // Boundary-contract tests: msw with error-on-unhandled, ordered
            // conversation snapshots, fake Postgres client, frozen clock.
            {
                test: {
                    name: 'contract',
                    environment: 'node',
                    include: ['test/contract/**/*.test.ts'],
                    setupFiles: ['./test/harness/setup-env.ts', './test/harness/setup-contract.ts'],
                },
            },
        ],
        // The first run after a fresh clone optimizes dependencies and takes
        // well over 10s — otherwise CI fails exactly once.
        hookTimeout: 60_000,
    },
});
