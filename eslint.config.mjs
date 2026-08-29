import path from 'node:path';

import { includeIgnoreFile } from '@eslint/compat';
import vitest from '@vitest/eslint-plugin';
import nextVitals from 'eslint-config-next/core-web-vitals';
import nextTs from 'eslint-config-next/typescript';
import { defineConfig } from 'eslint/config';
import tseslint from 'typescript-eslint';

const gitignorePath = path.resolve('.', '.gitignore');

export default defineConfig([
  // Ignore everything .gitignore ignores (.next, node_modules, coverage, ...)
  includeIgnoreFile(gitignorePath),
  // Vendored stub package (npm override target), not project code
  { ignores: ['stubs/**'] },
  // Next.js recommended + Core Web Vitals
  ...nextVitals,
  // Next.js TypeScript setup (typescript-eslint recommended)
  ...nextTs,
  // Strict typescript-eslint on top
  ...tseslint.configs.strict,
  {
    name: 'project/overrides',
    rules: {
      // Kept at warn for the four canonical fetch-effects (dashboard load,
      // symbol details, chart fetch, live-tick merge): a data fetch that
      // flips its own loading state has no rule-clean formulation that isn't
      // worse code. Everything else was refactored clean — don't add more.
      'react-hooks/set-state-in-effect': 'warn',
      '@typescript-eslint/no-unused-vars': ['error', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_',
        caughtErrorsIgnorePattern: '^_',
        ignoreRestSiblings: true,
      }],
    },
  },
  // Tests: vitest rules; snapshots/fixtures justify looser typing
  {
    name: 'project/tests',
    files: ['test/**/*.ts', 'vitest.config.ts'],
    plugins: { vitest },
    rules: {
      ...vitest.configs.recommended.rules,
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      // Harness scrubs env vars by computed key
      '@typescript-eslint/no-dynamic-delete': 'off',
    },
  },
]);
