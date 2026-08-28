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
      // Ratchet targets: ~600 pre-existing findings across lib/. Warn until
      // the any/assertion cleanup happens; new code should not add more.
      '@typescript-eslint/no-explicit-any': 'warn',
      '@typescript-eslint/no-non-null-assertion': 'warn',
      // React 19 compiler-era rules: real findings in the dashboard UI, but
      // behavior-touching refactors with no UI test net yet.
      'react-hooks/set-state-in-effect': 'warn',
      'react-hooks/refs': 'warn',
      'react-hooks/purity': 'warn',
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
