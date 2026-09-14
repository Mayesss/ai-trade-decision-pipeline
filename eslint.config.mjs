import path from 'node:path';

import { includeIgnoreFile } from '@eslint/compat';
import nextVitals from 'eslint-config-next/core-web-vitals';
import nextTs from 'eslint-config-next/typescript';
import { defineConfig } from 'eslint/config';
import tseslint from 'typescript-eslint';

const gitignorePath = path.resolve('.', '.gitignore');

export default defineConfig([
  // Ignore everything .gitignore ignores (.next, node_modules, coverage, ...),
  // the test suite, and .study — dev-only code whose harness/fixture/research
  // style clashes with the strict rules. .study is also excluded in
  // tsconfig.json: `next build` type-checks the project, so a throwaway
  // research script must never be able to fail a production deploy.
  includeIgnoreFile(gitignorePath),
  { ignores: ['test/**', '.study/**'] },
  // Next.js recommended + Core Web Vitals
  ...nextVitals,
  // Next.js TypeScript setup (typescript-eslint recommended)
  ...nextTs,
  // Strict typescript-eslint on top
  ...tseslint.configs.strict,
]);
