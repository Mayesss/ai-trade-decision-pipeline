import path from 'node:path';

import { includeIgnoreFile } from '@eslint/compat';
import nextVitals from 'eslint-config-next/core-web-vitals';
import nextTs from 'eslint-config-next/typescript';
import { defineConfig } from 'eslint/config';
import tseslint from 'typescript-eslint';

const gitignorePath = path.resolve('.', '.gitignore');

export default defineConfig([
  // Ignore everything .gitignore ignores (.next, node_modules, coverage, ...)
  // and the test suite — dev-only code whose harness/fixture style clashes
  // with the strict rules.
  includeIgnoreFile(gitignorePath),
  { ignores: ['test/**'] },
  // Next.js recommended + Core Web Vitals
  ...nextVitals,
  // Next.js TypeScript setup (typescript-eslint recommended)
  ...nextTs,
  // Strict typescript-eslint on top
  ...tseslint.configs.strict,
]);
