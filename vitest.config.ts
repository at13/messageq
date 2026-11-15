import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Database suites share a test database; concurrency is exercised within each suite.
    fileParallelism: false,
  },
});
