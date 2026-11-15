import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    env: {
      API_TOKEN: 'test-only-api-token-00000000000000',
      TEST_VIEWER_ENABLED: 'true',
    },
    // Database suites share a test database; concurrency is exercised within each suite.
    fileParallelism: false,
  },
});
