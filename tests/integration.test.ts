import { describe, expect, it } from 'vitest';
import { buildApp } from '../src/app.js';
describe('HTTP validation integration', () => {
  it('requires an idempotency key and validates event shape', async () => {
    const app = buildApp();
    const response = await app.inject({ method: 'POST', url: '/events', payload: { type: 'x', source: 's', payload: {} } });
    expect(response.statusCode).toBe(400);
    await app.close();
  });
});
