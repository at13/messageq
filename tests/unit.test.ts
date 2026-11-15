import { describe, expect, it } from 'vitest';
import { matches, retryDelay, shouldRetry, parseRetryAfter } from '../src/rules.js';
import { getEventQueueCapacity } from '../src/db.js';
import { eventSchema, rulesSchema } from '../src/validation.js';
describe('JSON events and filter validation', () => {
  it('accepts all JSON payload shapes and rejects an absent payload', () => {
    for (const payload of [null, false, 0, 'hello', [1, { nested: true }], { a: null }]) {
      expect(eventSchema.safeParse({ type: 'test', source: 'unit', payload }).success).toBe(true);
    }
    expect(eventSchema.safeParse({ type: 'test', source: 'unit' }).success).toBe(false);
  });
  it('matches structural JSON values, null and array paths but not inherited properties', () => {
    expect(matches({ type: 't', source: 's', payload: { a: [1, 2], b: null } }, { payload: { a: [1, 2], b: null } })).toBe(true);
    expect(matches({ type: 't', source: 's', payload: [1, 2] }, { payload: { '1': 2 } })).toBe(true);
    expect(matches({ type: 't', source: 's', payload: {} }, { payload: { toString: '' } })).toBe(false);
    expect(rulesSchema.safeParse({ payload: { '__proto__.polluted': true } }).success).toBe(false);
  });
  it('parses and bounds Retry-After without accepting invalid values', () => {
    expect(parseRetryAfter('2')).toBe(2000);
    expect(parseRetryAfter('999')).toBe(60000);
    expect(parseRetryAfter('bad')).toBeUndefined();
    expect(parseRetryAfter('Sun, 15 Nov 2026 06:00:02 GMT', Date.parse('2026-11-15T06:00:00Z'))).toBe(2000);
  });
});
describe('event admission configuration', () => {
  it('rejects invalid capacity rather than silently disabling admission limits', () => {
    for (const value of [0, -1, 1.5, 'NaN', '', 1_000_001]) expect(() => getEventQueueCapacity(value)).toThrow();
    expect(getEventQueueCapacity('20')).toBe(20);
  });
});
describe('filter rules', () => {
  it('matches type, source, and nested payload values', () => expect(matches({ type: 'order.created', source: 'checkout', payload: { customer: { id: '1' } } }, { type: 'order.created', source: 'checkout', payload: { 'customer.id': '1' } })).toBe(true));
  it('rejects mismatches and missing paths', () => expect(matches({ type: 'x', source: 's', payload: {} }, { payload: { missing: 1 } })).toBe(false));
});
describe('retry policy', () => {
  it('retries transient outcomes only', () => { expect(shouldRetry(503)).toBe(true); expect(shouldRetry(429)).toBe(true); expect(shouldRetry(400)).toBe(false); expect(shouldRetry(undefined, new Error('timeout'))).toBe(true); });
  it('uses capped exponential backoff with bounded jitter', () => {
    expect(retryDelay(1, 100, 60000, () => 1)).toBe(100);
    expect(retryDelay(3, 100, 60000, () => 0)).toBe(200);
    expect(retryDelay(20, 100, 60000, () => 1)).toBe(60000);
  });
});
