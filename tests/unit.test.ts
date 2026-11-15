import { describe, expect, it } from 'vitest';
import { matches, retryDelay, shouldRetry } from '../src/rules.js';
import { getEventQueueCapacity } from '../src/db.js';
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
  it('uses capped exponential backoff', () => { expect(retryDelay(1, 100)).toBe(100); expect(retryDelay(3, 100)).toBe(400); expect(retryDelay(20, 100)).toBe(60000); });
});
