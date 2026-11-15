import type { EventInput, FilterRules } from './types.js';
import { isDeepStrictEqual } from 'node:util';

export function matches(event: EventInput, rules: FilterRules): boolean {
  if (rules.type && rules.type !== event.type) return false;
  if (rules.source && rules.source !== event.source) return false;
  for (const [path, expected] of Object.entries(rules.payload ?? {})) {
    const actual = path.split('.').reduce<unknown>((value, key) =>
      value !== null && typeof value === 'object' && Object.prototype.hasOwnProperty.call(value, key) ? (value as Record<string, unknown>)[key] : undefined, event.payload);
    if (!isDeepStrictEqual(actual, expected)) return false;
  }
  return true;
}

export function shouldRetry(status: number | undefined, error?: unknown): boolean {
  if (error) return true;
  return status === undefined || status === 408 || status === 425 || status === 429 || status >= 500;
}

export function retryDelay(attempt: number, base = 500, max = 60_000, random = Math.random): number {
  const ceiling = Math.min(max, base * (2 ** Math.max(0, attempt - 1)));
  return Math.round(ceiling * (0.5 + random() * 0.5));
}

export function parseRetryAfter(value: string | undefined, now = Date.now()): number | undefined {
  if (!value) return undefined;
  const duration = /^\d+$/.test(value) ? Number(value) * 1000 : Date.parse(value) - now;
  return Number.isFinite(duration) && duration >= 0 ? Math.min(duration, 60_000) : undefined;
}
