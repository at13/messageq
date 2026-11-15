import type { EventInput, FilterRules } from './types.js';

export function matches(event: EventInput, rules: FilterRules): boolean {
  if (rules.type && rules.type !== event.type) return false;
  if (rules.source && rules.source !== event.source) return false;
  for (const [path, expected] of Object.entries(rules.payload ?? {})) {
    const actual = path.split('.').reduce<unknown>((value, key) =>
      value && typeof value === 'object' ? (value as Record<string, unknown>)[key] : undefined, event.payload);
    if (actual !== expected) return false;
  }
  return true;
}

export function shouldRetry(status: number | undefined, error?: unknown): boolean {
  if (error) return true;
  return status === undefined || status === 408 || status === 425 || status === 429 || status >= 500;
}

export function retryDelay(attempt: number, base = 500, max = 60_000): number {
  return Math.min(max, base * (2 ** Math.max(0, attempt - 1)));
}
