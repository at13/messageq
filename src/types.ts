export type DeliveryMode = 'at_most_once' | 'at_least_once' | 'exactly_once';
export type EventInput = { type: string; source: string; payload: Record<string, unknown> };
export type FilterRules = { type?: string; source?: string; payload?: Record<string, unknown> };
