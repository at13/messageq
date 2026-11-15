export type DeliveryMode = 'at_most_once' | 'at_least_once' | 'exactly_once';
export type JSONValue = null | string | number | boolean | JSONValue[] | { [key: string]: JSONValue };
export type EventInput = { type: string; source: string; payload: JSONValue };
export type FilterRules = { type?: string; source?: string; payload?: Record<string, JSONValue> };
