import { z } from 'zod';
import type { JSONValue } from './types.js';

export const jsonValue: z.ZodType<JSONValue> = z.lazy(() => z.union([
  z.null(), z.string(), z.number(), z.boolean(), z.array(jsonValue), z.record(z.string(), jsonValue),
]));
export const eventSchema = z.object({
  type: z.string().min(1).max(256),
  source: z.string().min(1).max(256),
  payload: jsonValue,
}).refine(value => Object.prototype.hasOwnProperty.call(value, 'payload'), 'payload is required');
export const rulesSchema = z.object({
  type: z.string().min(1).max(256).optional(),
  source: z.string().min(1).max(256).optional(),
  payload: z.record(z.string().min(1).max(256), jsonValue).optional(),
}).strict().refine(value => Object.keys(value.payload ?? {}).every(path =>
  path.split('.').every(part => part && !['__proto__', 'prototype', 'constructor'].includes(part))), 'Invalid payload path');
export const subscriptionSchema = z.object({
  webhookUrl: z.string().url(),
  secret: z.string().min(16).max(256),
  deliveryMode: z.enum(['at_most_once', 'at_least_once', 'exactly_once']).default('at_least_once'),
  receiverSupportsIdempotency: z.boolean().default(false),
  rules: rulesSchema.default({}),
}).refine(value => value.deliveryMode !== 'exactly_once' || value.receiverSupportsIdempotency,
  'Exactly-once processing requires a receiver implementing transactional deduplication');
