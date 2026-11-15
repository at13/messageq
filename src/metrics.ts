import { Registry, Counter, Histogram, Gauge, collectDefaultMetrics } from '@prometheus-io/client';

export const registry = new Registry();
collectDefaultMetrics({ register: registry });
export const httpRequests = new Counter({ name: 'messageq_http_requests_total', help: 'HTTP requests', labelNames: ['method', 'route', 'status'], registers: [registry] });
export const httpDuration = new Histogram({ name: 'messageq_http_duration_seconds', help: 'HTTP request duration', labelNames: ['method', 'route'], registers: [registry] });
export const deliveryAttempts = new Counter({ name: 'messageq_delivery_attempts_total', help: 'Completed or recovered webhook attempts', labelNames: ['outcome', 'status'], registers: [registry] });
export const deliveryDuration = new Histogram({ name: 'messageq_delivery_duration_seconds', help: 'Webhook attempt duration', labelNames: ['outcome'], registers: [registry] });
export const backlog = new Gauge({ name: 'messageq_backlog', help: 'Durable unfinished work', labelNames: ['kind'], registers: [registry] });
export const oldestPending = new Gauge({ name: 'messageq_oldest_pending_seconds', help: 'Age of oldest pending delivery', registers: [registry] });
