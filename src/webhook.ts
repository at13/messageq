import http from 'node:http';
import https from 'node:https';
import { createHmac } from 'node:crypto';
import { resolveWebhook } from './url.js';
import { parseRetryAfter } from './rules.js';
import type { JSONValue } from './types.js';

export type WebhookMessage = {
  url: string; secret: string; deliveryId: string; eventId: string;
  type: string; source: string; payload: JSONValue; allowLoopback?: boolean;
};
export async function sendWebhook(message: WebhookMessage, timeoutMs: number): Promise<{ status?: number; error?: string; retryAfterMs?: number }> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error('Webhook request timed out')), timeoutMs);
  const aborted = new Promise<never>((_, reject) => controller.signal.addEventListener('abort', () => reject(controller.signal.reason), { once: true }));
  try {
    const destination = await Promise.race([resolveWebhook(message.url, undefined, message.allowLoopback), aborted]);
    controller.signal.throwIfAborted();
    const { url, address } = destination;
    const body = JSON.stringify({
      deliveryId: message.deliveryId, eventId: message.eventId, type: message.type, source: message.source, payload: message.payload,
    });
    const timestamp = String(Math.floor(Date.now() / 1000));
    return await Promise.race([new Promise<{ status?: number; retryAfterMs?: number }>((resolve, reject) => {
      const options: https.RequestOptions & { autoSelectFamily: boolean } = {
        protocol: url.protocol,
        hostname: url.hostname.replace(/^\[|\]$/g, ''),
        port: url.port || (url.protocol === 'https:' ? '443' : '80'),
        path: `${url.pathname}${url.search}`,
        method: 'POST',
        servername: url.hostname.replace(/^\[|\]$/g, ''),
        autoSelectFamily: false,
        lookup: (_host, _options, callback) => callback(null, address.address, address.family),
        signal: controller.signal,
        headers: {
          host: url.host,
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(body),
          'x-messageq-delivery-id': message.deliveryId,
          'x-messageq-timestamp': timestamp,
          'x-messageq-signature': createHmac('sha256', message.secret).update(body).digest('hex'),
          'x-messageq-signature-v2': createHmac('sha256', message.secret).update(`${timestamp}.${body}`).digest('hex'),
        },
      };
      const onResponse = (response: http.IncomingMessage) => {
        const retryAfter = response.headers['retry-after'];
        resolve({ status: response.statusCode, retryAfterMs: parseRetryAfter(Array.isArray(retryAfter) ? retryAfter[0] : retryAfter) });
        // Response bodies are not part of the acknowledgement contract; do not buffer them.
        response.destroy();
      };
      const request = url.protocol === 'https:' ? https.request(options, onResponse) : http.request(options, onResponse);
      request.on('error', reject);
      request.end(body);
    }), aborted]);
  } catch (error) {
    return { error: error instanceof Error ? error.message : 'Webhook transport failed' };
  } finally {
    clearTimeout(timer);
  }
}
