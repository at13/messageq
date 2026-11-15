import { describe, expect, it } from 'vitest';
import { resolveWebhook } from '../src/url.js';
import { sendWebhook } from '../src/webhook.js';
import { createServer, type IncomingHttpHeaders } from 'node:http';
import { createHmac, randomUUID } from 'node:crypto';

describe('webhook destination safety', () => {
  it('rejects internal, mixed and mapped-private DNS results', async () => {
    for (const addresses of [
      [{ address: '127.0.0.1', family: 4 }],
      [{ address: '10.1.1.1', family: 4 }],
      [{ address: '::ffff:127.0.0.1', family: 6 }],
      [{ address: '8.8.8.8', family: 4 }, { address: '169.254.169.254', family: 4 }],
      [],
    ]) await expect(resolveWebhook('https://example.com/hook', async () => addresses)).rejects.toThrow();
  });

  it('signs the actual JSON body, caps Retry-After, and never follows redirects or waits for response bodies', async () => {
    const requests: { path: string | undefined; headers: IncomingHttpHeaders; body: string }[] = [];
    const server = createServer((request, response) => {
      let body = '';
      request.setEncoding('utf8');
      request.on('data', chunk => { body += chunk; });
      request.on('end', () => {
        requests.push({ path: request.url, headers: request.headers, body });
        if (requests.length === 1) {
          response.writeHead(503, { 'Retry-After': '120' });
          response.end();
        } else {
          response.writeHead(302, { Location: '/must-not-be-requested' });
          response.write('Ignored response body');
        }
      });
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Missing test server address');
    const message = {
      url: `http://127.0.0.1:${address.port}/test-viewer/webhooks/test`,
      allowLoopback: true, secret: 'unit-test-signing-secret',
      deliveryId: randomUUID(), eventId: randomUUID(), type: 'test', source: 'unit', payload: null,
    };
    try {
      expect(await sendWebhook(message, 1000)).toMatchObject({ status: 503, retryAfterMs: 60000 });
      expect(await sendWebhook(message, 1000)).toMatchObject({ status: 302 });
      expect(requests).toHaveLength(2);
      expect(requests.every(request => request.path === '/test-viewer/webhooks/test')).toBe(true);
      const sent = requests[1];
      expect(JSON.parse(sent.body)).toEqual({ deliveryId: message.deliveryId, eventId: message.eventId, type: 'test', source: 'unit', payload: null });
      expect(sent.headers['x-messageq-signature']).toBe(createHmac('sha256', message.secret).update(sent.body).digest('hex'));
      expect(sent.headers['x-messageq-signature-v2']).toBe(createHmac('sha256', message.secret).update(`${sent.headers['x-messageq-timestamp']}.${sent.body}`).digest('hex'));
    } finally {
      server.closeAllConnections();
      await new Promise<void>(resolve => server.close(() => resolve()));
    }
  });
  it('returns a validated address to pin the HTTP connection and rejects URL credentials', async () => {
    const address = { address: '8.8.8.8', family: 4 };
    expect((await resolveWebhook('https://example.com/hook', async () => [address])).address).toEqual(address);
    await expect(resolveWebhook('https://user:password@example.com/hook')).rejects.toThrow();
    await expect(resolveWebhook('http://example.com/hook')).rejects.toThrow();
  });
  it('permits only explicitly registered loopback viewer receivers', async () => {
    const local = async () => [{ address: '127.0.0.1', family: 4 }];
    await expect(resolveWebhook('http://127.0.0.1/private', local, true)).rejects.toThrow();
    await expect(resolveWebhook('http://127.0.0.1/test-viewer/webhooks/test', local)).rejects.toThrow();
    expect((await resolveWebhook('http://127.0.0.1/test-viewer/webhooks/test', local, true)).address.address).toBe('127.0.0.1');
  });
});
