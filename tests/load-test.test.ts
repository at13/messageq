import { createServer, type RequestListener } from 'node:http';
import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { parseOptions, runLoad } from './load-test.js';

async function withReceiver(handler: RequestListener, test: (url: string) => Promise<void>) {
  const server = createServer(handler);
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Missing receiver address');
  try { await test(`http://127.0.0.1:${address.port}`); }
  finally { await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve())); }
}

const config = (url: string, extra: string[] = []) => parseOptions([
  '--url', url, '--duration', '1', '--rate', '1000', '--concurrency', '2', '--max-requests', '4', ...extra,
]);

describe('load configuration', () => {
  it('rejects plaintext remote URLs before transmitting bearer credentials', () => {
    expect(() => parseOptions(['--url', 'http://example.com'])).toThrow('require HTTPS');
    expect(() => parseOptions(['--url', 'https://example.com'])).not.toThrow();
  });
  it('rejects unsafe or invalid configurations', () => {
    for (const args of [['--rate', '0'], ['--concurrency', '1.5'], ['--timeout', 'NaN'], ['--max-requests', '100001'], ['--url', 'file:///tmp/a']]) {
      expect(() => parseOptions(args)).toThrow();
    }
  });
});

describe('HTTP load runner', () => {
  it('bounds concurrency and generates unique idempotency keys and valid metrics', async () => {
    const keys: unknown[] = [];
    let active = 0, peak = 0;
    await withReceiver((request, response) => {
      keys.push(request.headers['idempotency-key']);
      peak = Math.max(peak, ++active);
      request.resume();
      setTimeout(() => { active--; response.writeHead(202).end(JSON.stringify({ id: randomUUID() })); }, 20);
    }, async url => {
      const result = await runLoad(config(url));
      expect(result.requests).toBe(4);
      expect(result.accepted).toBe(4);
      expect(result.errors).toBe(0);
      expect(result.passed).toBe(true);
      expect(peak).toBeLessThanOrEqual(2);
      expect(new Set(keys).size).toBe(4);
      expect(result.latencyMs!.p99).toBeGreaterThanOrEqual(result.latencyMs!.p50);
    });
  });

  it('counts HTTP errors, malformed acceptance and duplicate IDs rather than reporting success', async () => {
    let count = 0;
    const id = randomUUID();
    await withReceiver((request, response) => {
      request.resume();
      count++;
      response.writeHead(count === 1 ? 503 : 202).end(count === 2 ? 'invalid' : JSON.stringify({ id }));
    }, async url => {
      const result = await runLoad(config(url));
      expect(result.statuses).toEqual({ '503': 1, '202': 3 });
      expect(result.httpErrors).toBe(1);
      expect(result.protocolErrors).toBe(2);
      expect(result.accepted).toBe(1);
      expect(result.errorRate).toBe(0.75);
      expect(result.passed).toBe(false);
    });
  });

  it('counts timeouts as errors', async () => {
    await withReceiver((request, response) => {
      request.resume();
      setTimeout(() => response.writeHead(202).end(JSON.stringify({ id: randomUUID() })), 100);
    }, async url => {
      const result = await runLoad(config(url, ['--timeout', '10', '--max-requests', '1']));
      expect(result.networkErrors).toBe(1);
      expect(result.accepted).toBe(0);
      expect(result.passed).toBe(false);
    });
  });

  it('fails the latency threshold despite successful acceptance', async () => {
    await withReceiver((request, response) => {
      request.resume();
      setTimeout(() => response.writeHead(202).end(JSON.stringify({ id: randomUUID() })), 20);
    }, async url => {
      const result = await runLoad(config(url, ['--max-requests', '1', '--max-p99', '1']));
      expect(result.accepted).toBe(1);
      expect(result.thresholds.errorRate).toBe(true);
      expect(result.thresholds.p99Latency).toBe(false);
      expect(result.passed).toBe(false);
    });
  });

  it('honors the request cap without reserving unnecessary delayed requests', async () => {
    await withReceiver((request, response) => {
      request.resume();
      response.writeHead(202).end(JSON.stringify({ id: randomUUID() }));
    }, async url => {
      const result = await runLoad(config(url, ['--max-requests', '1', '--rate', '1', '--concurrency', '4']));
      expect(result.requests).toBe(1);
      expect(result.elapsedSeconds).toBeLessThan(0.9);
    });
  });

  it('paces aggregate dispatch and stops new requests after duration', async () => {
    await withReceiver((request, response) => {
      request.resume();
      response.writeHead(202).end(JSON.stringify({ id: randomUUID() }));
    }, async url => {
      const result = await runLoad(config(url, ['--rate', '5', '--duration', '0.3']));
      expect(result.requests).toBe(2);
      expect(result.elapsedSeconds).toBeGreaterThanOrEqual(0.19);
    });
  });

  it('fails an interrupted run even when no requests are sent', async () => {
    const stop = new AbortController();
    stop.abort();
    const result = await runLoad(config('http://127.0.0.1:1'), stop.signal);
    expect(result.requests).toBe(0);
    expect(result.latencyMs).toBeNull();
    expect(result.passed).toBe(false);
  });
});
