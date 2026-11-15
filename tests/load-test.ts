import { randomUUID } from 'node:crypto';
import { createHistogram, performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { writeFile } from 'node:fs/promises';

export type LoadOptions = {
  url: string;
  duration: number;
  concurrency: number;
  rate: number;
  timeout: number;
  maxRequests: number;
  payloadBytes: number;
  maxErrorRate: number;
  maxP99: number;
  eventType?: string;
  output?: string;
};

export function parseOptions(args: string[]): LoadOptions {
  const { values } = parseArgs({
    args,
    options: Object.fromEntries([
      'url', 'duration', 'concurrency', 'rate', 'timeout', 'max-requests',
      'payload-bytes', 'max-error-rate', 'max-p99', 'event-type', 'output',
    ].map(name => [name, { type: 'string' as const }])),
  });
  const url = new URL(values.url ?? 'http://127.0.0.1:3000');
  if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) {
    throw new Error('URL must be HTTP(S) without embedded credentials');
  }
  if (process.env.API_TOKEN && url.protocol !== 'https:' && !['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)) {
    throw new Error('Bearer-authenticated remote load tests require HTTPS');
  }
  function number(name: string, fallback: number, min: number, max: number, integer = false) {
    const value = Number(values[name] ?? fallback);
    if (!Number.isFinite(value) || value < min || value > max || (integer && !Number.isInteger(value))) {
      throw new Error(`--${name} must be ${integer ? 'an integer' : 'a number'} between ${min} and ${max}`);
    }
    return value;
  }
  return {
    url: url.origin,
    duration: number('duration', 10, 0.01, 3600),
    concurrency: number('concurrency', 4, 1, 256, true),
    rate: number('rate', 5, 0.1, 10000),
    timeout: number('timeout', 5000, 1, 60000, true),
    maxRequests: number('max-requests', 100000, 1, 100000, true),
    payloadBytes: number('payload-bytes', 0, 0, 512000, true),
    maxErrorRate: number('max-error-rate', 0.01, 0, 1),
    maxP99: number('max-p99', 1000, 1, 60000),
    ...(values['event-type'] ? { eventType: values['event-type'] } : {}),
    ...(values.output ? { output: values.output } : {}),
  };
}

export async function runLoad(options: LoadOptions, signal?: AbortSignal) {
  const runId = randomUUID();
  const start = performance.now();
  const deadline = start + options.duration * 1000;
  let nextSlot = start;
  let reservedRequests = 0;
  let requests = 0;
  let accepted = 0;
  let networkErrors = 0;
  let protocolErrors = 0;
  let httpErrors = 0;
  const statuses: Record<string, number> = {};
  const errorSamples: string[] = [];
  const eventIds = new Set<string>();
  const sampleEventIds: string[] = [];
  const histogram = createHistogram();
  const padding = 'x'.repeat(options.payloadBytes);
  const sampleError = (message: string) => {
    if (errorSamples.length < 5) errorSamples.push(message.slice(0, 300));
  };

  async function worker() {
    while (!signal?.aborted && reservedRequests < options.maxRequests) {
      const slot = Math.max(nextSlot, performance.now());
      if (slot >= deadline) return;
      nextSlot = slot + 1000 / options.rate;
      reservedRequests++;
      if (slot > performance.now()) {
        try {
          await sleep(slot - performance.now(), undefined, { signal });
        } catch (error) {
          if (signal?.aborted && error instanceof Error && error.name === 'AbortError') return;
          throw error;
        }
      }
      if (signal?.aborted || performance.now() >= deadline || requests >= options.maxRequests) return;
      const sequence = ++requests;
      const sentAt = performance.now();
      try {
        const response = await fetch(new URL('/events', options.url), {
          method: 'POST',
          redirect: 'error',
          headers: {
            'content-type': 'application/json',
            ...(process.env.API_TOKEN ? { authorization: `Bearer ${process.env.API_TOKEN}` } : {}),
            'idempotency-key': `${runId}:${sequence}`,
            'x-producer-id': `load-test:${runId}`,
          },
          body: JSON.stringify({
            type: options.eventType ?? `load.test.${runId}`,
            source: 'load-test',
            payload: { runId, sequence, padding },
          }),
          signal: AbortSignal.timeout(options.timeout),
        });
        statuses[response.status] = (statuses[response.status] ?? 0) + 1;
        const text = await response.text();
        if (response.status !== 202) {
          httpErrors++;
          sampleError(`Request ${sequence}: HTTP ${response.status}`);
        } else {
          let data: unknown;
          try { data = JSON.parse(text); } catch { data = undefined; }
          const id = data && typeof data === 'object' && 'id' in data ? data.id : undefined;
          if (typeof id !== 'string' || !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(id) || eventIds.has(id)) {
            protocolErrors++;
            sampleError(`Request ${sequence}: invalid or duplicate event ID in 202 response`);
          } else {
            accepted++;
            eventIds.add(id);
            if (sampleEventIds.length < 10) sampleEventIds.push(id);
          }
        }
      } catch (error) {
        networkErrors++;
        sampleError(`Request ${sequence}: ${error instanceof Error ? error.message : String(error)}`);
      } finally {
        histogram.record(Math.max(1, Math.round((performance.now() - sentAt) * 1e6)));
      }
    }
  }

  await Promise.all(Array.from({ length: options.concurrency }, worker));
  const elapsedSeconds = (performance.now() - start) / 1000;
  const errors = networkErrors + protocolErrors + httpErrors;
  const errorRate = requests ? errors / requests : 1;
  const latency = histogram.count ? {
    min: histogram.min / 1e6,
    mean: histogram.mean / 1e6,
    p50: histogram.percentile(50) / 1e6,
    p95: histogram.percentile(95) / 1e6,
    p99: histogram.percentile(99) / 1e6,
    max: histogram.max / 1e6,
  } : null;
  const thresholds = {
    errorRate: errorRate <= options.maxErrorRate,
    p99Latency: latency !== null && latency.p99 <= options.maxP99,
    acceptedRequests: accepted > 0,
    notInterrupted: !signal?.aborted,
  };
  return {
    schemaVersion: 1,
    runId,
    startedAt: new Date(Date.now() - elapsedSeconds * 1000).toISOString(),
    options,
    scope: 'HTTP ingestion only; acceptance does not establish fanout completion or webhook processing',
    elapsedSeconds,
    requests,
    accepted,
    errors,
    errorRate,
    httpErrors,
    networkErrors,
    protocolErrors,
    statuses,
    requestsPerSecond: requests / elapsedSeconds,
    acceptedPerSecond: accepted / elapsedSeconds,
    latencyMs: latency,
    sampleEventIds,
    errorSamples,
    thresholds,
    passed: Object.values(thresholds).every(Boolean),
  };
}

async function main() {
  if (process.argv.includes('--help')) {
    console.log(`Usage: npm run load:test -- [options]
  --url URL              Target origin (default http://127.0.0.1:3000)
  --duration SECONDS     Dispatch duration (default 10)
  --concurrency NUMBER   Maximum in-flight requests (default 4)
  --rate RPS             Aggregate maximum dispatch rate (default 5)
  --timeout MS           Request timeout (default 5000)
  --max-requests NUMBER  Hard request cap, at most 100000
  --payload-bytes NUMBER Padding bytes, not total encoded event size
  --max-error-rate RATIO Failure threshold (default 0.01)
  --max-p99 MS           p99 latency threshold (default 1000)
  --event-type TYPE      Optional type matching existing subscriptions
  --output FILE         Save JSON report (stdout always contains JSON)`);
    return;
  }
  const options = parseOptions(process.argv.slice(2));
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
  try {
    console.error(`Load test: ${options.url}, up to ${options.rate} RPS / ${options.concurrency} concurrent requests`);
    const report = await runLoad(options, controller.signal);
    const json = `${JSON.stringify(report, null, 2)}\n`;
    if (options.output) await writeFile(options.output, json, { flag: 'wx' });
    process.stdout.write(json);
    if (!report.passed) process.exitCode = 1;
  } finally {
    process.off('SIGINT', stop);
    process.off('SIGTERM', stop);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    console.error('Load test failed:', error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
