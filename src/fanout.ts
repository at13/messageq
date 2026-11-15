import { fanoutOnce } from './db.js';

export interface FanoutDispatcher {
  readonly backend: string;
  // Zero means idle; duplicate notifications still count as handled work.
  dispatchOnce(): Promise<number>;
}

export function createFanoutDispatcher(backend = process.env.FANOUT_DISPATCHER ?? 'postgresql'): FanoutDispatcher {
  if (backend !== 'postgresql') {
    throw new Error('Unsupported FANOUT_DISPATCHER. Supported: postgresql. Redis Streams and Kafka are not implemented.');
  }
  return { backend, dispatchOnce: fanoutOnce };
}
