import dns from 'node:dns/promises';
import ipaddr from 'ipaddr.js';
export type AddressResolver = (hostname: string) => Promise<{ address: string; family: number }[]>;
export class UnsafeWebhookError extends Error {}
export async function resolveWebhook(value: string, resolver: AddressResolver = host => dns.lookup(host, { all: true }), allowLoopback = false) {
  const url = new URL(value);
  const host = url.hostname.replace(/^\[|\]$/g, '');
  const testReceiver = allowLoopback && url.protocol === 'http:' && host === '127.0.0.1' && url.pathname.startsWith('/test-viewer/webhooks/');
  if ((!testReceiver && url.protocol !== 'https:') || url.username || url.password) throw new UnsafeWebhookError('Webhook must use HTTPS without URL credentials');
  const addresses = await resolver(host);
  if (!addresses.length || !addresses.every(({ address }) =>
    testReceiver ? address === '127.0.0.1' : ipaddr.process(address).range() === 'unicast')) {
    throw new UnsafeWebhookError('Webhook resolves to a prohibited network');
  }
  return { url, address: addresses[0] };
}
export async function isWebhookSafe(value: string): Promise<boolean> {
  try { await resolveWebhook(value); return true; }
  catch (error) { if (error instanceof UnsafeWebhookError) return false; throw error; }
}
