import dns from 'node:dns/promises';
import ipaddr from 'ipaddr.js';
export async function isWebhookSafe(value: string): Promise<boolean> {
  const url = new URL(value);
  if (url.protocol !== 'https:') return false;
  const addresses = await dns.lookup(url.hostname, { all: true });
  return addresses.every(({ address }) => { const range = ipaddr.parse(address).range(); return range === 'unicast'; });
}
