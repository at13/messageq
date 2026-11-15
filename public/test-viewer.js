'use strict';

const byId = id => document.getElementById(id);
const storageKey = 'messageq-test-viewer-session';
let session;
let refreshing = false;
const shownAttempts = new Map();
let apiToken = '';

function status(message, error = false) {
  byId('status').textContent = message;
  byId('status').dataset.error = String(error);
}

async function api(path, method = 'GET', body, extraHeaders = {}) {
  const response = await fetch(path, {
    method,
    headers: {
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      ...(session ? { 'x-test-viewer-session': session } : {}),
      authorization: `Bearer ${apiToken}`,
      ...extraHeaders,
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal: AbortSignal.timeout(10000),
  });
  const text = await response.text();
  let data;
  try { data = JSON.parse(text); }
  catch { throw new Error(`HTTP ${response.status}: expected JSON`); }
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${JSON.stringify(data.error ?? data)}`);
  return data;
}

function item(parent, title, data) {
  const container = document.createElement('div');
  container.className = 'item';
  const heading = document.createElement('strong');
  heading.textContent = title;
  container.append(heading);
  if (data !== undefined) {
    const pre = document.createElement('pre');
    pre.textContent = JSON.stringify(data, null, 2);
    container.append(pre);
  }
  parent.append(container);
  return container;
}

function button(parent, text, action) {
  const control = document.createElement('button');
  control.className = 'secondary';
  control.textContent = text;
  control.addEventListener('click', async () => {
    control.disabled = true;
    try { await action(); }
    catch (error) { status(error.message, true); }
    finally { control.disabled = false; }
  });
  parent.append(control);
}

async function refresh() {
  if (refreshing || !session || !apiToken) return;
  refreshing = true;
  try {
    const state = await api('/test-viewer/state');
    for (const name of ['subscriptions', 'deliveries', 'notifications']) {
      byId(name).replaceChildren();
      if (!state[name].length) byId(name).textContent = `No ${name} yet.`;
    }
    for (const sub of state.subscriptions) {
      const row = item(byId('subscriptions'), `${sub.active ? 'Active' : 'Disabled'} | ${sub.delivery_mode} | ${sub.id}`, {
        rules: sub.rules, failuresRemaining: sub.failures_remaining, receivedAttempts: sub.received_attempts,
      });
      if (sub.active) button(row, 'Disable subscription', async () => {
        await api(`/test-viewer/subscriptions/${sub.id}`, 'DELETE');
        status('Subscription disabled for future routing.');
        await refresh();
      });
    }
    for (const delivery of state.deliveries) {
      const row = item(byId('deliveries'), `${delivery.status} | ${delivery.attempt_count} attempts | ${delivery.id}`, delivery);
      if (shownAttempts.has(delivery.id)) item(row, 'Attempt history', shownAttempts.get(delivery.id));
      button(row, 'View attempts', async () => {
        const attempts = await api(`/test-viewer/deliveries/${delivery.id}/attempts`);
        shownAttempts.set(delivery.id, attempts);
        await refresh();
      });
    }
    for (const receipt of state.notifications) {
      item(byId('notifications'), `Received ${receipt.receive_count} time(s) | ${receipt.delivery_id}`, {
        firstReceivedAt: receipt.first_received_at, lastReceivedAt: receipt.last_received_at,
        subscriptionId: receipt.subscription_id, notification: receipt.notification,
      });
    }
  } catch (error) {
    status(`Refresh failed: ${error.message}. Use New test session if this session was removed.`, true);
  } finally {
    refreshing = false;
  }
}

async function newSession() {
  const result = await api('/test-viewer/sessions', 'POST');
  session = result.id;
  sessionStorage.setItem(storageKey, session);
  byId('session-id').textContent = session;
  byId('event-type').value = `viewer.demo.${session}`;
  byId('rules').value = JSON.stringify({ type: byId('event-type').value }, null, 2);
  byId('sent-events').replaceChildren();
  shownAttempts.clear();
  status('New session ready. Create a subscription, then send a matching event.');
  await refresh();
}

function bindForm(id, action) {
  byId(id).addEventListener('submit', async event => {
    event.preventDefault();
    const submit = event.target.querySelector('button[type=submit]');
    submit.disabled = true;
    try { await action(); await refresh(); }
    catch (error) { status(error.message, true); }
    finally { submit.disabled = false; }
  });
}

bindForm('subscribe-form', async () => {
  const result = await api('/test-viewer/subscriptions', 'POST', {
    deliveryMode: byId('mode').value,
    rules: JSON.parse(byId('rules').value),
    failFirst: Number(byId('fail-first').value),
  });
  status(`Subscription ${result.id} created. Send an event that matches its rules.`);
});

bindForm('event-form', async () => {
  const result = await api('/events', 'POST', {
    type: byId('event-type').value,
    source: byId('event-source').value,
    payload: JSON.parse(byId('event-payload').value),
  }, {
    'idempotency-key': Array.from(crypto.getRandomValues(new Uint8Array(16)), byte => byte.toString(16).padStart(2, '0')).join(''),
    'x-producer-id': session,
  });
  item(byId('sent-events'), `Accepted event ${result.id}`);
  status(`Event ${result.id} accepted. Waiting for matching deliveries.`);
});

byId('new-session').addEventListener('click', async () => {
  byId('new-session').disabled = true;
  try { await newSession(); }
  catch (error) { status(error.message, true); }
  finally { byId('new-session').disabled = false; }
});
byId('refresh').addEventListener('click', refresh);
setInterval(() => {
  if (byId('auto-refresh').checked && !document.hidden) refresh();
}, 2000);

async function initialize() {
  session = sessionStorage.getItem(storageKey);
  if (session) {
    byId('session-id').textContent = session;
    byId('event-type').value = `viewer.demo.${session}`;
    byId('rules').value = JSON.stringify({ type: byId('event-type').value }, null, 2);
    status('Session restored. Create a subscription or send another event.');
    await refresh();
  } else {
    await newSession();
  }
}

byId('connect-form').addEventListener('submit', async event => {
  event.preventDefault();
  apiToken = byId('api-token').value;
  byId('api-token').value = '';
  try { await initialize(); }
  catch (error) { status(error.message, true); }
});
byId('disconnect').addEventListener('click', () => {
  apiToken = '';
  session = undefined;
  sessionStorage.removeItem(storageKey);
  localStorage.removeItem(storageKey);
  shownAttempts.clear();
  byId('api-token').value = '';
  for (const name of ['subscriptions', 'deliveries', 'notifications', 'sent-events']) byId(name).replaceChildren();
  byId('session-id').textContent = 'Disconnected';
  status('Disconnected. Token and session cleared.');
});
byId('session-id').textContent = 'Enter API token to connect';
status('Paste the API token and connect. The token is kept in memory only.');
