// PBXware ARI conference demo.
//
// Holds one long-lived mixing bridge (the "conference") and shuffles call legs
// in and out of it through the Asterisk REST Interface. The browser never talks
// to ARI directly: it calls this server's /api/* endpoints and receives live
// state over Server-Sent Events, so ARI credentials stay server-side.
//
// Requires Node 22+ (global fetch + WebSocket). No npm dependencies.

'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const configPath = path.join(__dirname, 'config.json');
if (!fs.existsSync(configPath)) {
  console.error('Missing config.json. Copy config.example.json to config.json and fill it in.');
  process.exit(1);
}
const cfg = JSON.parse(fs.readFileSync(configPath, 'utf8'));
// Accept http(s)/ws(s) and an optional trailing /ari; we add /ari ourselves.
const ARI_BASE = cfg.ari.url.trim()
  .replace(/^ws(s?):\/\//i, 'http$1://')
  .replace(/\/+$/, '')
  .replace(/\/ari$/i, '');
const APP = cfg.ari.app || 'conf-demo';
const AUTH = 'Basic ' + Buffer.from(`${cfg.ari.username}:${cfg.ari.password}`).toString('base64');
const PORT = cfg.port || 3000;
// PBXware tenant code, substituted for {tenant} in endpoint templates.
const TENANT = String(cfg.tenant ?? '').trim();
if (TENANT && !/^[0-9A-Za-z_-]{1,16}$/.test(TENANT)) {
  console.error(`Invalid tenant "${TENANT}" in config.json`);
  process.exit(1);
}
for (const [role, tpl] of Object.entries(cfg.endpoints)) {
  if (tpl.includes('{tenant}') && !TENANT) {
    console.error(`endpoints.${role} uses {tenant} but config.json has no "tenant"`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/**
 * Leg: { id, role: 'internal'|'external', number, endpoint, status, cause,
 *        startedAt, answeredAt, endedAt }
 * status: dialing -> ringing -> answered -> in-conference -> ended | failed
 */
const state = {
  ariConnected: false,
  bridge: null,                         // { id, createdAt }
  legs: { internal: null, external: null }, // current leg per role
  history: [],                          // finished legs, newest first
};
const legsById = new Map();             // every leg we've originated, by channel id
let pendingExternal = null;             // number to dial once internal joins (initial start)
let ringback = null;                    // playback id of ring tone on the bridge

// ---------------------------------------------------------------------------
// SSE fan-out to browsers
// ---------------------------------------------------------------------------

const clients = new Set();
const logBuffer = [];

function send(res, event, data) {
  res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

function broadcastState() {
  for (const res of clients) send(res, 'state', state);
}

function log(kind, text, detail) {
  const entry = { ts: Date.now(), kind, text, detail };
  logBuffer.push(entry);
  if (logBuffer.length > 300) logBuffer.shift();
  for (const res of clients) send(res, 'log', entry);
  console.log(`[${kind}] ${text}`);
}

// ---------------------------------------------------------------------------
// ARI REST helper
// ---------------------------------------------------------------------------

async function ari(method, urlPath, query = {}) {
  const url = new URL(`${ARI_BASE}/ari${urlPath}`);
  for (const [k, v] of Object.entries(query)) {
    if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, v);
  }
  const res = await fetch(url, { method, headers: { Authorization: AUTH } });
  const text = await res.text();
  if (!res.ok) {
    const err = new Error(`${method} ${urlPath} -> ${res.status} ${text || res.statusText}`);
    err.status = res.status;
    throw err;
  }
  return text ? JSON.parse(text) : null;
}

// ---------------------------------------------------------------------------
// ARI event WebSocket (registers the Stasis app)
// ---------------------------------------------------------------------------

let warnedDown = false;

function connectEvents() {
  const wsUrl = new URL(`${ARI_BASE.replace(/^http/, 'ws')}/ari/events`);
  wsUrl.searchParams.set('app', APP);
  wsUrl.searchParams.set('api_key', `${cfg.ari.username}:${cfg.ari.password}`);
  wsUrl.searchParams.set('subscribeAll', 'false');

  const ws = new WebSocket(wsUrl.toString());

  ws.addEventListener('open', () => {
    state.ariConnected = true;
    warnedDown = false;
    log('system', `ARI WebSocket connected, Stasis app "${APP}" registered`);
    broadcastState();
  });

  ws.addEventListener('message', (msg) => {
    let evt;
    try { evt = JSON.parse(msg.data); } catch { return; }
    handleEvent(evt).catch((e) => log('error', `Handling ${evt.type}: ${e.message}`));
  });

  // A failed connect may fire only 'error' (no 'close'), so treat either as
  // the end of this socket, once.
  let ended = false;
  const onDown = async () => {
    if (ended) return;
    ended = true;
    if (!warnedDown) {
      warnedDown = true;
      const what = state.ariConnected ? 'closed' : 'could not connect';
      log('error', `ARI WebSocket ${what} (${ARI_BASE}): ${await diagnoseConnection()}. Retrying every 3s`);
    }
    state.ariConnected = false;
    broadcastState();
    setTimeout(connectEvents, 3000);
  };
  ws.addEventListener('close', onDown);
  ws.addEventListener('error', onDown);
}

// The WebSocket API gives no reason for a failed connect, so ask the REST
// side the same question and turn its answer into something actionable.
async function diagnoseConnection() {
  try {
    await ari('GET', '/asterisk/info');
    return 'REST works but the WebSocket upgrade failed (a proxy may not be forwarding WebSockets)';
  } catch (e) {
    if (e.status === 401) return 'username/password rejected (401)';
    if (e.status === 404) return `nothing at ${ARI_BASE}/ari (404), check ari.url`;
    if (e.status) return e.message;
    const detail = `${e.cause?.code || ''} ${e.cause?.message || e.message}`.trim();
    if (/WRONG_VERSION_NUMBER/i.test(detail)) return 'this port speaks plain HTTP, use http:// instead of https://';
    return `cannot reach server: ${detail}`;
  }
}

async function handleEvent(evt) {
  const channel = evt.channel;
  const leg = channel ? legsById.get(channel.id) : null;

  switch (evt.type) {
    case 'StasisStart': {
      if (!leg) {
        if (channel.id.startsWith(`${APP}-`)) {
          // One of ours that we already gave up on (e.g. answered after we hung up).
          log('ari', `StasisStart for stale leg ${channel.name}, hanging up`);
          await ari('DELETE', `/channels/${channel.id}`).catch(() => {});
        } else {
          log('ari', `StasisStart for unknown channel ${channel.name}, ignoring`);
        }
        return;
      }
      log('ari', `StasisStart: ${leg.role} ${leg.number} answered (${channel.name})`);
      setStatus(leg, 'answered');
      leg.answeredAt = Date.now();
      if (!state.bridge || leg.endedAt) {
        await hangup(leg, 'conference no longer active');
        return;
      }
      await ari('POST', `/bridges/${state.bridge.id}/addChannel`, { channel: leg.id });
      await onJoined(leg);
      break;
    }

    case 'ChannelStateChange': {
      if (!leg) return;
      log('ari', `ChannelStateChange: ${leg.role} ${leg.number} -> ${channel.state}`);
      if (channel.state === 'Ringing' && leg.status === 'dialing') setStatus(leg, 'ringing');
      break;
    }

    case 'Dial': {
      // Emitted for the outbound dial attempt; dialstatus is set when it resolves.
      const peerLeg = evt.peer && legsById.get(evt.peer.id);
      if (peerLeg && evt.dialstatus) {
        log('ari', `Dial: ${peerLeg.role} ${peerLeg.number} dialstatus=${evt.dialstatus}`);
      }
      break;
    }

    case 'ChannelEnteredBridge': {
      if (!leg) return;
      log('ari', `ChannelEnteredBridge: ${leg.role} ${leg.number} joined the conference`);
      await onJoined(leg);
      break;
    }

    case 'ChannelLeftBridge': {
      if (!leg) return;
      log('ari', `ChannelLeftBridge: ${leg.role} ${leg.number} left the conference`);
      break;
    }

    case 'StasisEnd': {
      if (!leg) return;
      log('ari', `StasisEnd: ${leg.role} ${leg.number}`);
      break;
    }

    case 'ChannelDestroyed': {
      if (!leg) return;
      const cause = evt.cause_txt || `cause ${evt.cause}`;
      log('ari', `ChannelDestroyed: ${leg.role} ${leg.number} (${cause})`);
      endLeg(leg, leg.answeredAt ? 'ended' : 'failed', cause);
      break;
    }

    default:
      break;
  }
}

// ---------------------------------------------------------------------------
// Conference operations
// ---------------------------------------------------------------------------

// Called from both the addChannel success and ChannelEnteredBridge; whichever
// lands first wins, the second is a no-op.
async function onJoined(leg) {
  if (leg.endedAt || leg.status === 'in-conference') return;
  setStatus(leg, 'in-conference');
  if (leg.role === 'external') await stopRingback();
  if (leg.role === 'internal' && pendingExternal) {
    const number = pendingExternal;
    pendingExternal = null;
    await dialLeg('external', number);
  }
}

function setStatus(leg, status) {
  if (leg.endedAt) return; // late events for a leg we've already closed out
  leg.status = status;
  broadcastState();
}

function endLeg(leg, status, cause) {
  if (!leg.endedAt) {
    leg.status = status;
    leg.cause = leg.cause || cause;
    leg.endedAt = Date.now();
    state.history.unshift({ ...leg });
    if (state.history.length > 20) state.history.pop();
  }
  legsById.delete(leg.id);

  // Leave the ended leg visible in its slot (so the UI can show the cause)
  // until it's replaced; just stop any ringback if the dialing external died.
  if (state.legs.external === leg) {
    stopRingback().catch(() => {});
  }
  if (state.legs.internal === leg && pendingExternal) {
    log('system', 'Internal leg did not join; skipping the queued external dial');
    pendingExternal = null;
  }
  broadcastState();
}

async function hangup(leg, reason) {
  if (!leg || leg.endedAt) return;
  leg.cause = reason;
  try {
    await ari('DELETE', `/channels/${leg.id}`, { reason: 'normal' });
  } catch (e) {
    if (e.status !== 404) throw e; // 404 = already gone
  }
  // ChannelDestroyed normally finalises the leg, but close it out now so a
  // replacement leg can take its slot immediately.
  endLeg(leg, leg.answeredAt ? 'ended' : 'failed', reason);
}

function sanitizeNumber(raw) {
  // Only dialable characters, so the number can't smuggle "@context" or
  // "&OtherChannel" into the endpoint string.
  const n = String(raw || '').trim().replace(/[\s\-().]/g, '');
  if (!/^\+?[0-9*#]{1,32}$/.test(n)) throw httpError(400, `Invalid number: "${raw}"`);
  return n;
}

async function dialLeg(role, rawNumber) {
  if (!state.bridge) throw httpError(409, 'No active conference');
  const number = sanitizeNumber(rawNumber);

  // Replace whatever currently occupies this role's slot.
  const current = state.legs[role];
  if (current && !current.endedAt) {
    log('action', `Hanging up ${role} ${current.number} to make room for ${number}`);
    await hangup(current, `replaced by ${number}`);
  }

  const endpoint = cfg.endpoints[role].replaceAll('{tenant}', TENANT).replaceAll('{number}', number);
  const leg = {
    id: `${APP}-${role}-${crypto.randomUUID().slice(0, 8)}`,
    role, number, endpoint,
    status: 'dialing', cause: null,
    startedAt: Date.now(), answeredAt: null, endedAt: null,
  };
  legsById.set(leg.id, leg);
  state.legs[role] = leg;
  broadcastState();
  log('action', `Originating ${role} leg to ${number} via ${endpoint}`);

  if (role === 'external') await startRingback();

  try {
    await ari('POST', '/channels', {
      endpoint,
      app: APP,
      appArgs: role,
      channelId: leg.id,
      callerId: cfg.callerId,
      timeout: cfg.dialTimeout || 30,
    });
  } catch (e) {
    const why = await diagnoseOriginate(endpoint);
    log('error', `Originate to ${endpoint} failed: ${why || e.message.replace(/\s+/g, ' ')}`);
    endLeg(leg, 'failed', why || 'originate rejected');
  }
  return leg;
}

// "Allocation failed" usually means the device can't be reached. For direct
// device endpoints (not Local/ dialplan ones), ask ARI which case it is.
async function diagnoseOriginate(endpoint) {
  const m = endpoint.match(/^([A-Za-z]+)\/([^@\/]+)$/);
  if (!m || m[1].toLowerCase() === 'local') return null;
  try {
    const ep = await ari('GET', `/endpoints/${m[1]}/${encodeURIComponent(m[2])}`);
    if (ep.state === 'offline') return `${endpoint} is offline (no device registered)`;
    return null;
  } catch (e) {
    return e.status === 404 ? `${endpoint} does not exist on the PBX` : null;
  }
}

async function startRingback() {
  if (!cfg.ringbackMedia || ringback || !state.bridge) return;
  // Only useful if someone is already in the room to hear it.
  const internal = state.legs.internal;
  if (!internal || internal.status !== 'in-conference') return;
  const id = `${APP}-ring-${crypto.randomUUID().slice(0, 8)}`;
  try {
    await ari('POST', `/bridges/${state.bridge.id}/play`, { media: cfg.ringbackMedia, playbackId: id });
    ringback = id;
  } catch (e) {
    log('error', `Ringback failed: ${e.message}`);
  }
}

async function stopRingback() {
  if (!ringback) return;
  const id = ringback;
  ringback = null;
  try { await ari('DELETE', `/playbacks/${id}`); } catch { /* already finished */ }
}

async function startConference(internalNumber, externalNumber) {
  if (!state.ariConnected) throw httpError(503, 'ARI is not connected');
  if (state.bridge) throw httpError(409, 'A conference is already running');
  sanitizeNumber(internalNumber);
  if (externalNumber) sanitizeNumber(externalNumber);

  const bridgeId = `${APP}-bridge-${crypto.randomUUID().slice(0, 8)}`;
  await ari('POST', '/bridges', { type: 'mixing', bridgeId, name: APP });
  state.bridge = { id: bridgeId, createdAt: Date.now() };
  // Subscribe to the bridge so ChannelEnteredBridge/ChannelLeftBridge reach the app.
  await ari('POST', `/applications/${APP}/subscription`, { eventSource: `bridge:${bridgeId}` })
    .catch((e) => log('error', `Bridge subscription failed (non-fatal): ${e.message}`));
  state.legs = { internal: null, external: null };
  log('action', `Created mixing bridge ${bridgeId}`);

  pendingExternal = externalNumber ? sanitizeNumber(externalNumber) : null;
  await dialLeg('internal', internalNumber);
}

async function endConference() {
  if (!state.bridge) return;
  pendingExternal = null;
  await stopRingback();
  for (const role of ['external', 'internal']) {
    try { await hangup(state.legs[role], 'conference ended'); } catch (e) { log('error', e.message); }
  }
  try {
    await ari('DELETE', `/bridges/${state.bridge.id}`);
  } catch (e) {
    if (e.status !== 404) log('error', e.message);
  }
  log('action', `Destroyed bridge ${state.bridge.id}`);
  state.bridge = null;
  broadcastState();
}

// ---------------------------------------------------------------------------
// HTTP server: static page, JSON API, SSE stream
// ---------------------------------------------------------------------------

function httpError(status, message) {
  const e = new Error(message);
  e.status = status;
  return e;
}

function readJson(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (c) => { body += c; if (body.length > 1e5) req.destroy(); });
    req.on('end', () => {
      try { resolve(body ? JSON.parse(body) : {}); } catch { reject(httpError(400, 'Bad JSON')); }
    });
    req.on('error', reject);
  });
}

const routes = {
  'POST /api/start': (b) => startConference(b.internal, b.external),
  'POST /api/dial': (b) => {
    if (!['internal', 'external'].includes(b.role)) throw httpError(400, 'role must be internal or external');
    return dialLeg(b.role, b.number);
  },
  'POST /api/hangup': (b) => hangup(state.legs[b.role], 'hung up from demo page'),
  'POST /api/end': () => endConference(),
};

const server = http.createServer(async (req, res) => {
  const { pathname } = new URL(req.url, 'http://localhost');

  if (req.method === 'GET' && pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    fs.createReadStream(path.join(__dirname, 'public', 'index.html')).pipe(res);
    return;
  }

  if (req.method === 'GET' && pathname === '/api/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });
    send(res, 'config', { app: APP, ari: ARI_BASE, tenant: TENANT, endpoints: cfg.endpoints });
    send(res, 'state', state);
    for (const entry of logBuffer) send(res, 'log', entry);
    clients.add(res);
    req.on('close', () => clients.delete(res));
    return;
  }

  const handler = routes[`${req.method} ${pathname}`];
  if (!handler) {
    res.writeHead(404).end();
    return;
  }
  try {
    await handler(await readJson(req));
    res.writeHead(200, { 'Content-Type': 'application/json' }).end('{"ok":true}');
  } catch (e) {
    log('error', e.message);
    res.writeHead(e.status && e.status < 600 ? e.status : 500, { 'Content-Type': 'application/json' })
      .end(JSON.stringify({ ok: false, error: e.message }));
  }
});

// Keep SSE connections from idling out behind proxies.
setInterval(() => { for (const res of clients) res.write(': ping\n\n'); }, 25000);

process.on('SIGINT', async () => {
  console.log('\nShutting down, tearing down conference...');
  try { await endConference(); } catch { /* best effort */ }
  process.exit(0);
});

server.listen(PORT, () => {
  console.log(`Demo page: http://localhost:${PORT}`);
  connectEvents();
});
