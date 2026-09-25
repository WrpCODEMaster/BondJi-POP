// Pop Cat ทีม — Node.js + ws (https://github.com/websockets/ws), for Render.com free web service.
// One process holds all rooms in memory (fine for a small-group game on a single instance).
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

process.on('uncaughtException', (e) => console.error('uncaught:', e));
process.on('unhandledRejection', (e) => console.error('unhandled rejection:', e));

const DURS = [1, 3, 5, 10];
const PAGE = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const rooms = new Map(); // code -> { hostId, teams, status, dur, startAt, endAt, players, socks:Set }

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(PAGE);
  }
  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocketServer({ noServer: true });
server.on('upgrade', (req, socket, head) => {
  const m = (req.url || '').match(/^\/ws\/([A-Za-z0-9]{4})$/);
  console.log('[upgrade]', req.url, 'match=', !!m);
  if (!m) { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, m[1].toUpperCase()));
});

// heartbeat: 'ws' won't detect a dead connection on its own, so ping regularly and
// drop anything that doesn't answer — this also keeps mobile-carrier / proxy idle timeouts from firing
function heartbeat() { this.alive = true; }
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.alive === false) { console.log('[heartbeat] terminating dead socket'); return ws.terminate(); }
    ws.alive = false;
    try { ws.ping(); } catch (e) {}
  });
}, 8000);

function json(room) {
  return JSON.stringify({
    t: 'state', now: Date.now(), hostId: room.hostId, teams: room.teams, status: room.status,
    dur: room.dur, startAt: room.startAt, endAt: room.endAt,
    players: Object.entries(room.players).map(([id, p]) => ({ id, ...p })),
  });
}
function broadcast(room) {
  clearTimeout(room.tm);
  room.tm = setTimeout(() => {
    const j = json(room);
    for (const s of room.socks) { try { s.send(j); } catch (e) { room.socks.delete(s); } }
  }, 100);
}
function send(ws, o) { try { ws.send(JSON.stringify(o)); } catch (e) {} }

wss.on('connection', (ws, code) => {
  ws.alive = true;
  ws.on('pong', heartbeat);
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    try { onMessage(ws, code, m); } catch (e) { console.error('handler error:', e); }
  });
  ws.on('close', () => { const room = rooms.get(code); if (room) room.socks.delete(ws); });
  ws.on('error', () => {});
});

function onMessage(ws, code, m) {
  const pid = String(m.pid || '').slice(0, 16);
  if (!pid) return;
  let room = rooms.get(code);
  const host = !!room && room.hostId === pid, now = Date.now();
  console.log('[msg]', code, m.t, 'pid=', pid, 'roomExists=', !!room);

  if (m.t === 'create') {
    if (room) return send(ws, { t: 'err', m: 'exists' });
    const teams = Array.from({ length: Math.min(6, Math.max(2, (m.teams || []).length || 0)) },
      (_, i) => String((m.teams || [])[i] || '').trim().slice(0, 14) || 'ทีม ' + (i + 1));
    room = { hostId: pid, teams, status: 'lobby', dur: 5, startAt: 0, endAt: 0, players: {}, socks: new Set() };
    rooms.set(code, room); room.socks.add(ws); return broadcast(room);
  }
  if (!room) return send(ws, { t: 'err', m: 'ไม่พบห้อง' });
  room.socks.add(ws);
  if (m.t === 'join') return send(ws, json(room));
  if (m.t === 'player' && room.status === 'lobby' && !host) {
    const name = String(m.name || '').trim().slice(0, 16), team = m.team | 0;
    if (!name || team < 0 || team >= room.teams.length) return;
    const p = room.players[pid];
    if (p) { p.name = name; p.team = team; } else room.players[pid] = { name, team, score: 0 };
    return broadcast(room);
  }
  if (m.t === 'start' && host && room.status === 'lobby' && Object.keys(room.players).length) {
    room.dur = DURS.includes(m.dur) ? m.dur : 5; room.status = 'run';
    room.startAt = now + 3000; room.endAt = room.startAt + room.dur * 60000;
    return broadcast(room);
  }
  if (m.t === 'score' && room.status === 'run' && room.players[pid] && now < room.endAt + 3000) {
    const p = room.players[pid];
    const max = Math.floor((Math.min(now, room.endAt) - room.startAt) / 1000 * 25) + 30;
    const v = Math.min(m.s | 0, max);
    if (v > p.score) { p.score = v; broadcast(room); }
    return;
  }
  if (m.t === 'reset' && host && room.status === 'run' && now >= room.endAt) {
    room.status = 'lobby'; for (const p of Object.values(room.players)) p.score = 0;
    return broadcast(room);
  }
  if (m.t === 'leave' && room.players[pid]) { delete room.players[pid]; return broadcast(room); }
  if (m.t === 'destroy' && host) {
    rooms.delete(code);
    const j = JSON.stringify({ t: 'state', now: Date.now(), status: 'gone' });
    for (const s of room.socks) { try { s.send(j); } catch (e) {} }
    room.socks.clear();
    return;
  }
}

// clear rooms nobody is connected to and that ended more than 2h ago
setInterval(() => {
  const cutoff = Date.now() - 2 * 3600 * 1000;
  for (const [code, room] of rooms) if (room.socks.size === 0 && (room.endAt || 0) < cutoff) rooms.delete(code);
}, 30 * 60 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('listening on', PORT));
