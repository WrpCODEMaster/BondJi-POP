// Pop Cat ทีม — plain Node.js, zero npm dependencies (works on Render's free web service).
// Implements just enough of RFC 6455 (WebSocket) to run this game; one process holds all
// rooms in memory, which is fine for a small-group game on a single instance.
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const DURS = [1, 3, 5, 10];
const PAGE = fs.readFileSync(path.join(__dirname, 'public', 'index.html'));
const WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const rooms = new Map(); // code -> room

// ---- minimal WebSocket framing ----
function encodeFrame(str) {
  const payload = Buffer.from(str);
  const len = payload.length;
  let header;
  if (len < 126) header = Buffer.from([0x81, len]);
  else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x81; header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x81; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([header, payload]);
}
function decodeFrames(buf, onMessage) {
  let off = 0;
  while (off + 2 <= buf.length) {
    const b0 = buf[off], b1 = buf[off + 1];
    const opcode = b0 & 0x0f, masked = !!(b1 & 0x80);
    let len = b1 & 0x7f, p = off + 2;
    if (len === 126) { if (p + 2 > buf.length) break; len = buf.readUInt16BE(p); p += 2; }
    else if (len === 127) { if (p + 8 > buf.length) break; len = Number(buf.readBigUInt64BE(p)); p += 8; }
    let mask;
    if (masked) { if (p + 4 > buf.length) break; mask = buf.slice(p, p + 4); p += 4; }
    if (p + len > buf.length) break;
    let data = buf.slice(p, p + len);
    if (masked) { data = Buffer.from(data); for (let i = 0; i < data.length; i++) data[i] ^= mask[i % 4]; }
    if (opcode === 1) onMessage(data.toString('utf8'));
    else if (opcode === 8) onMessage(null); // close
    off = p + len;
  }
  return buf.slice(off);
}

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(PAGE);
  }
  res.writeHead(404); res.end('Not found');
});

server.on('upgrade', (req, socket) => {
  const m = (req.url || '').match(/^\/ws\/([A-Za-z0-9]{4})$/);
  const key = req.headers['sec-websocket-key'];
  if (!m || !key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + WS_MAGIC).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.ws = { send: (str) => { try { socket.write(encodeFrame(str)); } catch (e) {} } };
  let buf = Buffer.alloc(0);
  socket.on('data', (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    buf = decodeFrames(buf, (msg) => { if (msg === null) socket.end(); else onMessage(socket, m[1].toUpperCase(), msg); });
  });
  socket.on('error', () => onClose(socket, m[1].toUpperCase()));
  socket.on('close', () => onClose(socket, m[1].toUpperCase()));
});

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
    for (const s of room.socks) { try { s.ws.send(j); } catch (e) { room.socks.delete(s); } }
  }, 100);
}
function send(socket, o) { socket.ws.send(JSON.stringify(o)); }

function onClose(socket, code) {
  const room = rooms.get(code);
  if (room) room.socks.delete(socket);
}

function onMessage(socket, code, raw) {
  let m; try { m = JSON.parse(raw); } catch (e) { return; }
  const pid = String(m.pid || '').slice(0, 16);
  if (!pid) return;
  let room = rooms.get(code);
  const host = !!room && room.hostId === pid, now = Date.now();

  if (m.t === 'create') {
    if (room) return send(socket, { t: 'err', m: 'exists' });
    const teams = Array.from({ length: Math.min(6, Math.max(2, (m.teams || []).length || 0)) },
      (_, i) => String((m.teams || [])[i] || '').trim().slice(0, 14) || 'ทีม ' + (i + 1));
    room = { hostId: pid, teams, status: 'lobby', dur: 5, startAt: 0, endAt: 0, players: {}, socks: new Set() };
    rooms.set(code, room); room.socks.add(socket); return broadcast(room);
  }
  if (!room) return send(socket, { t: 'err', m: 'ไม่พบห้อง' });
  room.socks.add(socket);
  if (m.t === 'join') return send(socket, json(room));
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
    for (const s of room.socks) { try { s.ws.send(j); } catch (e) {} }
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
