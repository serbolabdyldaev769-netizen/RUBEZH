// RUBEZH — сервер комнат (WebSocket). Раздаёт index.html и ретранслирует события между игроками.
const http = require('http'), fs = require('fs'), path = require('path'), zlib = require('zlib');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX = +process.env.MAX_ROOM || 12;
const INDEX = path.join(__dirname, 'index.html');
let html = null, gz = null;
try { html = fs.readFileSync(INDEX); gz = zlib.gzipSync(html, { level: 9 }); } catch (_) { console.log('index.html не найден: сервер работает только для APK'); }

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/health') { res.writeHead(200); return res.end('ok'); }
  if ((u === '/' || u === '/index.html') && !html) { res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' }); return res.end('RUBEZH server: ok'); }
  if (u === '/' || u === '/index.html') {
    const h = { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' };
    if (/gzip/.test(req.headers['accept-encoding'] || '')) { h['Content-Encoding'] = 'gzip'; res.writeHead(200, h); return res.end(gz); }
    res.writeHead(200, h); return res.end(html);
  }
  res.writeHead(404); res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 256 * 1024 });
const rooms = new Map();
const cleanCf = c => { c = c || {}; return { team: c.team === 'rf' ? 'rf' : 'vsu', gm: c.gm === 'atk' ? 'atk' : 'def', tod: c.tod === 'night' ? 'night' : 'day', fac: c.fac === 'reg' ? 'reg' : 'elite', enemyFac: c.enemyFac === 'reg' ? 'reg' : 'elite' }; };

const send = (ws, e, d) => { if (ws.readyState === 1) ws.send(JSON.stringify({ e, d })); };
const lst = R => [...R.p.values()].map(p => ({ id: p.id, name: p.name, team: p.team, host: p.id === R.host }));
const to = (R, id, e, d) => { const p = R.p.get(id); if (p) send(p.ws, e, d); };
const all = (R, e, d, ex) => { for (const p of R.p.values()) if (p.id !== ex) send(p.ws, e, d); };
const push = R => all(R, 'room', { code: R.c, mode: R.mode, map: R.map, cf: R.cf, started: R.started, players: lst(R) });

function add(R, ws, name) {
  const V = [...R.p.values()], A = V.filter(p => p.team === 'A').length, B = V.filter(p => p.team === 'B').length;
  const id = Math.random().toString(36).slice(2, 10);
  const p = { id, ws, name: String(name || 'Игрок').slice(0, 14), team: R.mode === 'coop' ? 'A' : (A <= B ? 'A' : 'B') };
  R.p.set(id, p); if (!R.host) R.host = id;
  return p;
}

function leave(R, p) {
  if (!R.p.delete(p.id)) return;
  all(R, 'left', p.id);
  if (R.host === p.id) R.host = R.p.size ? R.p.keys().next().value : null;
  if (!R.p.size) { rooms.delete(R.c); return; }
  push(R);
}

function handle(R, p, e, d) {
  const isHost = R.host === p.id;
  switch (e) {
    case 'team': if (R.mode === 'pvp' && !R.started && (d === 'A' || d === 'B')) { p.team = d; push(R); } break;
    case 'mode': if (isHost && !R.started) { R.mode = d === 'pvp' ? 'pvp' : 'coop'; if (R.mode === 'coop') for (const q of R.p.values()) q.team = 'A'; push(R); } break;
    case 'map': if (isHost && !R.started && +d >= 1 && +d <= 6) { R.map = +d; push(R); } break;
    case 'start': if (isHost) { R.started = true; all(R, 'start', { mode: R.mode, players: lst(R) }); push(R); } break;
    case 'st': case 'shot': all(R, e, { id: p.id, ...d }, p.id); break;
    case 'snap': if (isHost) all(R, 'snap', d, p.id); break;
    case 'boom': all(R, 'boom', d, p.id); break;
    case 'hit': {
      const t = R.p.get(d && d.to);
      if (!t || t.id === p.id || R.mode === 'coop' || p.team === t.team) break;
      to(R, t.id, 'hit', { from: p.id, name: p.name, dmg: Math.min(Math.max(+d.dmg || 0, 0), 200), pos: d.pos });
      break;
    }
    case 'dead': {
      const k = R.p.get(d && d.by);
      all(R, 'feed', { t: k ? k.name + ' ☠ ' + p.name : p.name + ' погиб', id: p.id });
      break;
    }
    case 'chat': {
      let t = String(d || '').slice(0, 120), tm = false; if (!t) break;
      if (t.startsWith('/t ')) { tm = true; t = t.slice(3); }
      for (const q of R.p.values()) if (!tm || q.team === p.team) send(q.ws, 'chat', { name: p.name, team: p.team, msg: t, tm });
      break;
    }
    case 'vhit': if (!isHost) to(R, R.host, 'vhit', d); break;
    case 'ehit': if (!isHost) to(R, R.host, 'ehit', { n: d.n, d: Math.min(+d.d || 0, 300), c: String(d.c || 'rifle'), h: !!d.h }); break;
    case 'bh': if (isHost && R.p.has(d.to)) to(R, d.to, 'hit', { from: 'bot', name: 'Бот', dmg: Math.min(+d.dmg || 0, 200), pos: d.pos }); break;
  }
}

wss.on('connection', ws => {
  let R = null, me = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || typeof m.e !== 'string') return;
    if (!me) {
      if (m.e !== 'hello') return;
      const h = m.d || {}, code = String(h.code || '');
      if (!/^(\d{4}|PUB)$/.test(code)) return send(ws, 'none');
      let room = rooms.get(code);
      if (h.create) {
        if (room) return send(ws, 'taken');
        room = { c: code, host: null, mode: h.mode === 'pvp' ? 'pvp' : 'coop', map: +h.map || 1, cf: cleanCf(h.cf), started: false, p: new Map() };
        rooms.set(code, room);
      } else if (!room) {
        if (code !== 'PUB') return send(ws, 'none');
        room = { c: 'PUB', host: null, mode: 'coop', map: +h.map || 1, cf: cleanCf(h.cf), started: true, p: new Map() };
        rooms.set('PUB', room);
      }
      if (room.p.size >= MAX) return send(ws, 'full');
      R = room; me = add(R, ws, h.name);
      send(ws, 'welcome', { id: me.id });
      push(R);
      return;
    }
    handle(R, me, m.e, m.d);
  });
  ws.on('close', () => { if (R && me) leave(R, me); });
  ws.on('error', () => {});
});

setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch (_) {} }
}, 15000);

server.listen(PORT, () => console.log('RUBEZH server on :' + PORT));
