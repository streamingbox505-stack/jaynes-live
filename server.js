// JAYNES LIVE: HTTP chunk ingest -> ffmpeg -> HLS (no WebSocket)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const PORT = process.env.PORT || 3000;
const KEY = process.env.STREAM_KEY || '';
const DIR = '/tmp/hls';
fs.mkdirSync(DIR, { recursive: true });

let ff = null;
let startedAt = 0;
let lastChunk = 0;
let curMode = 'copy';
const logs = [];

function clean() {
  for (const f of fs.readdirSync(DIR)) {
    try { fs.unlinkSync(path.join(DIR, f)); } catch (e) {}
  }
}

function stopFfmpeg() {
  if (!ff) return;
  try { ff.stdin.end(); } catch (e) {}
  const p = ff;
  setTimeout(() => { try { p.kill('SIGTERM'); } catch (e) {} }, 3000);
  ff = null;
}

function startFfmpeg(mode) {
  const input = ['-loglevel', 'warning', '-fflags', '+genpts', '-i', 'pipe:0'];
  const video = mode === 'encode'
    ? ['-c:v', 'libx264', '-preset', 'ultrafast', '-tune', 'zerolatency',
       '-vf', 'scale=-2:480', '-b:v', '800k', '-g', '48', '-sc_threshold', '0']
    : ['-c:v', 'copy'];
  const audio = ['-c:a', 'aac', '-b:a', '96k', '-ar', '44100'];
  const hls = ['-f', 'hls', '-hls_time', '3', '-hls_list_size', '6',
    '-hls_flags', 'delete_segments+independent_segments',
    '-hls_segment_filename', path.join(DIR, 'seg%05d.ts'),
    path.join(DIR, 'stream.m3u8')];
  const p = spawn('ffmpeg', [...input, ...video, ...audio, ...hls]);
  p.stderr.on('data', d => { const t = d.toString().trim(); console.log('[ffmpeg]', t); logs.push(t); if (logs.length > 40) logs.shift(); });
  p.stdin.on('error', () => {});
  p.on('exit', code => { console.log('ffmpeg exit', code); if (ff === p) ff = null; });
  return p;
}

// stop automatically if the studio disappears
setInterval(() => {
  if (ff && Date.now() - lastChunk > 20000) { console.log('no chunks, stopping'); stopFfmpeg(); }
}, 5000);

function send(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const u = new URL(req.url, 'http://x');

  if (u.pathname === '/' || u.pathname === '/health') {
    let segs = 0; try { segs = fs.readdirSync(DIR).filter(f => f.endsWith('.ts')).length; } catch (e) {}
    return send(res, 200, { ok: true, live: !!ff, since: startedAt, mode: curMode,
      lastChunkAgo: ff ? Math.round((Date.now() - lastChunk) / 1000) : null,
      playlist: fs.existsSync(path.join(DIR, 'stream.m3u8')), segments: segs });
  }

  if (u.pathname === '/log') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end(logs.join('\n')); }

  if (u.pathname.startsWith('/live/')) {
    const name = path.basename(u.pathname);
    return fs.readFile(path.join(DIR, name), (err, data) => {
      if (err) { res.writeHead(404); return res.end('not found'); }
      const isM3u8 = name.endsWith('.m3u8');
      res.writeHead(200, {
        'Content-Type': isM3u8 ? 'application/vnd.apple.mpegurl' : 'video/mp2t',
        'Cache-Control': isM3u8 ? 'no-store' : 'public, max-age=60'
      });
      res.end(data);
    });
  }

  if (req.method === 'POST' && u.pathname.startsWith('/ingest/')) {
    if (KEY && u.searchParams.get('key') !== KEY) return send(res, 401, { error: 'bad key' });

    if (u.pathname === '/ingest/start') {
      stopFfmpeg();
      clean();
      curMode = u.searchParams.get('mode') === 'encode' ? 'encode' : 'copy';
      logs.length = 0;
      ff = startFfmpeg(curMode);
      startedAt = Date.now();
      lastChunk = Date.now();
      return send(res, 200, { started: true });
    }

    if (u.pathname === '/ingest/chunk') {
      if (!ff) return send(res, 409, { error: 'not started' });
      lastChunk = Date.now();
      const parts = [];
      req.on('data', c => parts.push(c));
      req.on('end', () => {
        const buf = Buffer.concat(parts);
        if (!ff || ff.stdin.destroyed) return send(res, 409, { error: 'stopped' });
        ff.stdin.write(buf, () => send(res, 200, { ok: true, bytes: buf.length }));
      });
      return;
    }

    if (u.pathname === '/ingest/stop') {
      stopFfmpeg();
      return send(res, 200, { stopped: true });
    }
  }

  res.writeHead(404); res.end('not found');
});

server.listen(PORT, () => console.log('JAYNES LIVE on port', PORT));
             
