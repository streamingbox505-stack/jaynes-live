// JAYNES LIVE: WebSocket ingest -> ffmpeg -> HLS
const http = require('http');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const KEY = process.env.STREAM_KEY || '';
const DIR = '/tmp/hls';
fs.mkdirSync(DIR, { recursive: true });

let current = null; // { ws, ff }
let startedAt = 0;

function clean() {
  for (const f of fs.readdirSync(DIR)) {
    try { fs.unlinkSync(path.join(DIR, f)); } catch (e) {}
  }
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', '*');
}

const server = http.createServer((req, res) => {
  cors(res);
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const u = new URL(req.url, 'http://x');

  if (u.pathname === '/' || u.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, live: !!current, since: startedAt }));
  }

  if (u.pathname.startsWith('/live/')) {
    const name = path.basename(u.pathname);
    const file = path.join(DIR, name);
    fs.readFile(file, (err, data) => {
      if (err) { res.writeHead(404); return res.end('not found'); }
      const isM3u8 = name.endsWith('.m3u8');
      res.writeHead(200, {
        'Content-Type': isM3u8 ? 'application/vnd.apple.mpegurl' : 'video/mp2t',
        'Cache-Control': isM3u8 ? 'no-store' : 'public, max-age=60'
      });
      res.end(data);
    });
    return;
  }
  res.writeHead(404); res.end('not found');
});

function stopCurrent() {
  if (!current) return;
  try { current.ff.stdin.end(); } catch (e) {}
  try { current.ff.kill('SIGTERM'); } catch (e) {}
  try { current.ws.close(); } catch (e) {}
  current = null;
}

function startFfmpeg(mode) {
  const input = ['-loglevel', 'warning', '-fflags', '+genpts', '-i', 'pipe:0'];
  const video = mode === 'encode'
    ? ['-c:v', 'libx264', '-preset', 'ultrafast', '-tune', 'zerolatency',
       '-vf', 'scale=-2:480', '-b:v', '800k', '-g', '48', '-sc_threshold', '0']
    : ['-c:v', 'copy'];
  const audio = ['-c:a', 'aac', '-b:a', '96k', '-ar', '44100'];
  const hls = ['-f', 'hls', '-hls_time', '4', '-hls_list_size', '5',
    '-hls_flags', 'delete_segments+independent_segments',
    '-hls_segment_filename', path.join(DIR, 'seg%05d.ts'),
    path.join(DIR, 'stream.m3u8')];
  const ff = spawn('ffmpeg', [...input, ...video, ...audio, ...hls]);
  ff.stderr.on('data', d => console.log('[ffmpeg]', d.toString().trim()));
  ff.stdin.on('error', () => {});
  return ff;
}

const wss = new WebSocketServer({ server, path: '/ingest', maxPayload: 16 * 1024 * 1024 });

wss.on('connection', (ws, req) => {
  const u = new URL(req.url, 'http://x');
  if (KEY && u.searchParams.get('key') !== KEY) { ws.close(1008, 'bad key'); return; }
  const mode = u.searchParams.get('mode') === 'encode' ? 'encode' : 'copy';

  stopCurrent();
  clean();
  const ff = startFfmpeg(mode);
  current = { ws, ff };
  startedAt = Date.now();
  console.log('ingest started, mode =', mode);

  ws.on('message', data => { if (!ff.stdin.destroyed) ff.stdin.write(data); });
  ws.on('close', () => { if (current && current.ws === ws) stopCurrent(); console.log('ingest closed'); });
  ws.on('error', () => {});
  ff.on('exit', code => console.log('ffmpeg exit', code));
});

server.listen(PORT, () => console.log('JAYNES LIVE on port', PORT));
                     
