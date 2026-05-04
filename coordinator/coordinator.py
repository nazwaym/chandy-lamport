"""
=============================================================
COORDINATOR NODE - Algoritma Chandy-Lamport
=============================================================
Fungsi: Mengirim sinyal MARKER, mengumpulkan ACK dari semua
worker, dan merekam status sesi checkpoint secara global.
=============================================================
"""

import socket
import threading
import time
import uuid
import json
import pickle
import hashlib
import os
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

# ─── Konfigurasi Logging ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] COORDINATOR | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('coordinator')

# ─── Konfigurasi Sistem ───────────────────────────────────
COORDINATOR_HOST = '0.0.0.0'
COORDINATOR_PORT = 5000
CHECKPOINT_DIR   = '/checkpoint_storage'
CHECKPOINT_INTERVAL = 30  # detik antar checkpoint periodik
HEARTBEAT_INTERVAL  = 5   # detik antar heartbeat
MARKER_TIMEOUT      = 30  # detik tunggu ACK dari worker

# Daftar worker: nama → (host, port)
WORKERS = {
    'worker1': ('worker1', 6001),
    'worker2': ('worker2', 6002),
    'worker3': ('worker3', 6003),
}

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'db'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'checkpoint_db'),
    'user':     os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASS', 'admin123'),
}


# ─── Helper Database ──────────────────────────────────────
def get_db():
    """Buat koneksi ke PostgreSQL."""
    return psycopg2.connect(**DB_CONFIG)


def register_node(name, ip, role):
    """Daftarkan node ke tabel nodes."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO nodes (node_name, ip_address, role, status, last_heartbeat)
                VALUES (%s, %s, %s, 'active', NOW())
                ON CONFLICT (node_name) DO UPDATE
                SET ip_address = EXCLUDED.ip_address,
                    status = 'active',
                    last_heartbeat = NOW()
            """, (name, ip, role))
            conn.commit()


def update_heartbeat(node_name):
    """Perbarui waktu heartbeat node."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nodes SET last_heartbeat = NOW() WHERE node_name = %s",
                (node_name,)
            )
            conn.commit()


def record_session(session_id, trigger_type, total_nodes):
    """Catat sesi checkpoint baru ke database."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO checkpoint_sessions
                    (session_id, trigger_type, global_status, total_nodes, started_at)
                VALUES (%s, %s, 'running', %s, NOW())
                ON CONFLICT (session_id) DO NOTHING
            """, (session_id, trigger_type, total_nodes))
            conn.commit()


def update_session(session_id, status, acked_nodes, duration_ms):
    """Update status sesi checkpoint setelah selesai."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE checkpoint_sessions
                SET global_status  = %s,
                    acked_nodes    = %s,
                    completed_at   = NOW(),
                    duration_ms    = %s
                WHERE session_id = %s
            """, (status, acked_nodes, duration_ms, session_id))
            conn.commit()


def save_coordinator_checkpoint(session_id):
    """Simpan state koordinator sendiri ke file .ckpt."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    state = {
        'node':       'coordinator',
        'session_id': session_id,
        'timestamp':  datetime.now().isoformat(),
        'workers':    list(WORKERS.keys()),
        'status':     'active',
    }
    filename  = f'coordinator_{session_id[:8]}.ckpt'
    filepath  = os.path.join(CHECKPOINT_DIR, filename)
    data      = pickle.dumps(state)
    checksum  = hashlib.sha256(data).hexdigest()

    with open(filepath, 'wb') as f:
        f.write(data)

    # Simpan metadata ke DB
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT node_id FROM nodes WHERE node_name = 'coordinator'")
            row = cur.fetchone()
            if row:
                cur.execute("""
                    INSERT INTO checkpoints
                        (node_id, session_id, sequence_number, file_path, checksum,
                         file_size_bytes, status)
                    VALUES (%s, %s,
                        (SELECT COALESCE(MAX(sequence_number),0)+1
                         FROM checkpoints WHERE node_id = %s),
                        %s, %s, %s, 'valid')
                """, (row[0], session_id, row[0], filepath, checksum, len(data)))
                conn.commit()

    log.info(f"[CKPT] Coordinator state disimpan → {filename}")
    return filepath, checksum


# ─── Algoritma Chandy-Lamport: Inisiasi Checkpoint ────────
def initiate_checkpoint(trigger_type='periodic'):
    """
    Prosedur utama algoritma Chandy-Lamport pada sisi koordinator.
    1. Buat session_id unik
    2. Catat session ke DB
    3. Kirim MARKER ke semua worker secara asinkron
    4. Rekam state sendiri
    5. Tunggu ACK dari semua worker (timeout 30 detik)
    6. Update status sesi
    """
    session_id  = str(uuid.uuid4())
    start_time  = time.time()
    total_nodes = len(WORKERS)

    log.info(f"{'='*55}")
    log.info(f"[INIT] Memulai Checkpoint | Session: {session_id[:8]}...")
    log.info(f"[INIT] Trigger: {trigger_type} | Workers: {total_nodes}")
    log.info(f"{'='*55}")

    # Langkah 1: Catat session
    record_session(session_id, trigger_type, total_nodes)

    # Langkah 2: Simpan state koordinator
    save_coordinator_checkpoint(session_id)

    # Langkah 3: Kirim MARKER ke semua worker secara asinkron
    marker_msg = json.dumps({
        'type':       'MARKER',
        'session_id': session_id,
        'from':       'coordinator',
        'timestamp':  datetime.now().isoformat(),
    }).encode()

    ack_results = {}
    channel_state_info = {}  # Informasi channel state dari ACK
    threads = []

    def send_marker_to_worker(name, host, port):
        """Kirim MARKER ke satu worker dan tunggu ACK."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(MARKER_TIMEOUT)
            sock.connect((host, port))
            sock.sendall(marker_msg)
            log.info(f"[MARKER] → {name} ({host}:{port})")

            # Tunggu ACK (worker sekarang menunggu snapshot lengkap
            # termasuk channel state sebelum mengirim ACK)
            resp = sock.recv(65536)
            if resp:
                ack = json.loads(resp.decode())
                if ack.get('type') == 'ACK' and ack.get('session_id') == session_id:
                    ack_results[name] = True
                    # Log informasi channel state dari ACK
                    ch_summary = ack.get('channel_state_summary', {})
                    ch_total = ack.get('total_channel_msgs', 0)
                    channel_state_info[name] = ch_summary
                    log.info(f"[ACK ✓] {name} → sesi {session_id[:8]} | "
                             f"channel_msgs: {ch_total} | "
                             f"channels: {ch_summary}")
                else:
                    ack_results[name] = False
                    log.warning(f"[ACK ✗] {name} → respons tidak valid")
            sock.close()
        except (ConnectionRefusedError, socket.timeout, OSError) as e:
            ack_results[name] = False
            log.error(f"[ERROR] Gagal kirim MARKER ke {name}: {e}")

    # Jalankan pengiriman MARKER secara paralel (non-blocking)
    for name, (host, port) in WORKERS.items():
        t = threading.Thread(target=send_marker_to_worker,
                             args=(name, host, port), daemon=True)
        threads.append(t)
        t.start()

    # Tunggu semua thread selesai
    for t in threads:
        t.join(timeout=MARKER_TIMEOUT + 5)

    # Langkah 4: Evaluasi hasil
    duration_ms  = int((time.time() - start_time) * 1000)
    acked_count  = sum(1 for v in ack_results.values() if v)
    all_acked    = (acked_count == total_nodes)
    status       = 'completed' if all_acked else 'failed'

    update_session(session_id, status, acked_count, duration_ms)

    if all_acked:
        log.info(f"[✓ GLOBAL] Checkpoint BERHASIL & Dicatat ke DB")
        log.info(f"[✓ GLOBAL] ACK: {acked_count}/{total_nodes} | Durasi: {duration_ms}ms")
        # Log ringkasan channel state (Chandy-Lamport)
        if channel_state_info:
            log.info(f"[✓ GLOBAL] Channel state summary:")
            for wname, ch_info in channel_state_info.items():
                total_msgs = sum(ch_info.values()) if ch_info else 0
                log.info(f"    {wname}: {total_msgs} in-transit msg(s) "
                         f"dari {ch_info}")
    else:
        log.warning(f"[✗ GLOBAL] Checkpoint GAGAL (timeout/crash)")
        log.warning(f"[✗ GLOBAL] ACK: {acked_count}/{total_nodes} | Durasi: {duration_ms}ms")

    log.info(f"{'='*55}")
    return session_id, status, acked_count, duration_ms


# ─── Server Koordinator: Terima Koneksi dari Worker ───────
def handle_incoming(conn, addr):
    """Terima pesan masuk (status update / heartbeat dari worker)."""
    try:
        data = conn.recv(4096)
        if data:
            msg = json.loads(data.decode())
            if msg.get('type') == 'HEARTBEAT':
                update_heartbeat(msg.get('node_name', ''))
                conn.sendall(json.dumps({'type': 'HEARTBEAT_ACK'}).encode())
            elif msg.get('type') == 'STATUS':
                log.info(f"[STATUS] dari {addr}: {msg}")
    except Exception as e:
        log.debug(f"Handle incoming error: {e}")
    finally:
        conn.close()


def start_server():
    """Jalankan server TCP untuk menerima koneksi dari worker."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((COORDINATOR_HOST, COORDINATOR_PORT))
    srv.listen(10)
    log.info(f"[SERVER] Coordinator listening di port {COORDINATOR_PORT}")
    while True:
        try:
            conn, addr = srv.accept()
            threading.Thread(target=handle_incoming,
                             args=(conn, addr), daemon=True).start()
        except Exception as e:
            log.error(f"Server error: {e}")


# ─── Heartbeat Coordinator ────────────────────────────────
def heartbeat_loop():
    """Perbarui heartbeat koordinator ke database secara periodik."""
    while True:
        try:
            update_heartbeat('coordinator')
        except Exception as e:
            log.debug(f"Heartbeat error: {e}")
        time.sleep(HEARTBEAT_INTERVAL)


# ─── Checkpoint Periodik ──────────────────────────────────
def checkpoint_loop():
    """Jalankan checkpoint secara periodik."""
    log.info(f"[PERIODIK] Checkpoint setiap {CHECKPOINT_INTERVAL} detik")
    # Tunggu worker siap
    time.sleep(15)
    while True:
        try:
            initiate_checkpoint(trigger_type='periodic')
        except Exception as e:
            log.error(f"Checkpoint loop error: {e}")
        time.sleep(CHECKPOINT_INTERVAL)


# ─── Main ─────────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("  COORDINATOR NODE - Chandy-Lamport Algorithm")
    log.info("=" * 55)

    # Tunggu database siap
    for i in range(10):
        try:
            with get_db() as conn:
                log.info("[DB] Koneksi database berhasil")
                break
        except Exception as e:
            log.warning(f"[DB] Menunggu database... ({i+1}/10): {e}")
            time.sleep(5)

    # Daftarkan koordinator ke DB
    register_node('coordinator', COORDINATOR_HOST, 'coordinator')

    # Buat direktori checkpoint
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    # Jalankan semua thread
    threading.Thread(target=start_server,      daemon=True).start()
    threading.Thread(target=heartbeat_loop,    daemon=True).start()
    threading.Thread(target=checkpoint_loop,   daemon=False).start()


if __name__ == '__main__':
    main()
