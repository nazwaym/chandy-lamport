"""
=============================================================
WORKER NODE - Algoritma Chandy-Lamport (Enhanced)
=============================================================
Fungsi: Menerima MARKER dari koordinator DAN peer worker,
menyimpan snapshot state lokal + channel state (in-transit
messages), mempropagasi MARKER ke semua outgoing channel,
dan mengirim ACK kembali ke koordinator.

Perbaikan dari versi sebelumnya:
  - Komunikasi antar-worker (application messages)
  - Channel state recording (in-transit messages)
  - Propagasi MARKER ke peer workers
  - Snapshot = local state + channel state
=============================================================
"""

import socket
import threading
import time
import json
import pickle
import hashlib
import os
import random
import uuid
import logging
from datetime import datetime
import psycopg2

# ─── Konfigurasi Logging ──────────────────────────────────
NODE_NAME = os.getenv('NODE_NAME', 'worker1')
NODE_PORT = int(os.getenv('NODE_PORT', 6001))

logging.basicConfig(
    level=logging.INFO,
    format=f'[%(asctime)s] {NODE_NAME.upper()} | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger(NODE_NAME)

# ─── Konfigurasi ──────────────────────────────────────────
COORDINATOR_HOST  = os.getenv('COORDINATOR_HOST', 'coordinator')
COORDINATOR_PORT  = int(os.getenv('COORDINATOR_PORT', 5000))
CHECKPOINT_DIR    = '/checkpoint_storage'
HEARTBEAT_INTERVAL = 5
TASK_INTERVAL      = 2   # simulasi task setiap 2 detik
APP_MSG_INTERVAL   = 3   # kirim application message setiap 3 detik

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'db'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'checkpoint_db'),
    'user':     os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASS', 'admin123'),
}

# ─── Peer Workers (semua worker kecuali diri sendiri) ─────
ALL_WORKERS = {
    'worker1': ('worker1', 6001),
    'worker2': ('worker2', 6002),
    'worker3': ('worker3', 6003),
}
PEERS = {k: v for k, v in ALL_WORKERS.items() if k != NODE_NAME}

# Semua incoming channel (coordinator + peer workers)
ALL_INCOMING_CHANNELS = frozenset({'coordinator'} | set(PEERS.keys()))

# ─── State Lokal Worker ──────────────────────────────────
local_state = {
    'node_name':       NODE_NAME,
    'task_count':      0,
    'task_progress':   0.0,
    'files_processed': [],
    'last_task':       None,
    'status':          'running',
}
state_lock = threading.Lock()

# ─── Application Message Sequence Counter ─────────────────
app_msg_seq = 0
app_msg_seq_lock = threading.Lock()

# ─── Chandy-Lamport Snapshot State (per sesi) ─────────────
# session_id → {
#   'first_marker_from': str,
#   'local_state': dict,
#   'channel_recording': {channel: bool},
#   'channel_state': {channel: [messages]},
#   'markers_received': set,
#   'complete_event': threading.Event,
#   'filepath': str,
#   'checksum': str,
# }
snapshot_sessions = {}
snapshot_lock = threading.Lock()

# Legacy set untuk idempotency
checkpointed_sessions = set()


# ─── Helper Database ──────────────────────────────────────
def get_db():
    return psycopg2.connect(**DB_CONFIG)


def get_node_id():
    """Ambil node_id dari database."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT node_id FROM nodes WHERE node_name = %s", (NODE_NAME,))
            row = cur.fetchone()
            return row[0] if row else None


def register_node():
    """Daftarkan worker ke tabel nodes."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO nodes (node_name, ip_address, role, status, last_heartbeat)
                VALUES (%s, %s, 'worker', 'active', NOW())
                ON CONFLICT (node_name) DO UPDATE
                SET status = 'active', last_heartbeat = NOW()
            """, (NODE_NAME, f'{NODE_NAME}:{NODE_PORT}'))
            conn.commit()
    log.info(f"[REGISTER] {NODE_NAME} terdaftar di database")


def update_heartbeat():
    """Update heartbeat ke database."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nodes SET last_heartbeat = NOW() WHERE node_name = %s",
                (NODE_NAME,))
            conn.commit()


# ─── Chandy-Lamport: Channel State Recording ─────────────
def record_incoming_app_message(from_node, message):
    """
    Catat application message masuk jika sedang dalam mode
    recording untuk channel tersebut (sesuai Chandy-Lamport).
    """
    with snapshot_lock:
        for sid, ss in snapshot_sessions.items():
            if ss['channel_recording'].get(from_node, False):
                ss['channel_state'][from_node].append(message)
                log.info(
                    f"[CL-RECORD] Pesan dari {from_node} dicatat "
                    f"(sesi {sid[:8]}, total: "
                    f"{len(ss['channel_state'][from_node])})")


def forward_marker_to_peers(session_id):
    """
    Propagasi MARKER ke semua peer worker (outgoing channels).
    Sesuai algoritma Chandy-Lamport: setelah merekam local state,
    process harus mengirim MARKER ke semua outgoing channel.
    """
    marker_msg = json.dumps({
        'type':       'MARKER',
        'session_id': session_id,
        'from':       NODE_NAME,
        'timestamp':  datetime.now().isoformat(),
    }).encode()

    for peer_name, (host, port) in PEERS.items():
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            sock.sendall(marker_msg)
            log.info(f"[CL-MARKER →] Propagasi MARKER ke {peer_name}")
            sock.close()
        except Exception as e:
            log.warning(f"[CL-MARKER ✗] Gagal propagasi ke {peer_name}: {e}")


def check_snapshot_complete(session_id):
    """Cek apakah semua MARKER sudah diterima dari semua channel."""
    ss = snapshot_sessions.get(session_id)
    if not ss:
        return False
    return ss['markers_received'] == ALL_INCOMING_CHANNELS


# ─── Chandy-Lamport: Proses MARKER ───────────────────────
def process_marker(session_id, from_node):
    """
    Inti algoritma Chandy-Lamport pada sisi non-inisiator.

    Kasus 1 — MARKER pertama (dari channel manapun):
      1. Rekam local state
      2. Set channel state dari pengirim MARKER = KOSONG
      3. Mulai recording di semua channel lain
      4. Propagasi MARKER ke semua outgoing channel

    Kasus 2 — MARKER berikutnya (dari channel lain):
      1. Stop recording dari channel tersebut
      2. Simpan pesan yang tercatat sebagai channel state
      3. Cek apakah snapshot sudah lengkap
    """
    should_forward = False
    is_complete = False

    with snapshot_lock:
        if session_id not in snapshot_sessions:
            # ═══ KASUS 1: MARKER PERTAMA ═══
            log.info(f"[CL] ═══ MARKER PERTAMA dari {from_node} | "
                     f"Sesi: {session_id[:8]}")

            ss = {
                'first_marker_from': from_node,
                'local_state':       None,
                'channel_recording': {},
                'channel_state':     {},
                'markers_received':  set(),
                'complete_event':    threading.Event(),
                'filepath':          None,
                'checksum':          None,
                'created_at':        datetime.now().isoformat(),
            }
            snapshot_sessions[session_id] = ss

            # Langkah 1: Rekam local state
            with state_lock:
                ss['local_state'] = {
                    'node_name':       local_state['node_name'],
                    'session_id':      session_id,
                    'task_count':      local_state['task_count'],
                    'task_progress':   local_state['task_progress'],
                    'files_processed': local_state['files_processed'].copy(),
                    'last_task':       local_state['last_task'],
                    'timestamp':       datetime.now().isoformat(),
                    'status':          'snapshotted',
                }
            log.info(f"[CL] Local state direkam ✓")

            # Langkah 2: Channel pengirim MARKER = kosong (tidak ada
            # pesan in-transit karena MARKER tiba langsung)
            ss['markers_received'].add(from_node)
            ss['channel_state'][from_node] = []
            ss['channel_recording'][from_node] = False

            # Langkah 3: Mulai recording di semua channel lain
            for ch in ALL_INCOMING_CHANNELS:
                if ch != from_node:
                    ss['channel_recording'][ch] = True
                    ss['channel_state'][ch] = []

            recording_channels = [
                ch for ch, r in ss['channel_recording'].items() if r]
            log.info(f"[CL] Recording dimulai pada channel: {recording_channels}")

            should_forward = True
            is_complete = check_snapshot_complete(session_id)

        else:
            # ═══ KASUS 2: MARKER BERIKUTNYA ═══
            ss = snapshot_sessions[session_id]

            if from_node in ss['markers_received']:
                log.info(f"[CL] Duplikat MARKER dari {from_node}, diabaikan")
                return

            log.info(f"[CL] MARKER berikutnya dari {from_node} | "
                     f"Sesi: {session_id[:8]}")

            # Stop recording di channel ini
            ss['markers_received'].add(from_node)
            ss['channel_recording'][from_node] = False

            captured = len(ss['channel_state'].get(from_node, []))
            log.info(f"[CL] Recording STOP di channel {from_node} | "
                     f"Pesan tercatat: {captured}")

            is_complete = check_snapshot_complete(session_id)

    # Propagasi MARKER ke peer (di luar lock)
    if should_forward:
        forward_marker_to_peers(session_id)
        checkpointed_sessions.add(session_id)

    # Jika snapshot lengkap, simpan dan signal
    if is_complete:
        finalize_snapshot(session_id)


def finalize_snapshot(session_id):
    """
    Finalisasi snapshot: simpan ke file .ckpt dan database,
    lalu signal complete_event.
    """
    with snapshot_lock:
        ss = snapshot_sessions.get(session_id)
        if not ss:
            return

    log.info(f"[CL] ★ Snapshot LENGKAP untuk sesi {session_id[:8]}")

    # Hitung total in-transit messages
    total_channel_msgs = sum(
        len(msgs) for msgs in ss['channel_state'].values())
    log.info(f"[CL] Total in-transit messages tercatat: {total_channel_msgs}")

    # Buat full snapshot (local state + channel state)
    full_snapshot = {
        'node_name':          NODE_NAME,
        'session_id':         session_id,
        'local_state':        ss['local_state'],
        'channel_state':      ss['channel_state'],
        'first_marker_from':  ss['first_marker_from'],
        'markers_received':   list(ss['markers_received']),
        'total_channel_msgs': total_channel_msgs,
        'timestamp':          datetime.now().isoformat(),
    }

    # Serialisasi ke file
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    filename = f'{NODE_NAME}_{session_id[:8]}_{int(time.time())}.ckpt'
    filepath = os.path.join(CHECKPOINT_DIR, filename)
    data     = pickle.dumps(full_snapshot)
    checksum = hashlib.sha256(data).hexdigest()

    with open(filepath, 'wb') as f:
        f.write(data)

    ss['filepath'] = filepath
    ss['checksum'] = checksum

    log.info(f"[SNAPSHOT ✓] {filename} | checksum: {checksum[:12]}... | "
             f"channel_msgs: {total_channel_msgs}")

    # Simpan ke database
    node_id = get_node_id()
    if node_id:
        try:
            with get_db() as conn:
                with conn.cursor() as cur:
                    # Insert ke tabel checkpoints
                    cur.execute("""
                        INSERT INTO checkpoints
                            (node_id, session_id, sequence_number, file_path,
                             checksum, file_size_bytes, status)
                        VALUES (%s, %s,
                            (SELECT COALESCE(MAX(sequence_number),0)+1
                             FROM checkpoints WHERE node_id=%s),
                            %s, %s, %s, 'valid')
                        RETURNING checkpoint_id
                    """, (node_id, session_id, node_id,
                          filepath, checksum, len(data)))
                    ckpt_id = cur.fetchone()[0]

                    # Insert ke tabel task_states
                    cur.execute("""
                        INSERT INTO task_states
                            (node_id, checkpoint_id, task_name,
                             state_data, progress_pct)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        node_id, ckpt_id,
                        ss['local_state'].get('last_task') or 'idle',
                        json.dumps(ss['local_state']),
                        ss['local_state'].get('task_progress', 0)
                    ))

                    # Insert ke tabel channel_states
                    for ch_name, ch_msgs in ss['channel_state'].items():
                        cur.execute("""
                            INSERT INTO channel_states
                                (session_id, from_node, to_node,
                                 messages, message_count)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            session_id, ch_name, NODE_NAME,
                            json.dumps(ch_msgs), len(ch_msgs)
                        ))

                    conn.commit()
        except Exception as e:
            log.error(f"[DB ERROR] Gagal simpan snapshot: {e}")

    # Signal bahwa snapshot sudah lengkap
    ss['complete_event'].set()


# ─── Handler: MARKER dari Coordinator ─────────────────────
def handle_marker_from_coordinator(session_id, conn):
    """
    Terima MARKER dari coordinator.
    Proses sesuai Chandy-Lamport, tunggu snapshot lengkap,
    lalu kirim ACK kembali ke coordinator.
    """
    if session_id in snapshot_sessions and \
       snapshot_sessions[session_id]['complete_event'].is_set():
        log.info(f"[MARKER] Sesi {session_id[:8]} sudah selesai, kirim ACK")
        ss = snapshot_sessions[session_id]
        ack = json.dumps({
            'type':       'ACK',
            'session_id': session_id,
            'node':       NODE_NAME,
            'status':     'already_done',
            'checkpoint': ss.get('filepath'),
            'checksum':   ss.get('checksum'),
        })
        conn.sendall(ack.encode())
        return

    log.info(f"[MARKER ←] Diterima dari coordinator | Sesi: {session_id[:8]}")

    # Proses MARKER sesuai Chandy-Lamport
    process_marker(session_id, 'coordinator')

    # Tunggu snapshot lengkap (semua channel MARKER diterima)
    ss = snapshot_sessions.get(session_id)
    if ss:
        log.info(f"[CL] Menunggu MARKER dari semua channel...")
        completed = ss['complete_event'].wait(timeout=25)

        if completed:
            log.info(f"[CL] Snapshot lengkap, mengirim ACK ke coordinator")
            total_ch_msgs = sum(
                len(m) for m in ss['channel_state'].values())
            ack_msg = json.dumps({
                'type':               'ACK',
                'session_id':         session_id,
                'node':               NODE_NAME,
                'checkpoint':         ss.get('filepath'),
                'checksum':           ss.get('checksum'),
                'channel_state_summary': {
                    ch: len(msgs)
                    for ch, msgs in ss['channel_state'].items()
                },
                'total_channel_msgs': total_ch_msgs,
                'timestamp':          datetime.now().isoformat(),
            })
        else:
            log.warning(f"[CL] Timeout menunggu MARKER dari semua channel!")
            ack_msg = json.dumps({
                'type':       'ACK',
                'session_id': session_id,
                'node':       NODE_NAME,
                'status':     'partial',
                'checkpoint': ss.get('filepath'),
                'timestamp':  datetime.now().isoformat(),
            })

        conn.sendall(ack_msg.encode())
        log.info(f"[ACK →] Dikirim ke coordinator | Sesi: {session_id[:8]}")


# ─── Handler: MARKER dari Peer Worker ─────────────────────
def handle_marker_from_peer(session_id, from_node):
    """
    Terima MARKER dari peer worker.
    Proses sesuai Chandy-Lamport (stop recording di channel ini).
    Tidak perlu mengirim response — peer hanya fire-and-forget.
    """
    log.info(f"[MARKER ←] Diterima dari peer {from_node} | "
             f"Sesi: {session_id[:8]}")
    process_marker(session_id, from_node)


# ─── Handler: Application Message dari Peer ───────────────
def handle_app_message(msg):
    """
    Terima application message dari peer worker.
    Jika sedang dalam mode recording, catat pesan ini
    sebagai bagian dari channel state.
    """
    from_node = msg.get('from', 'unknown')
    log.debug(f"[APP_MSG ←] Dari {from_node}: {msg.get('data', {}).get('file', '?')}")

    # Catat jika sedang recording (Chandy-Lamport)
    record_incoming_app_message(from_node, msg)


# ─── Server Worker: Terima Koneksi ────────────────────────
def handle_connection(conn, addr):
    """Terima dan proses pesan masuk dari koordinator atau peer worker."""
    try:
        data = conn.recv(65536)
        if not data:
            return
        msg = json.loads(data.decode())
        msg_type = msg.get('type', '')

        if msg_type == 'MARKER':
            from_node = msg.get('from', 'unknown')
            session_id = msg['session_id']

            if from_node == 'coordinator' or from_node == 'test_runner':
                # MARKER dari coordinator → proses + kirim ACK
                handle_marker_from_coordinator(session_id, conn)
            else:
                # MARKER dari peer worker → proses saja
                handle_marker_from_peer(session_id, from_node)

        elif msg_type == 'APP_MSG':
            # Application message dari peer worker
            handle_app_message(msg)

        elif msg_type == 'RECOVER':
            log.info(f"[RECOVER] Menerima perintah pemulihan dari {addr}")
            conn.sendall(json.dumps({'type': 'RECOVER_ACK',
                                     'node': NODE_NAME}).encode())
        else:
            log.debug(f"Pesan tidak dikenal dari {addr}: {msg_type}")

    except json.JSONDecodeError as e:
        log.error(f"JSON decode error dari {addr}: {e}")
    except Exception as e:
        log.error(f"Connection handler error: {e}")
    finally:
        conn.close()


def start_server():
    """Jalankan server TCP worker untuk menerima MARKER dan app messages."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('0.0.0.0', NODE_PORT))
    srv.listen(20)
    log.info(f"[SERVER] {NODE_NAME} listening di port {NODE_PORT}")
    while True:
        try:
            conn, addr = srv.accept()
            threading.Thread(target=handle_connection,
                             args=(conn, addr), daemon=True).start()
        except Exception as e:
            log.error(f"Server error: {e}")


# ─── Simulasi Task/Pekerjaan Worker ───────────────────────
SIMULATED_FILES = [
    'data_batch_001.csv', 'data_batch_002.csv', 'data_batch_003.csv',
    'report_q1.xlsx',    'report_q2.xlsx',     'report_q3.xlsx',
    'image_001.png',     'image_002.png',       'model_weights.pkl',
    'config_v1.json',    'config_v2.json',      'backup_2024.tar.gz',
]


def simulate_work():
    """Simulasi pekerjaan worker: proses file secara bertahap."""
    log.info(f"[TASK] Memulai simulasi pekerjaan {NODE_NAME}")
    file_pool = SIMULATED_FILES.copy()
    random.shuffle(file_pool)

    while True:
        try:
            if not file_pool:
                file_pool = SIMULATED_FILES.copy()
                random.shuffle(file_pool)

            current_file = file_pool.pop(0)

            with state_lock:
                local_state['task_count']    += 1
                local_state['task_progress']  = round(
                    (local_state['task_count'] % 100) / 100 * 100, 2)
                local_state['last_task']      = current_file

                # Simpan maks 20 file terakhir
                local_state['files_processed'].append(current_file)
                if len(local_state['files_processed']) > 20:
                    local_state['files_processed'].pop(0)

            log.debug(f"[TASK] Processing: {current_file} "
                      f"(total: {local_state['task_count']})")
            time.sleep(TASK_INTERVAL + random.uniform(0, 1))

        except Exception as e:
            log.error(f"Task simulation error: {e}")
            time.sleep(1)


# ─── Application Messages antar-Worker ────────────────────
def send_app_messages_loop():
    """
    Kirim application messages ke peer workers secara periodik.
    Ini mensimulasikan komunikasi nyata antar-proses dalam
    sistem terdistribusi (transfer data, delegasi task, dsb).
    """
    global app_msg_seq
    log.info(f"[APP_MSG] Memulai pengiriman application messages ke peers")

    # Tunggu peer siap
    time.sleep(10)

    while True:
        try:
            # Pilih peer secara acak
            peer_name = random.choice(list(PEERS.keys()))
            host, port = PEERS[peer_name]

            with app_msg_seq_lock:
                app_msg_seq += 1
                seq = app_msg_seq

            # Buat application message berisi data task simulasi
            with state_lock:
                current_file = local_state.get('last_task', 'idle')
                progress = local_state.get('task_progress', 0)

            app_msg = json.dumps({
                'type': 'APP_MSG',
                'from': NODE_NAME,
                'to':   peer_name,
                'msg_id': str(uuid.uuid4())[:8],
                'seq':  seq,
                'data': {
                    'action':   'file_transfer',
                    'file':     current_file,
                    'progress': progress,
                    'result':   {
                        'rows_processed': random.randint(100, 5000),
                        'status': 'completed',
                    },
                },
                'timestamp': datetime.now().isoformat(),
            }).encode()

            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5)
                sock.connect((host, port))
                sock.sendall(app_msg)
                sock.close()
                log.debug(f"[APP_MSG →] Seq#{seq} ke {peer_name}: {current_file}")
            except Exception:
                pass  # peer mungkin belum ready

        except Exception as e:
            log.debug(f"App message error: {e}")

        time.sleep(APP_MSG_INTERVAL + random.uniform(0, 1))


# ─── Heartbeat ke Koordinator ─────────────────────────────
def heartbeat_loop():
    """Kirim heartbeat ke koordinator dan update DB secara periodik."""
    while True:
        try:
            # Update heartbeat di DB
            update_heartbeat()

            # Kirim ke koordinator juga
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                sock.connect((COORDINATOR_HOST, COORDINATOR_PORT))
                hb_msg = json.dumps({
                    'type':      'HEARTBEAT',
                    'node_name': NODE_NAME,
                    'timestamp': datetime.now().isoformat(),
                })
                sock.sendall(hb_msg.encode())
                sock.close()
            except Exception:
                pass  # koordinator mungkin belum ready

        except Exception as e:
            log.debug(f"Heartbeat error: {e}")
        time.sleep(HEARTBEAT_INTERVAL)


# ─── Main ─────────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info(f"  WORKER NODE: {NODE_NAME} (Port: {NODE_PORT})")
    log.info(f"  Peers: {list(PEERS.keys())}")
    log.info(f"  Incoming channels: {list(ALL_INCOMING_CHANNELS)}")
    log.info("=" * 55)

    # Tunggu database siap
    for i in range(15):
        try:
            with get_db():
                log.info("[DB] Koneksi database berhasil")
                break
        except Exception as e:
            log.warning(f"[DB] Menunggu database... ({i+1}/15): {e}")
            time.sleep(5)

    register_node()

    # Jalankan semua thread
    threading.Thread(target=start_server,            daemon=True).start()
    threading.Thread(target=heartbeat_loop,          daemon=True).start()
    threading.Thread(target=simulate_work,           daemon=True).start()
    threading.Thread(target=send_app_messages_loop,  daemon=True).start()

    # Keep alive
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
