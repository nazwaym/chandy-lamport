"""
=============================================================
RECOVERY MANAGER - Algoritma Chandy-Lamport
=============================================================
Fungsi: Mendeteksi kegagalan melalui mekanisme heartbeat dan
memulihkan state dari file checkpoint terakhir yang valid.
Juga menjalankan 3 skenario pengujian kegagalan.
=============================================================
"""

import socket
import threading
import time
import json
import pickle
import os
import logging
import random
import subprocess
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

# ─── Konfigurasi Logging ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] RECOVERY MGR | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('recovery_manager')

# ─── Konfigurasi ──────────────────────────────────────────
CHECKPOINT_DIR     = '/checkpoint_storage'
HEARTBEAT_TIMEOUT  = 15   # detik: node dianggap gagal
CHECK_INTERVAL     = 5    # detik: interval cek heartbeat
COORDINATOR_HOST   = 'coordinator'
COORDINATOR_PORT   = 5000

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
    return psycopg2.connect(**DB_CONFIG)


def get_latest_checkpoint(node_name):
    """Ambil metadata checkpoint terakhir yang valid untuk suatu node."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT c.checkpoint_id, c.file_path, c.checksum,
                       c.sequence_number, c.created_at, c.session_id
                FROM checkpoints c
                JOIN nodes n ON c.node_id = n.node_id
                WHERE n.node_name = %s AND c.status = 'valid'
                ORDER BY c.created_at DESC
                LIMIT 1
            """, (node_name,))
            return cur.fetchone()


def get_failed_nodes():
    """Cari node yang heartbeat-nya sudah kadaluarsa (dianggap gagal)."""
    threshold = datetime.now() - timedelta(seconds=HEARTBEAT_TIMEOUT)
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT node_name, role, last_heartbeat, status
                FROM nodes
                WHERE last_heartbeat < %s AND status = 'active'
            """, (threshold,))
            return cur.fetchall()


def mark_node_failed(node_name):
    """Tandai node sebagai gagal di database."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nodes SET status = 'failed' WHERE node_name = %s",
                (node_name,))
            conn.commit()
    log.warning(f"[FAILED] Node '{node_name}' ditandai sebagai GAGAL")


def mark_node_recovering(node_name):
    """Tandai node sedang dalam proses pemulihan."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nodes SET status = 'recovering' WHERE node_name = %s",
                (node_name,))
            conn.commit()


def mark_node_active(node_name):
    """Tandai node sudah aktif kembali setelah pemulihan."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nodes SET status = 'active', last_heartbeat = NOW() "
                "WHERE node_name = %s",
                (node_name,))
            conn.commit()


def log_recovery(checkpoint_id, session_id, failed_node,
                 trigger_reason, status, recovery_time_ms,
                 data_loss_ms, files_recovered, files_lost):
    """Catat hasil pemulihan ke tabel recovery_logs."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO recovery_logs
                    (checkpoint_id, session_id, failed_node, trigger_reason,
                     status, recovery_time_ms, data_loss_ms,
                     files_recovered, files_lost)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (checkpoint_id, session_id, failed_node, trigger_reason,
                  status, recovery_time_ms, data_loss_ms,
                  files_recovered, files_lost))
            conn.commit()


def update_checkpoint_status(checkpoint_id, status):
    """Update status file checkpoint."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE checkpoints SET status = %s WHERE checkpoint_id = %s",
                (status, checkpoint_id))
            conn.commit()


# ─── Proses Pemulihan ─────────────────────────────────────
def verify_checkpoint_file(filepath, expected_checksum):
    """Verifikasi integritas file checkpoint menggunakan checksum SHA256."""
    import hashlib
    if not os.path.exists(filepath):
        return False, "File tidak ditemukan"
    try:
        with open(filepath, 'rb') as f:
            data = f.read()
        actual = hashlib.sha256(data).hexdigest()
        if actual == expected_checksum:
            return True, "Checksum valid"
        return False, f"Checksum mismatch: {actual[:12]} ≠ {expected_checksum[:12]}"
    except Exception as e:
        return False, str(e)


def restore_from_checkpoint(node_name, ckpt_meta):
    """
    Pulihkan state node dari file checkpoint terakhir.
    Returns: (success, recovery_time_ms, data_loss_ms, files_recovered, files_lost)
    """
    start_time = time.time()
    log.info(f"[RECOVER] Memulihkan {node_name} dari checkpoint "
             f"#{ckpt_meta['checkpoint_id']}")
    log.info(f"[RECOVER] File: {ckpt_meta['file_path']}")

    # Verifikasi file
    valid, msg = verify_checkpoint_file(
        ckpt_meta['file_path'], ckpt_meta['checksum'])

    if not valid:
        log.error(f"[RECOVER] File checkpoint RUSAK: {msg}")
        return False, 0, 0, 0, 1

    # Load state dari file
    try:
        with open(ckpt_meta['file_path'], 'rb') as f:
            state = pickle.load(f)

        # Hitung data loss (waktu antara checkpoint dan kegagalan)
        ckpt_time = ckpt_meta['created_at']
        if isinstance(ckpt_time, str):
            from dateutil import parser
            ckpt_time = parser.parse(ckpt_time)
        data_loss_ms = int((datetime.now() - ckpt_time).total_seconds() * 1000)

        files_recovered = len(state.get('files_processed', []))
        log.info(f"[RECOVER] State dimuat: {files_recovered} file, "
                 f"progress {state.get('task_progress', 0):.1f}%")

        # Update status checkpoint
        update_checkpoint_status(ckpt_meta['checkpoint_id'], 'restored')

        recovery_time_ms = int((time.time() - start_time) * 1000)
        log.info(f"[RECOVER ✓] {node_name} dipulihkan dalam {recovery_time_ms}ms "
                 f"| Data loss: {data_loss_ms}ms")
        return True, recovery_time_ms, data_loss_ms, files_recovered, 0

    except Exception as e:
        log.error(f"[RECOVER ✗] Gagal load checkpoint: {e}")
        return False, 0, 0, 0, 1


def recover_node(node_name, trigger_reason):
    """Orkestrasi pemulihan lengkap untuk satu node."""
    log.info(f"{'─'*50}")
    log.info(f"[RECOVERY START] Node: {node_name} | Alasan: {trigger_reason}")

    mark_node_recovering(node_name)

    ckpt = get_latest_checkpoint(node_name)
    if not ckpt:
        log.error(f"[RECOVERY] Tidak ada checkpoint valid untuk {node_name}!")
        log_recovery(None, None, node_name, trigger_reason,
                     'failed', 0, 0, 0, 1)
        return False

    success, rto_ms, loss_ms, f_ok, f_lost = restore_from_checkpoint(
        node_name, ckpt)

    log_recovery(
        checkpoint_id   = ckpt['checkpoint_id'],
        session_id      = ckpt['session_id'],
        failed_node     = node_name,
        trigger_reason  = trigger_reason,
        status          = 'success' if success else 'failed',
        recovery_time_ms= rto_ms,
        data_loss_ms    = loss_ms,
        files_recovered = f_ok,
        files_lost      = f_lost,
    )

    if success:
        mark_node_active(node_name)
        log.info(f"[RECOVERY DONE] {node_name} aktif kembali ✓")
    else:
        log.error(f"[RECOVERY FAIL] {node_name} gagal dipulihkan ✗")

    log.info(f"{'─'*50}")
    return success


# ─── Monitor Heartbeat ────────────────────────────────────
def monitor_heartbeat():
    """Loop utama: deteksi node gagal via heartbeat dan pulihkan."""
    log.info(f"[MONITOR] Heartbeat monitor aktif (timeout={HEARTBEAT_TIMEOUT}s)")
    while True:
        try:
            failed = get_failed_nodes()
            for node in failed:
                log.warning(f"[DETECT] Node '{node['node_name']}' gagal! "
                            f"Last heartbeat: {node['last_heartbeat']}")
                mark_node_failed(node['node_name'])
                recover_node(node['node_name'], 'heartbeat_timeout')
        except Exception as e:
            log.error(f"Monitor error: {e}")
        time.sleep(CHECK_INTERVAL)


# ─── Skenario Pengujian Kegagalan ─────────────────────────
def trigger_checkpoint(trigger_type='manual'):
    """Minta koordinator menjalankan checkpoint."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((COORDINATOR_HOST, COORDINATOR_PORT))
        msg = json.dumps({'type': 'TRIGGER_CHECKPOINT',
                          'trigger_type': trigger_type})
        sock.sendall(msg.encode())
        sock.close()
        log.info(f"[TRIGGER] Checkpoint {trigger_type} diminta")
        time.sleep(5)  # tunggu checkpoint selesai
    except Exception as e:
        log.error(f"[TRIGGER] Gagal minta checkpoint: {e}")


# ─── Kumpulkan Metrik ─────────────────────────────────────
def collect_metrics():
    """Kumpulkan metrik kinerja dari database untuk laporan Bab 4."""
    log.info("\n" + "="*60)
    log.info("  LAPORAN METRIK KINERJA SISTEM")
    log.info("="*60)

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # 1. Ringkasan sesi checkpoint
            cur.execute("""
                SELECT global_status, COUNT(*) AS jumlah,
                       AVG(duration_ms) AS rata_durasi_ms
                FROM checkpoint_sessions
                GROUP BY global_status
            """)
            rows = cur.fetchall()
            log.info("\n[1] SESI CHECKPOINT:")
            for r in rows:
                log.info(f"    Status: {r['global_status']:12s} | "
                         f"Jumlah: {r['jumlah']:3d} | "
                         f"Rata-rata durasi: {r['rata_durasi_ms']:.1f}ms")

            # 2. Metrik pemulihan per skenario
            cur.execute("""
                SELECT trigger_reason,
                       COUNT(*) AS total_percobaan,
                       COUNT(CASE WHEN status='success' THEN 1 END) AS berhasil,
                       COUNT(CASE WHEN status='failed'  THEN 1 END) AS gagal,
                       ROUND(AVG(recovery_time_ms)::numeric, 2) AS rata_rto_ms,
                       ROUND(AVG(data_loss_ms)::numeric, 2)    AS rata_loss_ms,
                       SUM(files_recovered) AS total_file_ok,
                       SUM(files_lost)      AS total_file_hilang
                FROM recovery_logs
                GROUP BY trigger_reason
                ORDER BY trigger_reason
            """)
            rows = cur.fetchall()
            log.info("\n[2] METRIK PEMULIHAN PER SKENARIO:")
            for r in rows:
                flr = (r['total_file_hilang'] /
                       max(r['total_file_ok'] + r['total_file_hilang'], 1) * 100)
                log.info(f"\n    Skenario : {r['trigger_reason']}")
                log.info(f"    Iterasi  : {r['total_percobaan']}")
                log.info(f"    Berhasil : {r['berhasil']} | Gagal: {r['gagal']}")
                log.info(f"    RTO      : {r['rata_rto_ms']} ms")
                log.info(f"    Data Loss: {r['rata_loss_ms']} ms")
                log.info(f"    FLR      : {flr:.2f}%")
                log.info(f"    File OK  : {r['total_file_ok']} | "
                         f"File Hilang: {r['total_file_hilang']}")

            # 3. Checkpoint overhead
            cur.execute("""
                SELECT
                    COUNT(*) AS total_sesi,
                    SUM(CASE WHEN global_status='completed' THEN 1 ELSE 0 END) AS sukses,
                    AVG(CASE WHEN global_status='completed'
                        THEN duration_ms END) AS avg_overhead_ms,
                    MIN(CASE WHEN global_status='completed'
                        THEN duration_ms END) AS min_ms,
                    MAX(CASE WHEN global_status='completed'
                        THEN duration_ms END) AS max_ms
                FROM checkpoint_sessions
            """)
            r = cur.fetchone()
            if r and r['total_sesi']:
                overhead_pct = (r['avg_overhead_ms'] or 0) / \
                               (30000 + (r['avg_overhead_ms'] or 1)) * 100
                log.info(f"\n[3] CHECKPOINT OVERHEAD:")
                log.info(f"    Total sesi     : {r['total_sesi']}")
                log.info(f"    Sukses         : {r['sukses']}")
                log.info(f"    Avg durasi     : {r['avg_overhead_ms']:.1f} ms")
                log.info(f"    Min durasi     : {r['min_ms']} ms")
                log.info(f"    Max durasi     : {r['max_ms']} ms")
                log.info(f"    Est. overhead  : {overhead_pct:.2f}%")

    log.info("\n" + "="*60)


# ─── Main ─────────────────────────────────────────────────
def main():
    log.info("=" * 55)
    log.info("  RECOVERY MANAGER - Chandy-Lamport System")
    log.info("=" * 55)

    # Tunggu sistem siap
    for i in range(15):
        try:
            with get_db():
                log.info("[DB] Koneksi database berhasil")
                break
        except Exception as e:
            log.warning(f"[DB] Menunggu... ({i+1}/15): {e}")
            time.sleep(5)

    # Jalankan monitor heartbeat di background
    threading.Thread(target=monitor_heartbeat, daemon=True).start()

    log.info("[READY] Recovery Manager siap memantau sistem")

    # Keep alive & laporan berkala
    report_counter = 0
    while True:
        time.sleep(60)
        report_counter += 1
        if report_counter % 5 == 0:  # Setiap 5 menit
            collect_metrics()


if __name__ == '__main__':
    main()
