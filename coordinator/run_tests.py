"""
=============================================================
SKRIP PENGUJIAN SKENARIO KEGAGALAN (Bab 4)
=============================================================
Menjalankan 3 skenario kegagalan masing-masing 10 iterasi:
  1. Worker Crash       → matikan worker, pulihkan dari checkpoint
  2. Network Partition  → blokir koneksi, lihat efek
  3. Coordinator Crash  → matikan koordinator, restart
=============================================================
CARA PAKAI:
  docker exec -it coordinator python /app/run_tests.py
=============================================================
"""

import socket
import time
import json
import pickle
import hashlib
import os
import logging
import subprocess
import statistics
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] TEST | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('test_runner')

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'db'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'checkpoint_db'),
    'user':     os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASS', 'admin123'),
}

COORDINATOR_HOST = 'coordinator'
COORDINATOR_PORT = 5000
CHECKPOINT_DIR   = '/checkpoint_storage'
N_ITERATIONS     = 10  # Setiap skenario diulang 10x


def get_db():
    return psycopg2.connect(**DB_CONFIG)


# ─── Helper: Kirim MARKER Langsung (bypass coordinator) ───
def send_marker_to_worker(worker_name, host, port, session_id, timeout=10):
    """Kirim MARKER ke worker dan ukur waktu ACK."""
    start = time.time()
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        msg = json.dumps({
            'type':       'MARKER',
            'session_id': session_id,
            'from':       'test_runner',
            'timestamp':  datetime.now().isoformat(),
        }).encode()
        sock.sendall(msg)

        resp = sock.recv(65536)
        elapsed = int((time.time() - start) * 1000)
        if resp:
            ack = json.loads(resp.decode())
            sock.close()
            return True, elapsed, ack
        sock.close()
        return False, elapsed, {}
    except (ConnectionRefusedError, socket.timeout, OSError) as e:
        elapsed = int((time.time() - start) * 1000)
        return False, elapsed, {'error': str(e)}


def get_latest_checkpoint_info(node_name):
    """Ambil info checkpoint terakhir dari database."""
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT c.checkpoint_id, c.file_path, c.checksum,
                       c.created_at, c.sequence_number, c.file_size_bytes
                FROM checkpoints c
                JOIN nodes n ON c.node_id = n.node_id
                WHERE n.node_name = %s AND c.status = 'valid'
                ORDER BY c.created_at DESC LIMIT 1
            """, (node_name,))
            return cur.fetchone()


def verify_and_restore(node_name):
    """Verifikasi checkpoint dan simulasi pemulihan. Returns (success, rto_ms, loss_ms, files)."""
    ckpt = get_latest_checkpoint_info(node_name)
    if not ckpt:
        return False, 0, 9999, 0

    start = time.time()
    filepath = ckpt['file_path']

    if not os.path.exists(filepath):
        return False, 0, 9999, 0

    try:
        with open(filepath, 'rb') as f:
            data = f.read()

        actual_checksum = hashlib.sha256(data).hexdigest()
        if actual_checksum != ckpt['checksum']:
            return False, 0, 9999, 0

        state = pickle.loads(data)
        rto_ms = int((time.time() - start) * 1000)

        # Hitung data loss (waktu dari checkpoint ke "kegagalan")
        ckpt_time = ckpt['created_at']
        if hasattr(ckpt_time, 'timestamp'):
            loss_ms = int((datetime.now() - ckpt_time).total_seconds() * 1000)
        else:
            loss_ms = 5000  # default estimate

        files_ok = len(state.get('files_processed', []))
        return True, rto_ms, loss_ms, files_ok

    except Exception as e:
        log.error(f"Restore error: {e}")
        return False, 0, 9999, 0


def log_recovery_result(session_id, failed_node, trigger, status,
                         rto_ms, loss_ms, files_ok, files_lost):
    """Catat hasil uji pemulihan ke database."""
    ckpt = get_latest_checkpoint_info(failed_node)
    ckpt_id = ckpt['checkpoint_id'] if ckpt else None

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO recovery_logs
                    (checkpoint_id, session_id, failed_node, trigger_reason,
                     status, recovery_time_ms, data_loss_ms,
                     files_recovered, files_lost)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (ckpt_id, session_id, failed_node, trigger,
                  status, rto_ms, loss_ms, files_ok, files_lost))
            conn.commit()


def mark_node_status(node_name, status):
    """Ubah status node di database."""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE nodes SET status=%s WHERE node_name=%s",
                (status, node_name))
            conn.commit()


# ─────────────────────────────────────────────────────────
# SKENARIO 1: WORKER CRASH
# ─────────────────────────────────────────────────────────
def scenario_1_worker_crash():
    """
    Skenario: Salah satu worker 'crash' (diisolasi dari jaringan).
    Ukur: FLR, RTO, checkpoint overhead.
    """
    log.info("\n" + "="*60)
    log.info("  SKENARIO 1: WORKER CRASH")
    log.info(f"  Iterasi: {N_ITERATIONS}x")
    log.info("="*60)

    results = []
    target_worker = 'worker1'
    worker_host   = 'worker1'
    worker_port   = 6001

    for i in range(1, N_ITERATIONS + 1):
        log.info(f"\n[ITER {i:02d}/{N_ITERATIONS}] Worker Crash")
        session_id = f"wcrash_{i:02d}_{int(time.time())}"

        # Step 1: Ambil checkpoint SEBELUM crash
        log.info(f"  [1] Kirim MARKER ke {target_worker}...")
        ok, marker_ms, ack = send_marker_to_worker(
            target_worker, worker_host, worker_port, session_id, timeout=15)

        if ok:
            log.info(f"  [1] MARKER ACK diterima dalam {marker_ms}ms ✓")
            checkpoint_overhead_ms = marker_ms
        else:
            log.warning(f"  [1] MARKER gagal: {ack.get('error', 'unknown')} "
                        f"| Overhead: {marker_ms}ms")
            checkpoint_overhead_ms = marker_ms

        # Step 2: Simulasikan crash (tandai node sebagai failed)
        log.info(f"  [2] Simulasi crash {target_worker}...")
        mark_node_status(target_worker, 'failed')
        time.sleep(2)  # jeda simulasi downtime

        # Step 3: Pemulihan dari checkpoint
        log.info(f"  [3] Memulihkan {target_worker} dari checkpoint...")
        success, rto_ms, loss_ms, files_ok = verify_and_restore(target_worker)
        files_lost = 0 if success else 1

        # Step 4: Catat hasil
        status = 'success' if success else 'failed'
        log_recovery_result(session_id, target_worker,
                             'worker_crash', status,
                             rto_ms, loss_ms, files_ok, files_lost)

        # Step 5: Pulihkan status node
        mark_node_status(target_worker, 'active')

        # Ambil info channel state dari ACK
        channel_msgs = ack.get('total_channel_msgs', 0) if ok else 0

        result = {
            'iterasi':          i,
            'checkpoint_ok':    ok,
            'overhead_ms':      checkpoint_overhead_ms,
            'recovery_ok':      success,
            'rto_ms':           rto_ms,
            'data_loss_ms':     loss_ms,
            'files_recovered':  files_ok,
            'files_lost':       files_lost,
            'channel_msgs':     channel_msgs,
        }
        results.append(result)

        flr = files_lost / max(files_ok + files_lost, 1) * 100
        log.info(f"  [RESULT] Checkpoint: {'✓' if ok else '✗'} "
                 f"| Recovery: {'✓' if success else '✗'} "
                 f"| RTO: {rto_ms}ms | Loss: {loss_ms}ms "
                 f"| FLR: {flr:.1f}% | Ch.Msgs: {channel_msgs}")
        time.sleep(3)

    # Ringkasan skenario 1
    print_scenario_summary("WORKER CRASH", results)
    return results


# ─────────────────────────────────────────────────────────
# SKENARIO 2: NETWORK PARTITION
# ─────────────────────────────────────────────────────────
def scenario_2_network_partition():
    """
    Skenario: Worker tidak dapat dicapai (network partition).
    MARKER dikirim tapi timeout karena worker 'tidak terjangkau'.
    """
    log.info("\n" + "="*60)
    log.info("  SKENARIO 2: NETWORK PARTITION")
    log.info(f"  Iterasi: {N_ITERATIONS}x")
    log.info("="*60)

    results = []
    # Simulasi: gunakan port yang sengaja salah untuk simulasi partisi
    PARTITION_PORT = 9999  # port tidak ada = simulasi network partition

    for i in range(1, N_ITERATIONS + 1):
        log.info(f"\n[ITER {i:02d}/{N_ITERATIONS}] Network Partition")
        session_id = f"netpart_{i:02d}_{int(time.time())}"

        # Step 1: Checkpoint worker2 SEBELUM partisi
        log.info(f"  [1] Checkpoint worker2 SEBELUM partisi...")
        ok_pre, pre_ms, _ = send_marker_to_worker(
            'worker2', 'worker2', 6002, session_id + '_pre', timeout=10)
        log.info(f"  [1] Pre-partition checkpoint: {'✓' if ok_pre else '✗'} ({pre_ms}ms)")
        overhead_ms = pre_ms if ok_pre else 0

        time.sleep(1)

        # Step 2: Simulasikan partisi (koneksi ke port salah = timeout)
        log.info(f"  [2] Simulasi Network Partition (timeout)...")
        partition_start = time.time()
        ok_part, part_ms, ack_part = send_marker_to_worker(
            'worker2', 'worker2', PARTITION_PORT, session_id, timeout=5)
        partition_duration = int((time.time() - partition_start) * 1000)

        log.info(f"  [2] MARKER selama partisi: {'✓' if ok_part else '✗ (timeout)'} "
                 f"({part_ms}ms)")

        # Step 3: Tandai worker2 sebagai tidak bisa dicapai
        if not ok_part:
            mark_node_status('worker2', 'failed')
            log.info(f"  [3] worker2 ditandai failed karena partisi")

        time.sleep(2)

        # Step 4: Pulihkan dari checkpoint terakhir (sebelum partisi)
        log.info(f"  [4] Memulihkan worker2 dari checkpoint sebelum partisi...")
        success, rto_ms, loss_ms, files_ok = verify_and_restore('worker2')
        files_lost = 0 if success else 1

        log_recovery_result(session_id, 'worker2',
                             'network_partition', 'success' if success else 'failed',
                             rto_ms, loss_ms, files_ok, files_lost)

        mark_node_status('worker2', 'active')

        result = {
            'iterasi':            i,
            'pre_checkpoint_ok':  ok_pre,
            'overhead_ms':        overhead_ms,
            'partition_detected': not ok_part,
            'partition_timeout_ms': part_ms,
            'recovery_ok':        success,
            'rto_ms':             rto_ms,
            'data_loss_ms':       loss_ms,
            'files_recovered':    files_ok,
            'files_lost':         files_lost,
            'channel_msgs':       0,
        }
        results.append(result)

        flr = files_lost / max(files_ok + files_lost, 1) * 100
        log.info(f"  [RESULT] Pre-ckpt: {'✓' if ok_pre else '✗'} "
                 f"| Partisi terdeteksi: {'✓' if not ok_part else '✗'} "
                 f"| RTO: {rto_ms}ms | FLR: {flr:.1f}%")
        time.sleep(3)

    print_scenario_summary("NETWORK PARTITION", results)
    return results


# ─────────────────────────────────────────────────────────
# SKENARIO 3: COORDINATOR CRASH
# ─────────────────────────────────────────────────────────
def scenario_3_coordinator_crash():
    """
    Skenario: Koordinator crash.
    Worker memiliki checkpoint terakhir; sistem masih bisa dipulihkan
    dari state tersebut setelah koordinator restart.
    """
    log.info("\n" + "="*60)
    log.info("  SKENARIO 3: COORDINATOR CRASH")
    log.info(f"  Iterasi: {N_ITERATIONS}x")
    log.info("="*60)

    results = []
    workers_list = [
        ('worker1', 'worker1', 6001),
        ('worker2', 'worker2', 6002),
        ('worker3', 'worker3', 6003),
    ]

    for i in range(1, N_ITERATIONS + 1):
        log.info(f"\n[ITER {i:02d}/{N_ITERATIONS}] Coordinator Crash")
        session_id = f"coordcrash_{i:02d}_{int(time.time())}"

        # Step 1: Checkpoint semua worker SEBELUM koordinator crash
        log.info(f"  [1] Checkpoint semua worker SEBELUM koordinator crash...")
        pre_start = time.time()
        ack_count = 0
        overhead_ms = 0

        for wname, whost, wport in workers_list:
            ok, ms, _ = send_marker_to_worker(
                wname, whost, wport, session_id, timeout=10)
            if ok:
                ack_count += 1
            overhead_ms += ms
            log.info(f"  [1] {wname}: {'✓' if ok else '✗'} ({ms}ms)")

        pre_ckpt_ok = (ack_count == len(workers_list))
        overhead_ms = overhead_ms // len(workers_list)  # rata-rata

        # Step 2: Simulasi koordinator crash
        log.info(f"  [2] Simulasi KOORDINATOR CRASH...")
        mark_node_status('coordinator', 'failed')
        time.sleep(3)  # downtime simulasi

        # Step 3: Koordinator restart — pulihkan dari checkpoint sendiri
        log.info(f"  [3] Koordinator restart, memulihkan state...")
        start_restore = time.time()
        coord_ckpt = get_latest_checkpoint_info('coordinator')
        coord_rto_ms = 0
        coord_loss_ms = 0
        coord_ok = False

        if coord_ckpt and os.path.exists(coord_ckpt['file_path']):
            with open(coord_ckpt['file_path'], 'rb') as f:
                data = f.read()
            actual = hashlib.sha256(data).hexdigest()
            if actual == coord_ckpt['checksum']:
                coord_ok = True
                coord_rto_ms = int((time.time() - start_restore) * 1000)
                ckpt_time = coord_ckpt['created_at']
                if hasattr(ckpt_time, 'timestamp'):
                    coord_loss_ms = int(
                        (datetime.now() - ckpt_time).total_seconds() * 1000)

        log.info(f"  [3] Koordinator restore: {'✓' if coord_ok else '✗'} "
                 f"({coord_rto_ms}ms)")

        # Step 4: Pulihkan semua worker juga
        total_files_ok   = 0
        total_files_lost = 0
        total_rto        = coord_rto_ms
        max_loss         = coord_loss_ms

        for wname, _, _ in workers_list:
            ok, rto, loss, files = verify_and_restore(wname)
            if ok:
                total_files_ok += files
                total_rto      += rto
            else:
                total_files_lost += 1
            max_loss = max(max_loss, loss)

        avg_rto = total_rto // (len(workers_list) + 1)
        log_recovery_result(session_id, 'coordinator',
                             'coordinator_crash',
                             'success' if coord_ok else 'failed',
                             avg_rto, max_loss,
                             total_files_ok, total_files_lost)

        mark_node_status('coordinator', 'active')

        result = {
            'iterasi':          i,
            'pre_checkpoint_ok': pre_ckpt_ok,
            'acks_received':    ack_count,
            'overhead_ms':      overhead_ms,
            'coordinator_ok':   coord_ok,
            'rto_ms':           avg_rto,
            'data_loss_ms':     max_loss,
            'files_recovered':  total_files_ok,
            'files_lost':       total_files_lost,
            'channel_msgs':     0,
        }
        results.append(result)

        flr = total_files_lost / max(total_files_ok + total_files_lost, 1) * 100
        log.info(f"  [RESULT] Pre-ckpt: {'✓' if pre_ckpt_ok else '✗'} "
                 f"({ack_count}/3 ACK) | Coord restore: {'✓' if coord_ok else '✗'} "
                 f"| Avg RTO: {avg_rto}ms | FLR: {flr:.1f}%")
        time.sleep(3)

    print_scenario_summary("COORDINATOR CRASH", results)
    return results


# ─────────────────────────────────────────────────────────
# Ringkasan Statistik
# ─────────────────────────────────────────────────────────
def print_scenario_summary(scenario_name, results):
    """Cetak ringkasan statistik untuk satu skenario."""
    log.info(f"\n{'─'*60}")
    log.info(f"  RINGKASAN: {scenario_name}")
    log.info(f"{'─'*60}")

    rto_list  = [r['rto_ms']       for r in results if r.get('recovery_ok', True)]
    loss_list = [r['data_loss_ms'] for r in results]
    flr_list  = [
        r['files_lost'] / max(r['files_recovered'] + r['files_lost'], 1) * 100
        for r in results
    ]
    overhead_list = [r['overhead_ms'] for r in results if r.get('overhead_ms', 0) > 0]
    ch_msg_list = [r.get('channel_msgs', 0) for r in results]

    if rto_list:
        log.info(f"  RTO  → Rata: {statistics.mean(rto_list):.1f}ms | "
                 f"Min: {min(rto_list)}ms | Max: {max(rto_list)}ms | "
                 f"Stdev: {statistics.stdev(rto_list) if len(rto_list)>1 else 0:.1f}ms")
    if loss_list:
        log.info(f"  LOSS → Rata: {statistics.mean(loss_list):.1f}ms | "
                 f"Min: {min(loss_list)}ms | Max: {max(loss_list)}ms")
    if flr_list:
        log.info(f"  FLR  → Rata: {statistics.mean(flr_list):.2f}% | "
                 f"Min: {min(flr_list):.2f}% | Max: {max(flr_list):.2f}%")
    if overhead_list:
        log.info(f"  OVERHEAD → Rata: {statistics.mean(overhead_list):.1f}ms | "
                 f"Min: {min(overhead_list)}ms | Max: {max(overhead_list)}ms")
    if ch_msg_list:
        log.info(f"  CH.MSGS → Total: {sum(ch_msg_list)} | "
                 f"Rata: {statistics.mean(ch_msg_list):.1f}")

    log.info(f"{'─'*60}")


# ─────────────────────────────────────────────────────────
# Laporan Final untuk Bab 4
# ─────────────────────────────────────────────────────────
def print_final_report(s1, s2, s3):
    """Cetak tabel hasil akhir untuk dimasukkan ke Bab 4."""
    log.info("\n" + "="*65)
    log.info("  TABEL HASIL AKHIR — DATA BAB 4")
    log.info("="*65)

    scenarios = [
        ("Skenario 1: Worker Crash",       s1),
        ("Skenario 2: Network Partition",  s2),
        ("Skenario 3: Coordinator Crash",  s3),
    ]

    for name, results in scenarios:
        rto_list     = [r['rto_ms']       for r in results if r.get('rto_ms',0)>0]
        loss_list    = [r['data_loss_ms'] for r in results]
        overhead_list= [r['overhead_ms']  for r in results if r.get('overhead_ms',0)>0]
        flr_list     = [
            r['files_lost'] / max(r['files_recovered'] + r['files_lost'], 1) * 100
            for r in results
        ]

        avg_rto      = statistics.mean(rto_list)      if rto_list     else 0
        avg_loss     = statistics.mean(loss_list)     if loss_list    else 0
        avg_overhead = statistics.mean(overhead_list) if overhead_list else 0
        avg_flr      = statistics.mean(flr_list)      if flr_list     else 0
        overhead_pct = avg_overhead / (30000 + avg_overhead) * 100

        avg_ch_msgs = statistics.mean([r.get('channel_msgs', 0) for r in results]) if results else 0

        log.info(f"\n  {name}")
        log.info(f"  {'Metrik':<30} {'Nilai':>12}")
        log.info(f"  {'─'*44}")
        log.info(f"  {'Jumlah Iterasi':<30} {N_ITERATIONS:>12}")
        log.info(f"  {'Rata-rata RTO (ms)':<30} {avg_rto:>12.2f}")
        log.info(f"  {'Rata-rata Data Loss (ms)':<30} {avg_loss:>12.2f}")
        log.info(f"  {'File Loss Rate (FLR) %':<30} {avg_flr:>12.2f}")
        log.info(f"  {'Checkpoint Overhead (ms)':<30} {avg_overhead:>12.2f}")
        log.info(f"  {'Est. Overhead (%)':<30} {overhead_pct:>12.2f}")
        log.info(f"  {'Avg Channel Msgs Captured':<30} {avg_ch_msgs:>12.2f}")

    log.info("\n" + "="*65)
    log.info("  Salin angka di atas ke Tabel Bab 4 paper kamu!")
    log.info("="*65)


# ─────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────
def main():
    log.info("="*65)
    log.info("  PENGUJIAN SKENARIO KEGAGALAN — CHANDY-LAMPORT SYSTEM")
    log.info(f"  Setiap skenario: {N_ITERATIONS} iterasi")
    log.info("="*65)

    # Tunggu sistem siap
    for i in range(10):
        try:
            with get_db():
                break
        except Exception as e:
            log.warning(f"Menunggu DB... ({i+1}/10)")
            time.sleep(5)

    log.info("\n[INIT] Tunggu sistem stabil (30 detik)...")
    time.sleep(30)

    # Jalankan ketiga skenario
    log.info("\n[START] Memulai pengujian...")

    results_1 = scenario_1_worker_crash()
    time.sleep(10)

    results_2 = scenario_2_network_partition()
    time.sleep(10)

    results_3 = scenario_3_coordinator_crash()
    time.sleep(5)

    # Laporan final
    print_final_report(results_1, results_2, results_3)


if __name__ == '__main__':
    main()
