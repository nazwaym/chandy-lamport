"""
=============================================================
COLLECT RESULTS - Pengumpul Data untuk Bab 4
=============================================================
Jalankan SETELAH run_tests.py selesai untuk mendapatkan
tabel dan data yang siap masuk ke paper.

Cara pakai:
  docker exec -it coordinator python /app/collect_results.py
=============================================================
"""

import os
import json
import statistics
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'db'),
    'port':     int(os.getenv('DB_PORT', 5432)),
    'dbname':   os.getenv('DB_NAME', 'checkpoint_db'),
    'user':     os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASS', 'admin123'),
}


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def line(char='─', n=65):
    print(char * n)


def header(title):
    line('=')
    print(f"  {title}")
    line('=')


def sub(title):
    line('─')
    print(f"  {title}")
    line('─')


def collect_all():
    header("LAPORAN HASIL PENELITIAN — BAB IV")
    print(f"  Tanggal: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    line('=')

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # ══════════════════════════════════════════════════
            # A. Hasil Implementasi Sistem
            # ══════════════════════════════════════════════════
            print("\n[A] HASIL IMPLEMENTASI SISTEM")
            line()

            cur.execute("SELECT node_name, role, status, last_heartbeat FROM nodes ORDER BY role, node_name")
            nodes = cur.fetchall()
            print(f"\n  Node terdaftar: {len(nodes)}")
            print(f"  {'Node':<15} {'Role':<15} {'Status':<12} {'Last Heartbeat'}")
            line('-', 65)
            for n in nodes:
                hb = str(n['last_heartbeat'])[:19] if n['last_heartbeat'] else '-'
                print(f"  {n['node_name']:<15} {n['role']:<15} {n['status']:<12} {hb}")

            cur.execute("SELECT COUNT(*) AS total, global_status FROM checkpoint_sessions GROUP BY global_status")
            sessions = cur.fetchall()
            total_sessions = sum(r['total'] for r in sessions)
            print(f"\n  Total sesi checkpoint: {total_sessions}")
            for s in sessions:
                print(f"    {s['global_status']}: {s['total']}")

            cur.execute("SELECT COUNT(*) AS total FROM checkpoints WHERE status='valid'")
            ckpts = cur.fetchone()
            print(f"  Total file checkpoint valid: {ckpts['total']}")

            # ══════════════════════════════════════════════════
            # B. Hasil Pengujian Skenario Kegagalan
            # ══════════════════════════════════════════════════
            print("\n\n[B] HASIL PENGUJIAN SKENARIO KEGAGALAN")
            line()

            scenarios = ['worker_crash', 'network_partition', 'coordinator_crash']
            scenario_labels = {
                'worker_crash':       'Skenario 1: Worker Crash',
                'network_partition':  'Skenario 2: Network Partition',
                'coordinator_crash':  'Skenario 3: Coordinator Crash',
            }

            all_data = {}
            for sc in scenarios:
                cur.execute("""
                    SELECT recovery_time_ms, data_loss_ms,
                           files_recovered, files_lost, status
                    FROM recovery_logs
                    WHERE trigger_reason = %s
                    ORDER BY recovered_at
                """, (sc,))
                rows = cur.fetchall()
                all_data[sc] = rows

                if not rows:
                    print(f"\n  {scenario_labels[sc]}: Belum ada data")
                    continue

                rto_list  = [r['recovery_time_ms'] for r in rows if r['recovery_time_ms']]
                loss_list = [r['data_loss_ms']      for r in rows if r['data_loss_ms']]
                flr_list  = [
                    r['files_lost'] / max(r['files_recovered'] + r['files_lost'], 1) * 100
                    for r in rows
                ]
                ok_count  = sum(1 for r in rows if r['status'] == 'success')

                print(f"\n  {scenario_labels[sc]}")
                print(f"  {'─'*55}")
                print(f"  Jumlah iterasi       : {len(rows)}")
                print(f"  Berhasil / Gagal     : {ok_count} / {len(rows) - ok_count}")

                if rto_list:
                    print(f"  RTO rata-rata (ms)   : {statistics.mean(rto_list):.2f}")
                    print(f"  RTO min / max (ms)   : {min(rto_list)} / {max(rto_list)}")
                    if len(rto_list) > 1:
                        print(f"  RTO std dev (ms)     : {statistics.stdev(rto_list):.2f}")

                if loss_list:
                    print(f"  Data Loss rata² (ms) : {statistics.mean(loss_list):.2f}")
                    print(f"  Data Loss min / max  : {min(loss_list)} / {max(loss_list)}")

                if flr_list:
                    print(f"  FLR rata-rata (%)    : {statistics.mean(flr_list):.4f}")
                    print(f"  FLR min / max (%)    : {min(flr_list):.4f} / {max(flr_list):.4f}")

                # Tabel iterasi detail
                print(f"\n  Detail per iterasi:")
                print(f"  {'Iter':<6} {'Status':<10} {'RTO(ms)':<10} {'Loss(ms)':<12} {'FLR(%)':<10} {'Files OK'}")
                print(f"  {'─'*60}")
                for idx, r in enumerate(rows, 1):
                    flr = r['files_lost'] / max(r['files_recovered'] + r['files_lost'], 1) * 100
                    print(f"  {idx:<6} {r['status']:<10} "
                          f"{r['recovery_time_ms'] or 0:<10} "
                          f"{r['data_loss_ms'] or 0:<12} "
                          f"{flr:<10.4f} "
                          f"{r['files_recovered']}")

            # ══════════════════════════════════════════════════
            # C. Analisis Metrik Kinerja
            # ══════════════════════════════════════════════════
            print("\n\n[C] ANALISIS METRIK KINERJA")
            line()

            # Checkpoint Overhead
            cur.execute("""
                SELECT duration_ms FROM checkpoint_sessions
                WHERE global_status = 'completed' AND duration_ms IS NOT NULL
            """)
            overhead_rows = [r['duration_ms'] for r in cur.fetchall()]

            if overhead_rows:
                avg_overhead = statistics.mean(overhead_rows)
                overhead_pct = avg_overhead / (30000 + avg_overhead) * 100
                print(f"\n  CHECKPOINT OVERHEAD:")
                print(f"  Total sesi sukses    : {len(overhead_rows)}")
                print(f"  Rata-rata durasi     : {avg_overhead:.2f} ms")
                print(f"  Min / Max durasi     : {min(overhead_rows)} / {max(overhead_rows)} ms")
                if len(overhead_rows) > 1:
                    print(f"  Std Deviasi          : {statistics.stdev(overhead_rows):.2f} ms")
                print(f"  Estimasi overhead %  : {overhead_pct:.4f}%")

            # Channel State (Chandy-Lamport)
            cur.execute("""
                SELECT session_id,
                       COUNT(*) AS total_channels,
                       SUM(message_count) AS total_msgs,
                       AVG(message_count) AS avg_msgs_per_channel
                FROM channel_states
                GROUP BY session_id
                ORDER BY session_id
            """)
            ch_rows = cur.fetchall()
            if ch_rows:
                total_ch_msgs = sum(r['total_msgs'] or 0 for r in ch_rows)
                avg_ch_per_session = statistics.mean(
                    [r['total_msgs'] or 0 for r in ch_rows])
                print(f"\n  CHANNEL STATE (Chandy-Lamport):")
                print(f"  Total sesi dengan ch.state : {len(ch_rows)}")
                print(f"  Total in-transit messages  : {total_ch_msgs}")
                print(f"  Rata-rata per sesi         : {avg_ch_per_session:.2f}")

            # Tabel perbandingan antar skenario
            print(f"\n  TABEL PERBANDINGAN SKENARIO (Siap masuk paper):")
            line('-', 80)
            print(f"  {'Skenario':<25} {'RTO(ms)':>9} {'Loss(ms)':>10} "
                  f"{'FLR(%)':>8} {'Overhead':>10} {'Ch.Msgs':>8}")
            line('-', 80)

            for sc in scenarios:
                rows = all_data.get(sc, [])
                if not rows:
                    print(f"  {scenario_labels[sc]:<25} {'N/A':>9} {'N/A':>10} "
                          f"{'N/A':>8} {'N/A':>10} {'N/A':>8}")
                    continue
                rto  = statistics.mean([r['recovery_time_ms'] or 0 for r in rows])
                loss = statistics.mean([r['data_loss_ms'] or 0 for r in rows])
                flr  = statistics.mean([
                    r['files_lost'] / max(r['files_recovered'] + r['files_lost'], 1) * 100
                    for r in rows
                ])
                ovhd = avg_overhead if overhead_rows else 0
                # Query channel msgs for this scenario's sessions
                cur.execute("""
                    SELECT COALESCE(SUM(message_count), 0) AS total
                    FROM channel_states
                    WHERE session_id IN (
                        SELECT session_id FROM recovery_logs
                        WHERE trigger_reason = %s
                    )
                """, (sc,))
                ch_total = cur.fetchone()['total'] or 0
                short_name = {
                    'worker_crash':      'Worker Crash',
                    'network_partition': 'Network Partition',
                    'coordinator_crash': 'Coordinator Crash',
                }[sc]
                print(f"  {short_name:<25} {rto:>9.2f} {loss:>10.2f} "
                      f"{flr:>8.4f} {ovhd:>10.2f} {ch_total:>8}")

            line('-', 80)

            # ══════════════════════════════════════════════════
            # D. Export JSON untuk analisis lebih lanjut
            # ══════════════════════════════════════════════════
            print("\n\n[D] EXPORT DATA JSON")
            line()

            export = {}
            for sc in scenarios:
                rows = all_data.get(sc, [])
                export[sc] = [dict(r) for r in rows]

            # Konversi datetime ke string
            def serialize(obj):
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                return str(obj)

            json_path = '/checkpoint_storage/results_bab4.json'
            with open(json_path, 'w') as f:
                json.dump(export, f, default=serialize, indent=2)
            print(f"  Data berhasil diekspor ke: {json_path}")
            print(f"  (Bisa diambil dengan: docker cp coordinator:{json_path} .)")

    line('=')
    print("  Selesai! Gunakan data di atas untuk mengisi Bab 4 paper kamu.")
    line('=')


if __name__ == '__main__':
    collect_all()
