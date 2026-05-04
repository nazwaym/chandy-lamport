<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/>
  <img src="https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL"/>
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License"/>
</p>

<h1 align="center">🔄 Distributed Checkpointing — Chandy-Lamport Algorithm</h1>

<p align="center">
  <strong>Implementasi Distributed Checkpointing Menggunakan Algoritma Chandy-Lamport<br/>pada Cloud Computing untuk Meminimalisir Kehilangan File</strong>
</p>

<p align="center">
  Sistem distributed snapshot yang berjalan di atas Docker (simulasi cloud environment)<br/>
  dengan komunikasi TCP socket, pencatatan channel state, dan propagasi MARKER antar-node.
</p>

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Algorithm Flow](#-algorithm-flow---chandy-lamport)
- [Project Structure](#-project-structure)
- [Tech Stack](#-tech-stack)
- [Prerequisites](#-prerequisites)
- [Getting Started](#-getting-started)
- [Running Tests](#-running-tests)
- [Collecting Results](#-collecting-results)
- [Database Schema](#-database-schema)
- [Monitoring & Debugging](#-monitoring--debugging)
- [Key Metrics](#-key-metrics)
- [Configuration](#%EF%B8%8F-configuration)
- [Failure Scenarios](#-failure-scenarios)
- [Troubleshooting](#-troubleshooting)

---

## 🔍 Overview

Proyek ini mengimplementasikan **algoritma Chandy-Lamport** untuk distributed checkpointing pada lingkungan cloud computing yang disimulasikan menggunakan Docker. Sistem dirancang untuk:

- **Menangkap global snapshot** yang konsisten dari seluruh sistem terdistribusi
- **Merekam channel state** (in-transit messages) sesuai teori Chandy-Lamport
- **Meminimalisir kehilangan file** melalui mekanisme checkpoint dan recovery otomatis
- **Mengukur metrik kinerja** (RTO, Data Loss, FLR, Checkpoint Overhead)

### Fitur Utama

| Fitur | Deskripsi |
|-------|-----------|
| 🔁 **Periodic Checkpointing** | Checkpoint otomatis setiap 30 detik |
| 📨 **Application Messages** | Komunikasi nyata antar-worker (file transfer, task delegation) |
| 📸 **Channel State Recording** | Pencatatan in-transit messages sesuai algoritma Chandy-Lamport |
| 🔀 **MARKER Propagation** | Propagasi MARKER dari worker ke semua peer workers |
| 💾 **Persistent Storage** | Checkpoint disimpan sebagai file `.ckpt` (pickle + SHA256) |
| 🗃️ **Metadata in PostgreSQL** | Semua metadata tersimpan di database relasional |
| 🔄 **Auto Recovery** | Deteksi kegagalan via heartbeat dan pemulihan otomatis |
| 🧪 **Failure Testing** | 3 skenario pengujian × 10 iterasi masing-masing |
| 🐳 **Dockerized** | Seluruh sistem berjalan dalam container (simulasi cloud) |

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Docker Network (ckpt_net)                     │
│                                                                 │
│  ┌──────────────┐         MARKER          ┌──────────────┐      │
│  │              │ ──────────────────────▶  │              │      │
│  │  Coordinator │         ACK             │   Worker 1   │      │
│  │  (port 5000) │ ◀──────────────────────  │  (port 6001) │      │
│  │              │                         │              │      │
│  └──────┬───────┘                         └──┬───┬───────┘      │
│         │                                    │   │               │
│         │ MARKER                    MARKER   │   │ APP_MSG       │
│         │                          ┌─────────┘   │               │
│         ▼                          ▼             ▼               │
│  ┌──────────────┐          ┌──────────────┐                     │
│  │              │◀─ MARKER─│              │                     │
│  │   Worker 2   │  APP_MSG │   Worker 3   │                     │
│  │  (port 6002) │─ MARKER─▶│  (port 6003) │                     │
│  │              │  APP_MSG │              │                     │
│  └──────────────┘          └──────────────┘                     │
│                                                                 │
│  ┌──────────────┐          ┌──────────────┐                     │
│  │   Recovery   │          │  PostgreSQL  │                     │
│  │   Manager    │─────────▶│     (DB)     │                     │
│  │              │          │  (port 5432) │                     │
│  └──────────────┘          └──────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
```

### Komponen Sistem

| Komponen | Container | Port | Fungsi |
|----------|-----------|------|--------|
| **Coordinator** | `coordinator` | 5000 (→7000) | Inisiator checkpoint, kirim MARKER, kumpulkan ACK |
| **Worker 1** | `worker1` | 6001 | Proses task, terima/propagasi MARKER, rekam snapshot |
| **Worker 2** | `worker2` | 6002 | Proses task, terima/propagasi MARKER, rekam snapshot |
| **Worker 3** | `worker3` | 6003 | Proses task, terima/propagasi MARKER, rekam snapshot |
| **Recovery Manager** | `recovery_manager` | — | Monitor heartbeat, deteksi kegagalan, pulihkan state |
| **PostgreSQL** | `checkpoint_db` | 5432 | Simpan metadata checkpoint, session, dan recovery logs |

---

## 🔄 Algorithm Flow — Chandy-Lamport

Berikut alur lengkap satu sesi checkpoint sesuai algoritma Chandy-Lamport:

```
Waktu ──────────────────────────────────────────────────────────────▶

Coordinator    ─── MARKER ──▶ Worker1    Worker2    Worker3
                   MARKER ──▶            Worker2
                   MARKER ──▶                       Worker3

Worker1        Terima MARKER dari coordinator
               ├── 1. Rekam LOCAL STATE ✓
               ├── 2. Channel[coordinator] = KOSONG
               ├── 3. Mulai RECORDING di channel[worker2] & channel[worker3]
               └── 4. Propagasi MARKER → worker2, worker3

Worker2        Terima MARKER dari coordinator (atau worker1, mana duluan)
               ├── 1. Rekam LOCAL STATE ✓
               ├── 2. Channel[pengirim_pertama] = KOSONG
               ├── 3. Mulai RECORDING di channel lainnya
               └── 4. Propagasi MARKER → worker1, worker3

Worker1        Terima MARKER dari worker2
               ├── STOP recording di channel[worker2]
               └── Simpan pesan tercatat sebagai channel state

Worker1        Terima MARKER dari worker3
               ├── STOP recording di channel[worker3]
               ├── ★ SNAPSHOT LENGKAP (semua channel selesai)
               ├── Simpan full snapshot (local + channel) → .ckpt
               └── Kirim ACK → coordinator

Coordinator    Terima ACK dari SEMUA worker
               └── ★ GLOBAL SNAPSHOT BERHASIL
```

### Apa Itu Channel State?

Channel state merekam **pesan yang sedang transit** (in-flight) di antara dua proses saat snapshot diambil:

```
Worker1 ──── [APP_MSG: file_transfer] ────▶ Worker2
                      ↑
              Pesan ini sedang "terbang"
              dan harus dicatat sebagai
              CHANNEL STATE agar tidak hilang
```

Tanpa channel state, pesan in-transit akan **hilang saat recovery**, menyebabkan inkonsistensi.

---

## 📁 Project Structure

```
chandy-lamport/
│
├── 📄 docker-compose.yml           # Orkestrasi seluruh container
├── 📄 README.md                    # Dokumentasi proyek (file ini)
│
├── 📂 coordinator/
│   ├── 📄 Dockerfile               # Image coordinator
│   ├── 📄 coordinator.py           # Inisiator checkpoint, MARKER sender
│   ├── 📄 run_tests.py             # Skrip pengujian 3 skenario kegagalan
│   └── 📄 collect_results.py       # Pengumpul data metrik untuk Bab 4
│
├── 📂 worker/
│   ├── 📄 Dockerfile               # Image worker
│   └── 📄 worker.py                # Chandy-Lamport non-initiator logic
│                                    #   ├── Application messages
│                                    #   ├── Channel state recording
│                                    #   ├── MARKER propagation
│                                    #   └── Full snapshot (local + channel)
│
├── 📂 recovery_manager/
│   ├── 📄 Dockerfile               # Image recovery manager
│   └── 📄 recovery_manager.py      # Heartbeat monitor & auto-recovery
│
├── 📂 db/
│   └── 📄 init.sql                 # Skema database PostgreSQL (6 tabel)
│
├── 📂 test_scenarios/
│   └── 📄 run_tests.py             # Mirror skrip pengujian
│
└── 📂 scripts/
    └── 📄 collect_results.py       # Mirror pengumpul hasil
```

---

## 🛠 Tech Stack

| Teknologi | Versi | Fungsi |
|-----------|-------|--------|
| **Python** | 3.11 | Bahasa pemrograman utama |
| **Docker** | 24+ | Containerization (simulasi cloud) |
| **Docker Compose** | 2+ | Orkestrasi multi-container |
| **PostgreSQL** | 15-alpine | Database metadata & recovery logs |
| **TCP Socket** | — | Komunikasi antar-node (FIFO channel) |
| **pickle** | stdlib | Serialisasi state ke file `.ckpt` |
| **hashlib (SHA256)** | stdlib | Checksum integritas file checkpoint |
| **psycopg2** | latest | PostgreSQL driver untuk Python |
| **threading** | stdlib | Non-blocking concurrency |

---

## 📋 Prerequisites

Pastikan tools berikut sudah terinstal di sistem Anda:

```bash
# Cek Docker
docker --version          # minimal v24+

# Cek Docker Compose
docker compose version    # minimal v2+
```

> **Note:** Tidak ada dependensi Python yang perlu diinstal secara manual. Semua dependensi diinstal otomatis di dalam Docker container.

---

## 🚀 Getting Started

### 1. Clone Repository

```bash
git clone https://github.com/<username>/chandy-lamport.git
cd chandy-lamport
```

### 2. Build dan Jalankan Semua Container

```bash
docker compose up --build -d
```

### 3. Verifikasi Semua Container Running

```bash
docker compose ps
```

Output yang diharapkan:

```
NAME                STATUS
checkpoint_db       running (healthy)
coordinator         running
worker1             running
worker2             running
worker3             running
recovery_manager    running
```

### 4. Monitor Log Sistem

```bash
# Log semua container
docker compose logs -f

# Atau per container
docker logs -f coordinator
docker logs -f worker1
```

Contoh output log:

```log
[10:30:01] COORDINATOR | INFO | [INIT] Memulai Checkpoint | Session: abc12345...
[10:30:01] COORDINATOR | INFO | [MARKER] → worker1 (worker1:6001)

[10:30:01] WORKER1     | INFO | [CL] ═══ MARKER PERTAMA dari coordinator | Sesi: abc12345
[10:30:01] WORKER1     | INFO | [CL] Local state direkam ✓
[10:30:01] WORKER1     | INFO | [CL] Recording dimulai pada channel: ['worker2', 'worker3']
[10:30:01] WORKER1     | INFO | [CL-MARKER →] Propagasi MARKER ke worker2
[10:30:01] WORKER1     | INFO | [CL-MARKER →] Propagasi MARKER ke worker3

[10:30:01] WORKER1     | INFO | [CL-RECORD] Pesan dari worker2 dicatat (sesi abc12345, total: 1)
[10:30:02] WORKER1     | INFO | [CL] MARKER berikutnya dari worker2 | Sesi: abc12345
[10:30:02] WORKER1     | INFO | [CL] Recording STOP di channel worker2 | Pesan tercatat: 1
[10:30:02] WORKER1     | INFO | [CL] ★ Snapshot LENGKAP untuk sesi abc12345
[10:30:02] WORKER1     | INFO | [SNAPSHOT ✓] worker1_abc12345_1234.ckpt | channel_msgs: 1

[10:30:02] COORDINATOR | INFO | [ACK ✓] worker1 → sesi abc12345 | channel_msgs: 1
[10:30:02] COORDINATOR | INFO | [✓ GLOBAL] Checkpoint BERHASIL & Dicatat ke DB
```

### 5. Tunggu Checkpoint Periodik

Checkpoint berjalan **otomatis setiap 30 detik**. Tunggu minimal **5–10 menit** agar tersedia cukup data.

```bash
# Cek jumlah sesi checkpoint
docker exec checkpoint_db psql -U admin -d checkpoint_db \
  -c "SELECT global_status, COUNT(*) FROM checkpoint_sessions GROUP BY global_status;"
```

---

## 🧪 Running Tests

Skrip pengujian menjalankan **3 skenario kegagalan**, masing-masing **10 iterasi**:

```bash
docker exec -it coordinator python /app/run_tests.py
```

| Skenario | Deskripsi | Yang Diukur |
|----------|-----------|-------------|
| **1. Worker Crash** | Satu worker dihentikan paksa lalu dipulihkan | RTO, FLR, Data Loss |
| **2. Network Partition** | Koneksi ke worker diblokir (timeout) | Deteksi partisi, Recovery |
| **3. Coordinator Crash** | Coordinator crash, semua node dipulihkan | Full system RTO |

> **Estimasi waktu:** ±15–20 menit untuk seluruh pengujian.

---

## 📊 Collecting Results

Setelah pengujian selesai, kumpulkan data metrik:

```bash
docker exec -it coordinator python /app/collect_results.py
```

Output mencakup:

- **Tabel implementasi sistem** — node terdaftar, status, checkpoint count
- **Hasil per skenario** — RTO, Data Loss, FLR per iterasi
- **Analisis metrik kinerja** — overhead, channel state summary
- **Tabel perbandingan skenario** — siap masuk ke paper/laporan
- **Export JSON** — data mentah untuk analisis lebih lanjut

### Ambil File Hasil

```bash
docker cp coordinator:/checkpoint_storage/results_bab4.json ./results_bab4.json
```

---

## 🗃 Database Schema

Sistem menggunakan **6 tabel** di PostgreSQL:

```
┌──────────────────┐     ┌────────────────────┐     ┌────────────────────┐
│      nodes       │     │    checkpoints     │     │ checkpoint_sessions│
├──────────────────┤     ├────────────────────┤     ├────────────────────┤
│ node_id (PK)     │◀───┤ node_id (FK)       │     │ session_id (PK)    │
│ node_name        │     │ checkpoint_id (PK) │     │ trigger_type       │
│ ip_address       │     │ session_id         │     │ global_status      │
│ role             │     │ file_path          │     │ total_nodes        │
│ status           │     │ checksum (SHA256)  │     │ acked_nodes        │
│ last_heartbeat   │     │ file_size_bytes    │     │ duration_ms        │
└──────────────────┘     │ status             │     └────────────────────┘
                         └────────────────────┘
┌──────────────────┐     ┌────────────────────┐     ┌────────────────────┐
│   task_states    │     │  recovery_logs     │     │  channel_states    │
├──────────────────┤     ├────────────────────┤     ├────────────────────┤
│ task_id (PK)     │     │ recovery_id (PK)   │     │ channel_state_id   │
│ node_id (FK)     │     │ checkpoint_id (FK) │     │ session_id         │
│ checkpoint_id(FK)│     │ failed_node        │     │ from_node          │
│ task_name        │     │ trigger_reason     │     │ to_node            │
│ state_data (JSON)│     │ recovery_time_ms   │     │ messages (JSONB)   │
│ progress_pct     │     │ data_loss_ms       │     │ message_count      │
└──────────────────┘     │ files_recovered    │     └────────────────────┘
                         │ files_lost         │
                         └────────────────────┘
```

### Query Berguna

```sql
-- Masuk ke database
docker exec -it checkpoint_db psql -U admin -d checkpoint_db

-- Lihat 10 sesi checkpoint terakhir
SELECT session_id, global_status, acked_nodes, duration_ms
FROM checkpoint_sessions ORDER BY started_at DESC LIMIT 10;

-- Lihat channel state (in-transit messages)
SELECT session_id, from_node, to_node, message_count
FROM channel_states ORDER BY created_at DESC LIMIT 20;

-- Total in-transit messages per sesi
SELECT session_id, SUM(message_count) AS total_in_transit
FROM channel_states GROUP BY session_id ORDER BY session_id DESC LIMIT 10;

-- Hasil recovery per skenario
SELECT trigger_reason, COUNT(*) AS total,
       AVG(recovery_time_ms) AS avg_rto,
       AVG(data_loss_ms) AS avg_loss
FROM recovery_logs GROUP BY trigger_reason;

-- View ringkasan metrik
SELECT * FROM v_metrics_summary;
```

---

## 👀 Monitoring & Debugging

### Log per Container

```bash
docker logs -f coordinator        # Log coordinator
docker logs -f worker1            # Log worker 1
docker logs -f worker2            # Log worker 2
docker logs -f worker3            # Log worker 3
docker logs -f recovery_manager   # Log recovery manager
```

### Inspeksi File Checkpoint

```bash
# List semua file checkpoint
docker exec coordinator ls -la /checkpoint_storage/

# Inspect isi file .ckpt (Python pickle)
docker exec coordinator python -c "
import pickle, json
with open('/checkpoint_storage/<nama_file>.ckpt', 'rb') as f:
    data = pickle.load(f)
print(json.dumps(data, indent=2, default=str))
"
```

### Inspeksi Container

```bash
# Cek resource usage
docker stats

# Masuk ke dalam container
docker exec -it worker1 /bin/bash
```

---

## 📈 Key Metrics

Sistem mengukur 4 metrik utama:

| Metrik | Definisi | Formula |
|--------|----------|---------|
| **RTO** (Recovery Time Objective) | Waktu pemulihan dari checkpoint ke state aktif | `waktu_restore_selesai - waktu_restore_mulai` |
| **Data Loss** | Durasi data yang hilang antara checkpoint terakhir dan kegagalan | `waktu_kegagalan - waktu_checkpoint_terakhir` |
| **FLR** (File Loss Rate) | Persentase file yang hilang saat pemulihan | `files_lost / (files_lost + files_recovered) × 100%` |
| **Checkpoint Overhead** | Durasi proses checkpoint relatif terhadap interval normal | `durasi_checkpoint / (interval + durasi_checkpoint) × 100%` |

Metrik tambahan setelah implementasi Chandy-Lamport:

| Metrik | Definisi |
|--------|----------|
| **Channel Messages** | Jumlah in-transit messages yang tercapture per sesi |
| **Snapshot Completeness** | Apakah snapshot mencakup local state + channel state |

---

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Deskripsi |
|----------|---------|-----------|
| `NODE_NAME` | `worker1` | Nama unik worker |
| `NODE_PORT` | `6001` | Port TCP worker |
| `COORDINATOR_HOST` | `coordinator` | Hostname coordinator |
| `COORDINATOR_PORT` | `5000` | Port TCP coordinator |
| `DB_HOST` | `db` | Hostname PostgreSQL |
| `DB_PORT` | `5432` | Port PostgreSQL |
| `DB_NAME` | `checkpoint_db` | Nama database |
| `DB_USER` | `admin` | Username database |
| `DB_PASS` | `admin123` | Password database |

### Tunable Parameters

| Parameter | File | Default | Deskripsi |
|-----------|------|---------|-----------|
| `CHECKPOINT_INTERVAL` | `coordinator.py` | `30s` | Interval antar checkpoint periodik |
| `MARKER_TIMEOUT` | `coordinator.py` | `30s` | Timeout tunggu ACK dari worker |
| `HEARTBEAT_INTERVAL` | `worker.py` | `5s` | Interval heartbeat ke coordinator |
| `TASK_INTERVAL` | `worker.py` | `2s` | Interval simulasi task |
| `APP_MSG_INTERVAL` | `worker.py` | `3s` | Interval application message antar-worker |
| `HEARTBEAT_TIMEOUT` | `recovery_manager.py` | `15s` | Timeout deteksi kegagalan node |
| `N_ITERATIONS` | `run_tests.py` | `10` | Jumlah iterasi per skenario pengujian |

---

## 💥 Failure Scenarios

### Skenario 1: Worker Crash

```
Worker1 dihentikan paksa → sistem mendeteksi via heartbeat timeout
→ Recovery manager memuat checkpoint terakhir → Worker dipulihkan
```

### Skenario 2: Network Partition

```
Koneksi ke Worker2 terputus → MARKER timeout → Coordinator menandai failed
→ Recovery dari checkpoint sebelum partisi → Worker kembali aktif
```

### Skenario 3: Coordinator Crash

```
Coordinator crash → Semua worker tetap punya checkpoint terakhir
→ Coordinator restart → State dipulihkan dari .ckpt → Sistem kembali normal
```

---

## ❓ Troubleshooting

| Masalah | Penyebab | Solusi |
|---------|----------|--------|
| Container tidak start | Database belum healthy | `docker compose logs db` — tunggu sampai healthy |
| Worker tidak kirim ACK | Port 6001-6003 bentrok | Pastikan port tidak dipakai aplikasi lain |
| `psycopg2` error | Dependency tidak terinstal | Rebuild: `docker compose build --no-cache` |
| Checkpoint tidak tersimpan | Volume tidak terbuat | `docker volume inspect checkpoint_storage` |
| Tabel `channel_states` tidak ada | Database lama masih ada | Reset: `docker compose down -v` lalu `up --build` |
| MARKER timeout | Worker belum ready | Naikkan `MARKER_TIMEOUT` atau tunggu lebih lama |
| `Connection refused` antar-worker | Worker belum selesai startup | Tunggu ~10 detik setelah container UP |

### Reset Lengkap

```bash
# Hentikan semua container dan hapus volume
docker compose down -v

# Build ulang dari awal
docker compose up --build -d
```

---

## 📊 Hasil dan Evaluasi

### A. Hasil Implementasi Sistem

Setelah sistem dijalankan, seluruh komponen berjalan stabil:

| Komponen | Status | Checkpoint | Keterangan |
|----------|--------|------------|------------|
| Coordinator | ✅ Active | 43 sesi | Inisiator checkpoint setiap 30 detik |
| Worker 1 | ✅ Active | 43 file .ckpt | Snapshot + channel state tersimpan |
| Worker 2 | ✅ Active | 43 file .ckpt | Snapshot + channel state tersimpan |
| Worker 3 | ✅ Active | 43 file .ckpt | Snapshot + channel state tersimpan |
| Recovery Manager | ✅ Active | — | Monitoring heartbeat aktif |
| PostgreSQL | ✅ Healthy | 6 tabel | Metadata lengkap |

**Ringkasan checkpoint periodik:**

| Metrik | Nilai |
|--------|-------|
| Total sesi checkpoint | 40+ sesi |
| Success rate | **100%** (40/40 completed) |
| Rata-rata durasi checkpoint | **~1.110 ms** per sesi |
| ACK rate per sesi | **3/3 worker** (semua merespons) |
| File .ckpt per node | 40+ file valid |

### B. Bukti Algoritma Chandy-Lamport Berjalan

Berikut **log aktual** dari sistem yang membuktikan setiap langkah Chandy-Lamport dieksekusi:

#### Langkah 1 — Coordinator mengirim MARKER ke semua worker:
```log
[03:52:32] COORDINATOR | [MARKER] → worker1 (worker1:6001)
[03:52:32] COORDINATOR | [MARKER] → worker2 (worker2:6002)
[03:52:32] COORDINATOR | [MARKER] → worker3 (worker3:6003)
```

#### Langkah 2 — Worker menerima MARKER pertama, rekam local state, mulai recording:
```log
[03:52:01] WORKER1 | [MARKER ←] Diterima dari coordinator | Sesi: bfd7c165
[03:52:01] WORKER1 | [CL] ═══ MARKER PERTAMA dari coordinator | Sesi: bfd7c165
[03:52:01] WORKER1 | [CL] Local state direkam ✓
[03:52:01] WORKER1 | [CL] Recording dimulai pada channel: ['worker2', 'worker3']
```

#### Langkah 3 — Worker mempropagasi MARKER ke semua peer:
```log
[03:52:01] WORKER1 | [CL-MARKER →] Propagasi MARKER ke worker2
[03:52:01] WORKER1 | [CL-MARKER →] Propagasi MARKER ke worker3
```

#### Langkah 4 — Worker menerima MARKER dari peer, stop recording:
```log
[03:52:01] WORKER1 | [MARKER ←] Diterima dari peer worker3 | Sesi: bfd7c165
[03:52:01] WORKER1 | [CL] MARKER berikutnya dari worker3 | Sesi: bfd7c165
[03:52:01] WORKER1 | [CL] Recording STOP di channel worker3 | Pesan tercatat: 0
[03:52:01] WORKER1 | [MARKER ←] Diterima dari peer worker2 | Sesi: bfd7c165
[03:52:01] WORKER1 | [CL] MARKER berikutnya dari worker2 | Sesi: bfd7c165
[03:52:01] WORKER1 | [CL] Recording STOP di channel worker2 | Pesan tercatat: 0
```

#### Langkah 5 — Snapshot lengkap (local + channel state), simpan .ckpt:
```log
[03:52:01] WORKER1 | [CL] ★ Snapshot LENGKAP untuk sesi bfd7c165
[03:52:01] WORKER1 | [CL] Total in-transit messages tercatat: 0
[03:52:01] WORKER1 | [SNAPSHOT ✓] worker1_bfd7c165_1777866721.ckpt | checksum: da1328af47c7...
```

#### Langkah 6 — ACK ke coordinator dengan channel state summary:
```log
[03:52:01] WORKER1 | [CL] Snapshot lengkap, mengirim ACK ke coordinator
[03:52:32] COORDINATOR | [ACK ✓] worker1 → sesi 077ae707 | channel_msgs: 0 | channels: {'coordinator': 0, 'worker2': 0, 'worker3': 0}
[03:52:32] COORDINATOR | [ACK ✓] worker2 → sesi 077ae707 | channel_msgs: 0 | channels: {'coordinator': 0, 'worker3': 0, 'worker1': 0}
[03:52:32] COORDINATOR | [ACK ✓] worker3 → sesi 077ae707 | channel_msgs: 0 | channels: {'coordinator': 0, 'worker1': 0, 'worker2': 0}
[03:52:32] COORDINATOR | [✓ GLOBAL] Checkpoint BERHASIL & Dicatat ke DB
[03:52:32] COORDINATOR | [✓ GLOBAL] ACK: 3/3 | Durasi: 653ms
[03:52:32] COORDINATOR | [✓ GLOBAL] Channel state summary:
```

### C. Evaluasi Kepatuhan Algoritma Chandy-Lamport

| # | Aspek Teori Chandy-Lamport (1985) | Implementasi | Bukti |
|---|---|---|---|
| 1 | Inisiator merekam local state sendiri | ✅ | `save_coordinator_checkpoint()` |
| 2 | Inisiator mengirim MARKER ke semua outgoing channel | ✅ | Log: `[MARKER] → worker1, worker2, worker3` |
| 3 | Non-inisiator merekam local state saat terima MARKER pertama | ✅ | Log: `[CL] Local state direkam ✓` |
| 4 | Non-inisiator mem-forward MARKER ke semua outgoing channel | ✅ | Log: `[CL-MARKER →] Propagasi MARKER ke worker2` |
| 5 | Channel state pengirim MARKER pertama = kosong | ✅ | `ss['channel_state'][from_node] = []` |
| 6 | Mulai recording di semua channel lain | ✅ | Log: `Recording dimulai pada channel: ['worker2', 'worker3']` |
| 7 | Stop recording saat terima MARKER dari channel tersebut | ✅ | Log: `Recording STOP di channel worker3 \| Pesan tercatat: 0` |
| 8 | Pesan antara snapshot dan MARKER dicatat sebagai channel state | ✅ | `record_incoming_app_message()` |
| 9 | Global snapshot = Σ(local state) + Σ(channel state) | ✅ | File .ckpt berisi `local_state` + `channel_state` |
| 10 | Channel FIFO | ✅ | TCP menjamin FIFO per-connection |
| 11 | MARKER duplikat diabaikan (idempotent) | ✅ | `if from_node in ss['markers_received']: return` |
| 12 | Snapshot selesai saat semua MARKER diterima dari semua channel | ✅ | `markers_received == ALL_INCOMING_CHANNELS` |

**Skor kepatuhan teori: 12/12 (100%)** ✅

### D. Analisis Kinerja

#### 1. Checkpoint Overhead

| Metrik | Nilai |
|--------|-------|
| Rata-rata durasi checkpoint | ~1.110 ms |
| Interval checkpoint | 30.000 ms |
| **Estimasi overhead** | **~3,57%** |

> Overhead ~3,57% menunjukkan bahwa proses checkpoint **tidak mengganggu operasi normal** secara signifikan. Dalam praktik industri, overhead di bawah 5% dianggap **acceptable** (referensi: Apache Flink checkpointing guideline).

#### 2. Channel State Recording

| Metrik | Nilai |
|--------|-------|
| Total sesi dengan channel state | 43 sesi |
| Channel per sesi per worker | 3 channel (coordinator + 2 peer) |
| Total record di tabel `channel_states` | 43 × 9 = 387 record |

Setiap sesi checkpoint mencatat **9 channel state** (3 worker × 3 incoming channel), membuktikan bahwa mekanisme recording berjalan pada **semua channel**.

#### 3. Konsistensi Global Snapshot

Snapshot dianggap **consistent cut** jika memenuhi:
- ✅ Semua process merekam local state **setelah** menerima MARKER
- ✅ Pesan in-transit dicatat sebagai channel state
- ✅ Tidak ada pesan yang dikirim sesudah snapshot tapi diterima sebelum snapshot (dijamin oleh FIFO + MARKER sebagai separator)


---



