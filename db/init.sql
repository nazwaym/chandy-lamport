-- ============================================================
-- SKEMA DATABASE SISTEM DISTRIBUTED CHECKPOINTING
-- Chandy-Lamport Implementation
-- ============================================================

-- Tabel 1: Registri semua node aktif
CREATE TABLE IF NOT EXISTS nodes (
    node_id     SERIAL PRIMARY KEY,
    node_name   VARCHAR(50) UNIQUE NOT NULL,
    ip_address  VARCHAR(50),
    role        VARCHAR(20) CHECK (role IN ('coordinator', 'worker')),
    status      VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'failed', 'recovering')),
    last_heartbeat TIMESTAMP DEFAULT NOW(),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Tabel 2: Metadata setiap file checkpoint (.ckpt)
CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id   SERIAL PRIMARY KEY,
    node_id         INTEGER REFERENCES nodes(node_id),
    session_id      VARCHAR(100),
    sequence_number INTEGER DEFAULT 0,
    file_path       VARCHAR(255),
    checksum        VARCHAR(64),
    file_size_bytes INTEGER DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'valid' CHECK (status IN ('valid', 'corrupted', 'restored')),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Tabel 3: Log sesi koordinasi global
CREATE TABLE IF NOT EXISTS checkpoint_sessions (
    session_id      VARCHAR(100) PRIMARY KEY,
    trigger_type    VARCHAR(30) DEFAULT 'periodic' CHECK (trigger_type IN ('periodic', 'manual', 'pre_failure')),
    global_status   VARCHAR(20) DEFAULT 'running' CHECK (global_status IN ('running', 'completed', 'failed')),
    total_nodes     INTEGER DEFAULT 0,
    acked_nodes     INTEGER DEFAULT 0,
    started_at      TIMESTAMP DEFAULT NOW(),
    completed_at    TIMESTAMP,
    duration_ms     INTEGER
);

-- Tabel 4: Histori pemulihan sistem
CREATE TABLE IF NOT EXISTS recovery_logs (
    recovery_id         SERIAL PRIMARY KEY,
    checkpoint_id       INTEGER REFERENCES checkpoints(checkpoint_id),
    session_id          VARCHAR(100),
    failed_node         VARCHAR(50),
    trigger_reason      VARCHAR(100),
    status              VARCHAR(20) DEFAULT 'success' CHECK (status IN ('success', 'failed', 'partial')),
    recovery_time_ms    INTEGER,
    data_loss_ms        INTEGER,
    files_recovered     INTEGER DEFAULT 0,
    files_lost          INTEGER DEFAULT 0,
    recovered_at        TIMESTAMP DEFAULT NOW()
);

-- Tabel 5: State aktual task saat checkpoint
CREATE TABLE IF NOT EXISTS task_states (
    task_id         SERIAL PRIMARY KEY,
    node_id         INTEGER REFERENCES nodes(node_id),
    checkpoint_id   INTEGER REFERENCES checkpoints(checkpoint_id),
    task_name       VARCHAR(100),
    state_data      JSONB,
    progress_pct    FLOAT DEFAULT 0.0,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Tabel 6: Channel state (in-transit messages) per sesi Chandy-Lamport
CREATE TABLE IF NOT EXISTS channel_states (
    channel_state_id SERIAL PRIMARY KEY,
    session_id       VARCHAR(100),
    from_node        VARCHAR(50),
    to_node          VARCHAR(50),
    messages         JSONB DEFAULT '[]',
    message_count    INTEGER DEFAULT 0,
    created_at       TIMESTAMP DEFAULT NOW()
);

-- Index untuk performa query
CREATE INDEX IF NOT EXISTS idx_channel_states_session ON channel_states(session_id);
CREATE INDEX IF NOT EXISTS idx_checkpoints_session ON checkpoints(session_id);
CREATE INDEX IF NOT EXISTS idx_checkpoints_node ON checkpoints(node_id);
CREATE INDEX IF NOT EXISTS idx_recovery_session ON recovery_logs(session_id);
CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);

-- View untuk melihat sesi checkpoint terakhir
CREATE OR REPLACE VIEW v_latest_checkpoints AS
SELECT 
    n.node_name,
    n.role,
    n.status AS node_status,
    c.session_id,
    c.sequence_number,
    c.file_path,
    c.checksum,
    c.status AS checkpoint_status,
    c.created_at
FROM nodes n
LEFT JOIN checkpoints c ON n.node_id = c.node_id
WHERE c.checkpoint_id IN (
    SELECT MAX(checkpoint_id) 
    FROM checkpoints 
    GROUP BY node_id
);

-- View metrik ringkasan untuk Bab 4
CREATE OR REPLACE VIEW v_metrics_summary AS
SELECT
    cs.session_id,
    cs.global_status,
    cs.total_nodes,
    cs.acked_nodes,
    cs.duration_ms,
    COUNT(rl.recovery_id) AS total_recoveries,
    AVG(rl.recovery_time_ms) AS avg_recovery_time_ms,
    AVG(rl.data_loss_ms) AS avg_data_loss_ms,
    SUM(rl.files_lost) AS total_files_lost,
    SUM(rl.files_recovered) AS total_files_recovered
FROM checkpoint_sessions cs
LEFT JOIN recovery_logs rl ON cs.session_id = rl.session_id
GROUP BY cs.session_id, cs.global_status, cs.total_nodes, cs.acked_nodes, cs.duration_ms;

