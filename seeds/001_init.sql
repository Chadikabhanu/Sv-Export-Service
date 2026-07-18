-- Runs automatically on first container start because /seeds is mounted to
-- /docker-entrypoint-initdb.d in the postgres image.

CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    email       VARCHAR(255) NOT NULL UNIQUE,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Table that tracks export job state so it survives app restarts and so a
-- freshly-opened WebSocket connection can immediately report current status.
CREATE TABLE IF NOT EXISTS exports (
    export_id       UUID PRIMARY KEY,
    status          VARCHAR(20) NOT NULL DEFAULT 'queued', -- queued|processing|completed|failed|cancelled
    total           INTEGER DEFAULT 0,
    processed       INTEGER DEFAULT 0,
    file_path       TEXT,
    file_size       BIGINT,
    error           TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP
);

-- Bulk-generate 120,000 sample users. generate_series keeps this fast and
-- avoids shipping a giant static seed file.
INSERT INTO users (name, email)
SELECT
    'User ' || i,
    'user' || i || '@example.com'
FROM generate_series(1, 120000) AS i
ON CONFLICT (email) DO NOTHING;
