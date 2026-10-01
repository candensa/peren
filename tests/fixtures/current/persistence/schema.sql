CREATE TABLE IF NOT EXISTS kv (
    scope TEXT NOT NULL,
    k     BLOB NOT NULL,
    v     BLOB NOT NULL,
    PRIMARY KEY (scope, k)
);

CREATE TABLE IF NOT EXISTS alarms (
    scope         TEXT PRIMARY KEY,
    at_ms         INTEGER NOT NULL,
    retry         INTEGER NOT NULL DEFAULT 0,
    counted_retry INTEGER NOT NULL DEFAULT 0,
    generation    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS cell_metadata (
    scope       TEXT PRIMARY KEY,
    actor_name  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ws_attachments (
    connection_id TEXT PRIMARY KEY,
    bytes         BLOB NOT NULL
);
