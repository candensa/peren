pub const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS kv (scope TEXT NOT NULL, k BLOB NOT NULL, v BLOB NOT NULL, PRIMARY KEY(scope,k)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS alarms (scope TEXT PRIMARY KEY, at_ms INTEGER NOT NULL, retry INTEGER NOT NULL DEFAULT 0, counted_retry INTEGER NOT NULL DEFAULT 0, generation INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS cell_metadata (scope TEXT PRIMARY KEY, actor_name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS ws_attachments (connection_id TEXT PRIMARY KEY, bytes BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS mutation_outcomes (id TEXT PRIMARY KEY, outcome BLOB NOT NULL, revision INTEGER NOT NULL CHECK(revision>=0));
CREATE TABLE IF NOT EXISTS effects (id TEXT PRIMARY KEY, destination TEXT NOT NULL, inbox_key TEXT NOT NULL, payload BLOB NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due_at_ms INTEGER NOT NULL, lease_token TEXT, leased_until_ms INTEGER, revision INTEGER NOT NULL CHECK(revision>=0), UNIQUE(destination,inbox_key));
CREATE INDEX IF NOT EXISTS effects_ready ON effects(status,due_at_ms);
CREATE TABLE IF NOT EXISTS storage_metadata (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL CHECK(revision>=0));
INSERT OR IGNORE INTO storage_metadata(id,revision) VALUES(1,0);
";
