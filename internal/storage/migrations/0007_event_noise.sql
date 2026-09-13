-- +goose Up
CREATE TABLE IF NOT EXISTS event_noise (
                                           id         INTEGER PRIMARY KEY AUTOINCREMENT,
                                           source     TEXT NOT NULL,
                                           type       TEXT NOT NULL,
                                           message    TEXT NOT NULL,
                                           count      INTEGER NOT NULL DEFAULT 1,
                                           first_seen DATETIME NOT NULL,
                                           last_seen  DATETIME NOT NULL,
                                           UNIQUE(source, type, message)
    );
CREATE INDEX IF NOT EXISTS idx_event_noise_last_seen ON event_noise(last_seen);

-- +goose Down
DROP TABLE IF EXISTS event_noise;