-- +goose Up

CREATE TABLE IF NOT EXISTS event_pending (
                                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                                             source TEXT NOT NULL,
                                             type TEXT NOT NULL,
                                             message TEXT NOT NULL,
                                             severity TEXT NOT NULL,
                                             timestamp DATETIME NOT NULL,
                                             fingerprint TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_event_pending_fingerprint
    ON event_pending(fingerprint);

CREATE INDEX IF NOT EXISTS idx_event_pending_timestamp
    ON event_pending(timestamp);

-- +goose Down

DROP TABLE IF EXISTS event_pending;