-- +goose Up

ALTER TABLE event_noise
    ADD COLUMN fingerprint TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS
    idx_event_noise_fingerprint
    ON event_noise(fingerprint);

-- +goose Down

DROP INDEX IF EXISTS idx_event_noise_fingerprint;