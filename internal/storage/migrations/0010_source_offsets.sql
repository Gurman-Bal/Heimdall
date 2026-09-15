-- +goose Up

CREATE TABLE IF NOT EXISTS source_offsets (
                                              source TEXT NOT NULL,
                                              path TEXT NOT NULL,
                                              offset INTEGER NOT NULL DEFAULT 0,
                                              PRIMARY KEY (source, path)
    );

-- +goose Down

DROP TABLE IF EXISTS source_offsets;