package storage

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"heimdall/internal/core"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func New(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	// SQLite is shared by the controller and worker processes.
	//
	// WAL allows readers to continue while another process is writing.
	// busy_timeout gives SQLite time to wait for a competing writer rather
	// than immediately returning "database is locked".
	//
	// Keep one connection per process. This makes connection-specific SQLite
	// PRAGMAs such as busy_timeout predictable and avoids unnecessary
	// concurrent connections against the same SQLite database.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	if _, err := db.Exec(`PRAGMA busy_timeout=5000;`); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	s := &Store{db: db}

	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}

	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// -----------------------------------------------------------------------------
// Events
// -----------------------------------------------------------------------------

// isNoise reports whether an event's severity marks it as something the
// rule engine wants suppressed from the normal Watch feed (RULES tab: rows
// with severity "ignore"). These are still tracked, just aggregated instead
// of stored per-occurrence, so a single chatty source can't flood the
// events table or push a user's expanded row out of the result window.
func isNoise(severity string) bool {
	return strings.EqualFold(severity, "ignore")
}

func (s *Store) SaveEvent(e core.Event) error {
	if isNoise(e.Severity) {
		return s.recordNoise(e)
	}

	_, err := s.db.Exec(
		`INSERT INTO events
(timestamp, source, type, severity, message)
VALUES (?, ?, ?, ?, ?)`,
		e.Timestamp,
		e.Source,
		e.Type,
		e.Severity,
		e.Message,
	)

	return err
}

func (s *Store) RecentEvents(limit int) ([]core.Event, error) {
	rows, err := s.db.Query(
		`SELECT id, timestamp, source, type, severity, message
FROM events
ORDER BY id DESC
LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Return [] rather than null when there are no events.
	events := []core.Event{}

	for rows.Next() {
		var e core.Event
		var ts time.Time

		if err := rows.Scan(
			&e.ID,
			&ts,
			&e.Source,
			&e.Type,
			&e.Severity,
			&e.Message,
		); err != nil {
			return nil, err
		}

		e.Timestamp = ts
		events = append(events, e)
	}

	return events, rows.Err()
}

// -----------------------------------------------------------------------------
// Offsets
// -----------------------------------------------------------------------------

func (s *Store) GetOffset(source, path string) (int64, bool, error) {
	var offset int64

	err := s.db.QueryRow(
		`SELECT offset
FROM offsets
WHERE source = ? AND path = ?`,
		source,
		path,
	).Scan(&offset)

	if err == sql.ErrNoRows {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	return offset, true, nil
}

func (s *Store) SetOffset(source, path string, offset int64) error {
	_, err := s.db.Exec(
		`INSERT INTO offsets (source, path, offset)
VALUES (?, ?, ?)
ON CONFLICT(source, path)
DO UPDATE SET offset = excluded.offset`,
		source,
		path,
		offset,
	)

	return err
}

// EventsSince returns events at or after the given time, oldest first.
// Noise never lands in the events table (see SaveEvent), so this
// naturally excludes it without any extra filtering here.
func (s *Store) EventsSince(since time.Time) ([]core.Event, error) {
	rows, err := s.db.Query(
		`SELECT id, timestamp, source, type, severity, message
FROM events
WHERE timestamp >= ?
ORDER BY id ASC`,
		since,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []core.Event{}

	for rows.Next() {
		var e core.Event
		var ts time.Time

		if err := rows.Scan(
			&e.ID,
			&ts,
			&e.Source,
			&e.Type,
			&e.Severity,
			&e.Message,
		); err != nil {
			return nil, err
		}

		e.Timestamp = ts
		events = append(events, e)
	}

	return events, rows.Err()
}

func (s *Store) SaveEvents(events []core.Event) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(
		`INSERT INTO events
(timestamp, source, type, severity, message)
VALUES (?, ?, ?, ?, ?)`,
	)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()

	noiseStmt, err := tx.Prepare(`
		INSERT INTO event_noise (source, type, message, count, first_seen, last_seen)
		VALUES (?, ?, ?, 1, ?, ?)
		ON CONFLICT(source, type, message) DO UPDATE SET
			count     = count + 1,
			last_seen = excluded.last_seen
	`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer noiseStmt.Close()

	for _, e := range events {
		if isNoise(e.Severity) {
			if _, err := noiseStmt.Exec(e.Source, e.Type, e.Message, e.Timestamp, e.Timestamp); err != nil {
				_ = tx.Rollback()
				return err
			}
			continue
		}
		if _, err := stmt.Exec(
			e.Timestamp,
			e.Source,
			e.Type,
			e.Severity,
			e.Message,
		); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

// -----------------------------------------------------------------------------
// Noise (aggregated, suppressed events)
// -----------------------------------------------------------------------------

type NoiseCount struct {
	ID        int64     `json:"id"`
	Source    string    `json:"source"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
	Count     int64     `json:"count"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
}

func (s *Store) recordNoise(e core.Event) error {
	_, err := s.db.Exec(`
		INSERT INTO event_noise (source, type, message, count, first_seen, last_seen)
		VALUES (?, ?, ?, 1, ?, ?)
		ON CONFLICT(source, type, message) DO UPDATE SET
			count     = count + 1,
			last_seen = excluded.last_seen
	`, e.Source, e.Type, e.Message, e.Timestamp, e.Timestamp)
	return err
}

// ListNoise returns aggregated noise counts, most recently seen first.
func (s *Store) ListNoise(limit int) ([]NoiseCount, error) {
	rows, err := s.db.Query(
		`SELECT id, source, type, message, count, first_seen, last_seen
FROM event_noise
ORDER BY last_seen DESC
LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []NoiseCount{}
	for rows.Next() {
		var n NoiseCount
		if err := rows.Scan(&n.ID, &n.Source, &n.Type, &n.Message, &n.Count, &n.FirstSeen, &n.LastSeen); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// PruneNoiseOlderThan removes noise buckets that haven't been seen recently,
// so long-dead noise patterns (e.g. a removed source) don't linger forever.
func (s *Store) PruneNoiseOlderThan(cutoff time.Time) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM event_noise WHERE last_seen < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
