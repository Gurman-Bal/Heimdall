package storage

import (
	"database/sql"
	"fmt"
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

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`PRAGMA journal_mode=WAL;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	if _, err := db.Exec(`PRAGMA busy_timeout=5000;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to set busy timeout: %w", err)
	}

	s := &Store{db: db}

	if err := s.migrate(); err != nil {
		_ = db.Close()
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

func (s *Store) SaveEvent(e core.Event) error {
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

	for _, e := range events {
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

// -----------------------------------------------------------------------------
// Pending events
// -----------------------------------------------------------------------------

func (s *Store) SavePendingEvents(
	events []core.Event,
	threshold int,
	window time.Duration,
) ([]core.Event, error) {
	if len(events) == 0 {
		return nil, nil
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	publish := make([]core.Event, 0)

	for _, e := range events {
		if e.Severity != "info" || e.Type != "log" {
			if err := s.insertEventTx(tx, e); err != nil {
				return nil, err
			}

			publish = append(publish, e)
			continue
		}

		fingerprint := core.EventFingerprint(e)

		var count int

		err := tx.QueryRow(
			`SELECT COUNT(*)
FROM event_pending
WHERE fingerprint = ?`,
			fingerprint,
		).Scan(&count)

		if err != nil {
			return nil, err
		}

		if count == 0 {
			_, err := tx.Exec(
				`INSERT INTO event_pending
(source, type, message, severity, timestamp, fingerprint)
VALUES (?, ?, ?, ?, ?, ?)`,
				e.Source,
				e.Type,
				e.Message,
				e.Severity,
				e.Timestamp,
				fingerprint,
			)
			if err != nil {
				return nil, err
			}

			continue
		}

		var firstSeen time.Time

		err = tx.QueryRow(
			`SELECT MIN(timestamp)
FROM event_pending
WHERE fingerprint = ?`,
			fingerprint,
		).Scan(&firstSeen)

		if err != nil {
			return nil, err
		}

		if e.Timestamp.Sub(firstSeen) > window {
			expired, err := s.promoteFingerprintTx(tx, fingerprint)
			if err != nil {
				return nil, err
			}

			publish = append(publish, expired...)

			_, err = tx.Exec(
				`INSERT INTO event_pending
(source, type, message, severity, timestamp, fingerprint)
VALUES (?, ?, ?, ?, ?, ?)`,
				e.Source,
				e.Type,
				e.Message,
				e.Severity,
				e.Timestamp,
				fingerprint,
			)
			if err != nil {
				return nil, err
			}

			continue
		}

		_, err = tx.Exec(
			`INSERT INTO event_pending
(source, type, message, severity, timestamp, fingerprint)
VALUES (?, ?, ?, ?, ?, ?)`,
			e.Source,
			e.Type,
			e.Message,
			e.Severity,
			e.Timestamp,
			fingerprint,
		)
		if err != nil {
			return nil, err
		}

		count++

		if count >= threshold {
			if err := s.promoteFingerprintToNoiseTx(tx, fingerprint); err != nil {
				return nil, err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return publish, nil
}

func (s *Store) PromoteExpiredPending(cutoff time.Time) ([]core.Event, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.Query(
		`SELECT id, timestamp, source, type, severity, message
FROM event_pending
WHERE timestamp < ?
ORDER BY id ASC`,
		cutoff,
	)
	if err != nil {
		return nil, err
	}

	var events []core.Event
	var ids []int64

	for rows.Next() {
		var e core.Event

		if err := rows.Scan(
			&e.ID,
			&e.Timestamp,
			&e.Source,
			&e.Type,
			&e.Severity,
			&e.Message,
		); err != nil {
			_ = rows.Close()
			return nil, err
		}

		events = append(events, e)
		ids = append(ids, e.ID)
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	if len(events) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}

		return nil, nil
	}

	stmt, err := tx.Prepare(
		`INSERT INTO events
(timestamp, source, type, severity, message)
VALUES (?, ?, ?, ?, ?)`,
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	for _, e := range events {
		if _, err := stmt.Exec(
			e.Timestamp,
			e.Source,
			e.Type,
			e.Severity,
			e.Message,
		); err != nil {
			return nil, err
		}
	}

	deleteStmt, err := tx.Prepare(
		`DELETE FROM event_pending WHERE id = ?`,
	)
	if err != nil {
		return nil, err
	}
	defer deleteStmt.Close()

	for _, id := range ids {
		if _, err := deleteStmt.Exec(id); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return events, nil
}

func (s *Store) insertEventTx(tx *sql.Tx, e core.Event) error {
	_, err := tx.Exec(
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

func (s *Store) promoteFingerprintTx(
	tx *sql.Tx,
	fingerprint string,
) ([]core.Event, error) {
	rows, err := tx.Query(
		`SELECT id, timestamp, source, type, severity, message
FROM event_pending
WHERE fingerprint = ?
ORDER BY id ASC`,
		fingerprint,
	)
	if err != nil {
		return nil, err
	}

	var events []core.Event
	var ids []int64

	for rows.Next() {
		var e core.Event

		if err := rows.Scan(
			&e.ID,
			&e.Timestamp,
			&e.Source,
			&e.Type,
			&e.Severity,
			&e.Message,
		); err != nil {
			_ = rows.Close()
			return nil, err
		}

		events = append(events, e)
		ids = append(ids, e.ID)
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	for _, e := range events {
		if err := s.insertEventTx(tx, e); err != nil {
			return nil, err
		}
	}

	for _, id := range ids {
		if _, err := tx.Exec(
			`DELETE FROM event_pending WHERE id = ?`,
			id,
		); err != nil {
			return nil, err
		}
	}

	return events, nil
}

func (s *Store) promoteFingerprintToNoiseTx(
	tx *sql.Tx,
	fingerprint string,
) error {
	var source string
	var eventType string
	var message string
	var firstSeen time.Time
	var lastSeen time.Time
	var count int64

	err := tx.QueryRow(
		`SELECT source, type, message,
       MIN(timestamp),
       MAX(timestamp),
       COUNT(*)
FROM event_pending
WHERE fingerprint = ?`,
		fingerprint,
	).Scan(
		&source,
		&eventType,
		&message,
		&firstSeen,
		&lastSeen,
		&count,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		`INSERT INTO event_noise
(fingerprint, source, type, message, count, first_seen, last_seen)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fingerprint) DO UPDATE SET
    count = event_noise.count + excluded.count,
    last_seen = excluded.last_seen`,
		fingerprint,
		source,
		eventType,
		message,
		count,
		firstSeen,
		lastSeen,
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		`DELETE FROM event_pending WHERE fingerprint = ?`,
		fingerprint,
	)

	return err
}
