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
	// e.id is now selected/scanned - it was silently dropped before,
	// meaning every event handed to the frontend had ID: 0.
	rows, err := s.db.Query(
		`SELECT e.id, e.timestamp, e.source, e.type, e.severity, e.message
         FROM events e
         WHERE NOT EXISTS (
             SELECT 1
             FROM event_noise n
             WHERE n.source = e.source
               AND n.type = e.type
               AND n.message = e.message
         )
         ORDER BY e.id DESC
         LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]core.Event, 0)

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
		`SELECT e.id, e.timestamp, e.source, e.type, e.severity, e.message
FROM events e
WHERE e.timestamp >= ?
  AND NOT EXISTS (
      SELECT 1
      FROM event_noise n
      WHERE n.source = e.source
        AND n.type = e.type
        AND n.message = e.message
  )
ORDER BY e.id ASC`,
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
// Offsets live in offsets.go (GetOffset/SetOffset) - not duplicated here.
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Pending events - unchanged from your version. PendingEventManager (core
// package) almost certainly calls these directly; I'm leaving the logic
// exactly as you had it rather than guessing at a rewrite without seeing
// that file. See the accompanying note about one specific thing worth
// checking in it.
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
		// A rule already deterministically decided this is noise (the
		// docker bridge / veth chatter rule, etc). Aggregate it straight
		// into event_noise and never let it touch `events` or the live
		// bus - there's no reason to wait through the pending/threshold
		// window for something already classified, and letting it fall
		// through to the "insert immediately" branch below (which used to
		// catch every non-info/log event, ignore included) was exactly
		// why explicitly-tagged noise kept flooding Watch no matter how
		// the fallback classifier or staging window were tuned.
		if strings.EqualFold(e.Severity, "ignore") {
			if err := s.recordNoiseTx(tx, e); err != nil {
				return nil, err
			}
			continue
		}

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

// recordNoiseTx aggregates an already-classified noise event directly,
// bypassing the pending/fingerprint staging entirely - that staging exists
// to figure out whether an *unclassified* line is noise by watching it
// repeat; something a rule already tagged ignore doesn't need to prove
// itself first.
func (s *Store) recordNoiseTx(tx *sql.Tx, e core.Event) error {
	fingerprint := core.EventFingerprint(e)
	pattern := core.NormalizeMessage(e.Message)

	_, err := tx.Exec(
		`INSERT INTO event_noise
(fingerprint, source, type, message, count, first_seen, last_seen)
VALUES (?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(fingerprint) DO UPDATE SET
    count = event_noise.count + 1,
    last_seen = excluded.last_seen`,
		fingerprint,
		e.Source,
		e.Type,
		pattern,
		e.Timestamp,
		e.Timestamp,
	)
	return err
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
