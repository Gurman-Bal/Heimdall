package storage

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"time"

	"heimdall/internal/core"
)

func (s *Store) SaveActivity(e core.ActivityEntry) error {
	attrsJSON, err := json.Marshal(e.Attrs)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT INTO activity_log (time, level, message, attrs) VALUES (?, ?, ?, ?)`,
		e.Time, e.Level, e.Message, string(attrsJSON),
	)
	return err
}

func (s *Store) RecentActivity(since time.Time, limit int) ([]core.ActivityEntry, error) {
	rows, err := s.db.Query(
		`SELECT id, time, level, message, attrs FROM activity_log WHERE time >= ? ORDER BY id DESC LIMIT ?`,
		since, limit,
	)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close storage", "error", err)
		}
	}(rows)

	var out []core.ActivityEntry
	for rows.Next() {
		var e core.ActivityEntry
		var attrsJSON string
		if err := rows.Scan(&e.ID, &e.Time, &e.Level, &e.Message, &attrsJSON); err != nil {
			return nil, err
		}
		e.Attrs = map[string]string{}
		if err := json.Unmarshal([]byte(attrsJSON), &e.Attrs); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) PruneActivityOlderThan(cutoff time.Time) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM activity_log WHERE time < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
