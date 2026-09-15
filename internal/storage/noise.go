package storage

import (
	"database/sql"
	"time"
)

type NoiseCount struct {
	ID          int64     `json:"id"`
	Fingerprint string    `json:"fingerprint"`
	Source      string    `json:"source"`
	Type        string    `json:"type"`
	Message     string    `json:"message"`
	Count       int64     `json:"count"`
	FirstSeen   time.Time `json:"first_seen"`
	LastSeen    time.Time `json:"last_seen"`
}

func (s *Store) ListNoise(limit int) ([]NoiseCount, error) {
	rows, err := s.db.Query(
		`SELECT id, fingerprint, source, type, message,
		        count, first_seen, last_seen
		 FROM event_noise
		 ORDER BY last_seen DESC
		 LIMIT ?`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]NoiseCount, 0)

	for rows.Next() {
		var n NoiseCount
		var fingerprint sql.NullString

		if err := rows.Scan(
			&n.ID,
			&fingerprint,
			&n.Source,
			&n.Type,
			&n.Message,
			&n.Count,
			&n.FirstSeen,
			&n.LastSeen,
		); err != nil {
			return nil, err
		}

		if fingerprint.Valid {
			n.Fingerprint = fingerprint.String
		}

		out = append(out, n)
	}

	return out, rows.Err()
}
