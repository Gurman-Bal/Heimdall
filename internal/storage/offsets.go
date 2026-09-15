package storage

import (
	"database/sql"
)

func (s *Store) GetOffset(source, path string) (int64, bool, error) {
	var offset int64

	err := s.db.QueryRow(
		`SELECT offset
		 FROM source_offsets
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
		`INSERT INTO source_offsets (source, path, offset)
		 VALUES (?, ?, ?)
		 ON CONFLICT(source, path)
		 DO UPDATE SET offset = excluded.offset`,
		source,
		path,
		offset,
	)

	return err
}
