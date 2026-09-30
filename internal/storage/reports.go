package storage

import (
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

type ReportRecord struct {
	ID          int64
	GeneratedAt time.Time
	PeriodStart time.Time
	PeriodEnd   time.Time
	EventCount  int
	Summary     string
	IssuesJSON  string
	Model       string
}

func (s *Store) SaveReport(r ReportRecord) (int64, error) {
	res, err := s.db.Exec(
		`INSERT INTO reports (generated_at, period_start, period_end, event_count, summary, issues_json, model)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.GeneratedAt, r.PeriodStart, r.PeriodEnd, r.EventCount, r.Summary, r.IssuesJSON, r.Model,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) ListReports(limit int) ([]ReportRecord, error) {
	rows, err := s.db.Query(
		`SELECT id, generated_at, period_start, period_end, event_count, summary, issues_json, model
		 FROM reports ORDER BY id DESC LIMIT ?`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			slog.Error("failed to close rows", "error", err)
		}
	}(rows)

	var out []ReportRecord
	for rows.Next() {
		var r ReportRecord
		if err := rows.Scan(&r.ID, &r.GeneratedAt, &r.PeriodStart, &r.PeriodEnd, &r.EventCount, &r.Summary, &r.IssuesJSON, &r.Model); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) GetReport(id int64) (ReportRecord, error) {
	var r ReportRecord
	err := s.db.QueryRow(
		`SELECT id, generated_at, period_start, period_end, event_count, summary, issues_json, model FROM reports WHERE id = ?`, id,
	).Scan(&r.ID, &r.GeneratedAt, &r.PeriodStart, &r.PeriodEnd, &r.EventCount, &r.Summary, &r.IssuesJSON, &r.Model)
	return r, err
}

func (s *Store) LastReportTime() (time.Time, error) {
	slog.Info("LastReportTime: before QueryRow")

	var t time.Time

	err := s.db.QueryRow(
		`SELECT period_end
         FROM reports
         ORDER BY id DESC
         LIMIT 1`,
	).Scan(&t)

	slog.Info("LastReportTime: after QueryRow",
		"time", t,
		"error", err,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}

	return t, err
}
