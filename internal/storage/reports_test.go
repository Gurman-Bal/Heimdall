package storage

import (
	"testing"
	"time"
)

func TestSaveAndGetReport(t *testing.T) {
	store := newTestStore(t)

	now := time.Now().Truncate(time.Second)
	id, err := store.SaveReport(ReportRecord{
		GeneratedAt: now,
		PeriodStart: now.Add(-time.Hour),
		PeriodEnd:   now,
		EventCount:  5,
		Summary:     "all fine",
		IssuesJSON:  `[]`,
		Model:       "test-model",
	})
	if err != nil {
		t.Fatal(err)
	}

	report, err := store.GetReport(id)
	if err != nil {
		t.Fatal(err)
	}
	if report.Summary != "all fine" || report.EventCount != 5 || report.Model != "test-model" {
		t.Errorf("got %+v", report)
	}
}

func TestListReportsOrderedNewestFirst(t *testing.T) {
	store := newTestStore(t)

	for i := 0; i < 3; i++ {
		store.SaveReport(ReportRecord{
			GeneratedAt: time.Now(),
			PeriodStart: time.Now(),
			PeriodEnd:   time.Now(),
			Summary:     "report",
			IssuesJSON:  "[]",
			Model:       "m",
		})
	}

	reports, err := store.ListReports(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 3 {
		t.Fatalf("expected 3 reports, got %d", len(reports))
	}
	if reports[0].ID <= reports[1].ID {
		t.Error("expected newest report first")
	}
}

func TestLastReportTimeNoReportsYet(t *testing.T) {
	store := newTestStore(t)

	last, err := store.LastReportTime()
	if err != nil {
		t.Fatal(err)
	}
	if !last.IsZero() {
		t.Errorf("expected zero time with no reports, got %v", last)
	}
}

func TestLastReportTimeReturnsMostRecentPeriodEnd(t *testing.T) {
	store := newTestStore(t)

	older := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	newer := time.Now().Truncate(time.Second)

	store.SaveReport(ReportRecord{GeneratedAt: older, PeriodStart: older, PeriodEnd: older, Summary: "x", IssuesJSON: "[]", Model: "m"})
	store.SaveReport(ReportRecord{GeneratedAt: newer, PeriodStart: newer, PeriodEnd: newer, Summary: "y", IssuesJSON: "[]", Model: "m"})

	last, err := store.LastReportTime()
	if err != nil {
		t.Fatal(err)
	}
	if !last.Equal(newer) {
		t.Errorf("expected %v, got %v", newer, last)
	}
}
