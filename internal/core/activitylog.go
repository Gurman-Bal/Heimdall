package core

import (
	"context"
	"log/slog"
	"time"
)

type ActivityEntry struct {
	ID      int64
	Time    time.Time
	Level   string
	Message string
	Attrs   map[string]string
}

// ActivityStore is implemented by storage.Store. Defined here as an
// interface so core never imports storage (storage already imports core
// for Event, the reverse would be a cycle).
type ActivityStore interface {
	SaveActivity(e ActivityEntry) error
	RecentActivity(since time.Time, limit int) ([]ActivityEntry, error)
	PruneActivityOlderThan(cutoff time.Time) (int64, error)
}

// ActivityLog buffers Heimdall's own operational log lines and persists
// them in small batches, so the Activity tab survives restarts and supports
// querying a time window (1h/24h/48h).
type ActivityLog struct {
	ch    chan ActivityEntry
	store ActivityStore
}

func NewActivityLog(store ActivityStore, bufferSize int) *ActivityLog {
	a := &ActivityLog{ch: make(chan ActivityEntry, bufferSize), store: store}
	go a.run()
	return a
}

func (a *ActivityLog) run() {
	const maxBatch = 100
	const flushInterval = time.Second

	batch := make([]ActivityEntry, 0, maxBatch)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		for _, e := range batch {
			if err := a.store.SaveActivity(e); err != nil {
				// Deliberately not slog.Error here: that would re-enter this
				// same handler and risk a loop. stderr directly instead.
				println("failed to save activity record:", err.Error())
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case e, ok := <-a.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, e)
			if len(batch) >= maxBatch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (a *ActivityLog) add(e ActivityEntry) {
	select {
	case a.ch <- e:
	default:
		// Activity log entries are diagnostic, not monitored data - fine to
		// drop under truly extreme load rather than block the caller.
	}
}

func (a *ActivityLog) Recent(since time.Time, limit int) ([]ActivityEntry, error) {
	return a.store.RecentActivity(since, limit)
}

func (a *ActivityLog) Prune(olderThan time.Duration) {
	cutoff := time.Now().Add(-olderThan)
	if n, err := a.store.PruneActivityOlderThan(cutoff); err == nil && n > 0 {
		slog.Info("pruned old activity records", "count", n, "older_than", olderThan)
	}
}

func (a *ActivityLog) Handler(wrapped slog.Handler) slog.Handler {
	return &activityHandler{wrapped: wrapped, log: a}
}

type activityHandler struct {
	wrapped slog.Handler
	log     *ActivityLog
}

func (h *activityHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.wrapped.Enabled(ctx, level)
}

func (h *activityHandler) Handle(ctx context.Context, r slog.Record) error {
	attrs := map[string]string{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	h.log.add(ActivityEntry{Time: r.Time, Level: r.Level.String(), Message: r.Message, Attrs: attrs})
	return h.wrapped.Handle(ctx, r)
}

func (h *activityHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &activityHandler{wrapped: h.wrapped.WithAttrs(attrs), log: h.log}
}

func (h *activityHandler) WithGroup(name string) slog.Handler {
	return &activityHandler{wrapped: h.wrapped.WithGroup(name), log: h.log}
}
