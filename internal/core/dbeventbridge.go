package core

import (
	"log/slog"
	"time"
)

type EventLister interface {
	EventsSince(since time.Time) ([]Event, error)
}

func StartDBEventBridge(
	store EventLister,
	bus *EventBus,
	interval time.Duration,
) {
	go func() {
		last := time.Now()
		seen := make(map[int64]struct{})

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			events, err := store.EventsSince(last)
			if err != nil {
				slog.Warn(
					"db event bridge poll failed",
					"error",
					err,
				)
				continue
			}

			for _, e := range events {
				if _, ok := seen[e.ID]; ok {
					continue
				}

				seen[e.ID] = struct{}{}
				bus.Publish(e)

				if e.Timestamp.After(last) {
					last = e.Timestamp
				}
			}

			if len(seen) > 10000 {
				next := make(map[int64]struct{}, 1000)

				for _, e := range events {
					next[e.ID] = struct{}{}
				}

				seen = next
			}
		}
	}()
}
