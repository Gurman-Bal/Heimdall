package core

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type EventSubscription struct {
	Events <-chan Event
	cancel func()
}

func (s *EventSubscription) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}

type EventBus struct {
	mu          sync.RWMutex
	subscribers map[chan Event]struct{}
	dropped     atomic.Int64
}

func NewEventBus() *EventBus {
	b := &EventBus{
		subscribers: make(map[chan Event]struct{}),
	}

	go b.reportDrops()

	return b
}

func (b *EventBus) Subscribe(buffer int) *EventSubscription {
	ch := make(chan Event, buffer)

	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()

	return &EventSubscription{
		Events: ch,
		cancel: func() {
			b.mu.Lock()

			if _, ok := b.subscribers[ch]; ok {
				delete(b.subscribers, ch)
				close(ch)
			}

			b.mu.Unlock()
		},
	}
}

func (b *EventBus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.subscribers {
		select {
		case ch <- e:
		default:
			b.dropped.Add(1)
		}
	}
}

func (b *EventBus) DroppedCount() int64 {
	return b.dropped.Load()
}

func (b *EventBus) reportDrops() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	var last int64

	for range ticker.C {
		current := b.dropped.Load()

		if current > last {
			slog.Warn(
				"event bus dropped events",
				"dropped_since_last_report",
				current-last,
				"total_dropped",
				current,
			)
		}

		last = current
	}
}
