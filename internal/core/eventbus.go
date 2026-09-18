package core

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type EventBus struct {
	mu          sync.Mutex
	subscribers map[chan Event]struct{}
	dropped     atomic.Int64
}

func NewEventBus() *EventBus {
	b := &EventBus{subscribers: map[chan Event]struct{}{}}
	go b.reportDrops()
	return b
}

// Subscription is a live feed from the bus. Callers must call Close when
// done (e.g. when an SSE client disconnects) - without it, a stream of
// disconnected browser tabs each leaves its channel subscribed forever,
// quietly leaking memory and eventually filling up with dropped events
// nobody's reading.
type Subscription struct {
	Events chan Event
	bus    *EventBus
}

func (sub *Subscription) Close() {
	sub.bus.mu.Lock()
	delete(sub.bus.subscribers, sub.Events)
	sub.bus.mu.Unlock()
	close(sub.Events)
}

func (b *EventBus) Subscribe(buffer int) *Subscription {
	ch := make(chan Event, buffer)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()
	return &Subscription{Events: ch, bus: b}
}

func (b *EventBus) Publish(e Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
			slog.Warn("event bus dropped events - buffer full, increase HEIMDALL_EVENT_BUFFER_SIZE if this recurs",
				"dropped_since_last_report", current-last, "total_dropped", current)
		}
		last = current
	}
}
