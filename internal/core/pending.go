package core

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"regexp"
	"strings"
	"time"
)

type PendingStore interface {
	SavePendingEvents(events []Event, threshold int, window time.Duration) ([]Event, error)
	PromoteExpiredPending(cutoff time.Time) ([]Event, error)
}

type EventPublisher interface {
	Publish(Event)
}

type PendingEventManager struct {
	store     PendingStore
	bus       EventPublisher
	window    time.Duration
	threshold int
}

func NewPendingEventManager(
	store PendingStore,
	bus EventPublisher,
	window time.Duration,
	threshold int,
) *PendingEventManager {
	m := &PendingEventManager{
		store:     store,
		bus:       bus,
		window:    window,
		threshold: threshold,
	}

	go m.expiryLoop()

	return m
}

func (m *PendingEventManager) ProcessBatch(events []Event) error {
	if len(events) == 0 {
		return nil
	}

	publish, err := m.store.SavePendingEvents(
		events,
		m.threshold,
		m.window,
	)
	if err != nil {
		return err
	}

	for _, e := range publish {
		m.bus.Publish(e)
	}

	return nil
}

func (m *PendingEventManager) expiryLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		cutoff := time.Now().Add(-m.window)

		events, err := m.store.PromoteExpiredPending(cutoff)
		if err != nil {
			slog.Warn(
				"failed to promote expired pending events",
				"error",
				err,
			)
			continue
		}

		for _, e := range events {
			m.bus.Publish(e)
		}
	}
}

var variableToken = regexp.MustCompile(
	`\b(?:0x)?[0-9a-fA-F]{4,}\b|\b\d+\b`,
)

func NormalizeMessage(message string) string {
	return variableToken.ReplaceAllString(message, "#")
}

func EventFingerprint(e Event) string {
	h := sha256.New()

	h.Write([]byte(e.Source))
	h.Write([]byte{0})
	h.Write([]byte(e.Type))
	h.Write([]byte{0})
	h.Write([]byte(strings.TrimSpace(NormalizeMessage(e.Message))))

	return hex.EncodeToString(h.Sum(nil))
}
