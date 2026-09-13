package core

import (
	"log/slog"
	"time"
)

type Scheduler struct {
	plugins  []Plugin
	interval time.Duration
	bus      *EventBus
	spool    *EventSpool
}

func NewScheduler(bus *EventBus, spool *EventSpool, interval time.Duration) *Scheduler {
	return &Scheduler{bus: bus, spool: spool, interval: interval}
}

func (s *Scheduler) Register(p Plugin) {
	s.plugins = append(s.plugins, p)
	slog.Info("plugin registered", "plugin", p.Name())
}

func (s *Scheduler) Run(stop <-chan struct{}) {
	for _, p := range s.plugins {
		if err := p.Start(); err != nil {
			slog.Error("plugin failed to start", "plugin", p.Name(), "error", err)
		} else {
			slog.Info("plugin started", "plugin", p.Name())
		}
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			for _, p := range s.plugins {
				events, err := p.Poll()
				if err != nil {
					slog.Error("plugin poll failed", "plugin", p.Name(), "error", err)
					continue
				}
				for _, e := range events {
					s.bus.Publish(e) // live SSE - best-effort, fine to drop
					s.spool.Push(e)  // persistence - never dropped, spills to disk if needed
				}
			}
		}
	}
}
