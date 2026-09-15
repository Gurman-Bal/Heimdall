package serverapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"heimdall/internal/core"
	"heimdall/internal/ingest"
	"heimdall/internal/services/reporting"
	"heimdall/internal/storage"
)

// Server is the worker's internal API - reachable only on the docker
// compose network, never published to the host. The controller is its only
// client. Every route is gated by a shared token so nothing else on the
// network can trigger reloads or read internal state.
type Server struct {
	store    *storage.Store
	rules    *core.RuleEngine
	reporter *reporting.Reporter
	sources  map[string]ManagedSource
	spool    *core.EventSpool
	bus      *core.EventBus
	status   *core.StatusTracker
	token    string
}

type ManagedSource interface {
	AddPath(path string)
	RemovePath(path string)
	Paths() []string
}

func New(store *storage.Store, rules *core.RuleEngine, reporter *reporting.Reporter,
	sources map[string]ManagedSource, spool *core.EventSpool, bus *core.EventBus,
	status *core.StatusTracker, token string) *Server {
	return &Server{store: store, rules: rules, reporter: reporter, sources: sources, spool: spool, bus: bus, status: status, token: token}
}

func (s *Server) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.token != "" && r.Header.Get("X-Internal-Token") != s.token {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/health", s.requireToken(s.handleHealth))
	mux.HandleFunc("GET /internal/llm-health", s.requireToken(s.handleLLMHealth))
	mux.HandleFunc("POST /internal/reload", s.requireToken(s.handleReload))
	mux.HandleFunc("POST /internal/reports/generate", s.requireToken(s.handleGenerateReport))
	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]any{
		"state":          s.status.Get(),
		"events_dropped": s.bus.DroppedCount(),
		"events_spilled": s.spool.SpilledCount(),
		"spool_backlog":  s.spool.BacklogSize(),
	})
	if err != nil {
		return
	}
}

func (s *Server) handleLLMHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(s.reporter.Health(ctx))
	if err != nil {
		return
	}
}

// handleReload reconciles every registered source type's tailed paths and
// the rule engine against whatever's currently in the database - called by
// the controller right after any write to the sources or rules tables.
func (s *Server) handleReload(w http.ResponseWriter, _ *http.Request) {
	for _, sourceType := range ingest.Registered() {
		managed, ok := s.sources[sourceType]
		if !ok {
			continue
		}

		wanted, err := s.store.ListSources(sourceType)
		if err != nil {
			continue
		}
		wantedSet := map[string]bool{}
		for _, c := range wanted {
			wantedSet[c.Path] = true
		}

		current := managed.Paths()
		currentSet := map[string]bool{}
		for _, p := range current {
			currentSet[p] = true
		}

		for path := range wantedSet {
			if !currentSet[path] {
				managed.AddPath(path)
			}
		}
		for path := range currentSet {
			if !wantedSet[path] {
				managed.RemovePath(path)
			}
		}

		cfgs, err := s.store.ListRules(sourceType)
		if err != nil {
			continue
		}
		defs := make([]core.RuleDef, len(cfgs))
		for i, c := range cfgs {
			defs[i] = core.RuleDef{ID: c.ID, Pattern: c.Pattern, Severity: c.Severity, EventType: c.EventType, Priority: c.Priority}
		}
		s.rules.Load(sourceType, defs)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGenerateReport(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()

	id, err := s.reporter.Generate(ctx, time.Hour)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if id == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"id": id}); err != nil {
		return
	}
}
