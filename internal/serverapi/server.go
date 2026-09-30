package serverapi

import (
	"context"
	"encoding/json"
	"log/slog"
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
			slog.Warn("internal api request rejected: bad or missing token", "path", r.URL.Path, "remote", r.RemoteAddr)
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

	slog.Info("worker internal api listening", "addr", addr, "token_configured", s.token != "")
	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	payload := map[string]any{
		"state":          s.status.Get(),
		"events_dropped": s.bus.DroppedCount(),
		"events_spilled": s.spool.SpilledCount(),
		"spool_backlog":  s.spool.BacklogSize(),
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("failed to encode health response", "error", err)
	}
}

func (s *Server) handleLLMHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()

	health := s.reporter.Health(ctx)

	slog.Info(
		"llm health check completed",
		"reachable", health.Reachable,
		"model", health.Model,
	)

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(health); err != nil {
		slog.Error("failed to encode llm health response", "error", err)
	}
}

// handleReload reconciles every registered source type's tailed paths and
// the rule engine against whatever's currently in the database - called by
// the controller right after any write to the sources or rules tables.
func (s *Server) handleReload(w http.ResponseWriter, _ *http.Request) {
	slog.Info("reload requested")

	for _, sourceType := range ingest.Registered() {
		managed, ok := s.sources[sourceType]
		if !ok {
			slog.Warn("reload: no managed source for registered type, skipping", "type", sourceType)
			continue
		}

		wanted, err := s.store.ListSources(sourceType)
		if err != nil {
			slog.Error("reload: failed to list sources", "type", sourceType, "error", err)
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

		added, removed := 0, 0
		for path := range wantedSet {
			if !currentSet[path] {
				managed.AddPath(path)
				added++
			}
		}
		for path := range currentSet {
			if !wantedSet[path] {
				managed.RemovePath(path)
				removed++
			}
		}
		if added > 0 || removed > 0 {
			slog.Info("reload: paths reconciled", "type", sourceType, "added", added, "removed", removed)
		}

		cfgs, err := s.store.ListRules(sourceType)
		if err != nil {
			slog.Error("reload: failed to list rules", "type", sourceType, "error", err)
			continue
		}
		defs := make([]core.RuleDef, len(cfgs))
		for i, c := range cfgs {
			defs[i] = core.RuleDef{ID: c.ID, Pattern: c.Pattern, Severity: c.Severity, EventType: c.EventType, Priority: c.Priority}
		}
		if errs := s.rules.Load(sourceType, defs); len(errs) > 0 {
			for _, e := range errs {
				slog.Error("reload: rule failed to compile", "type", sourceType, "error", e)
			}
		}
	}

	slog.Info("reload complete")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGenerateReport(w http.ResponseWriter, r *http.Request) {
	slog.Info("report generation requested via internal api")

	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()

	id, err := s.reporter.Generate(ctx, time.Hour)
	if err != nil {
		slog.Error("report generation failed", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if id == 0 {
		slog.Info("report generation returned no new report (nothing since last run)")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"id": id}); err != nil {
		slog.Error("failed to encode report generation response", "error", err)
	}
}
