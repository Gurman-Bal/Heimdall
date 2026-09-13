package api

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"heimdall/internal/auth"
	"heimdall/internal/core"
	"heimdall/internal/services/dockerctl"
	"heimdall/internal/services/workerclient"
	"heimdall/internal/storage"
)

//go:embed web/*
var webFS embed.FS

type Server struct {
	bus           *core.EventBus
	store         *storage.Store
	auth          *auth.Store
	sessions      *auth.SessionManager
	dockerctl     *dockerctl.Controller
	worker        *workerclient.Client
	activityLog   *core.ActivityLog
	status        *core.StatusTracker
	selfContainer string
}

func New(
	bus *core.EventBus,
	store *storage.Store,
	authStore *auth.Store,
	sessions *auth.SessionManager,
	ctl *dockerctl.Controller,
	worker *workerclient.Client,
	activityLog *core.ActivityLog,
	status *core.StatusTracker,
	selfContainer string,
) *Server {
	return &Server{
		bus:           bus,
		store:         store,
		auth:          authStore,
		sessions:      sessions,
		dockerctl:     ctl,
		worker:        worker,
		activityLog:   activityLog,
		status:        status,
		selfContainer: selfContainer,
	}
}

func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()

	// Authentication endpoints are public.
	mux.HandleFunc("POST /api/auth/login", s.handleLogin)
	mux.HandleFunc("POST /api/auth/logout", s.handleLogout)

	// Everything else requires an authenticated session.
	protected := http.NewServeMux()

	// Events / streaming.
	protected.HandleFunc("GET /api/events", s.handleEvents)
	protected.HandleFunc("GET /api/events/noise", s.handleEventNoise)
	protected.HandleFunc("GET /api/stream", s.handleStream)

	// Sources.
	protected.HandleFunc("GET /api/sources", s.handleListSources)
	protected.HandleFunc("GET /api/source-types", s.handleSourceTypes)
	protected.HandleFunc("POST /api/sources", s.handleAddSource)
	protected.HandleFunc("DELETE /api/sources/{id}", s.handleDeleteSource)

	// Rules.
	protected.HandleFunc("GET /api/rules", s.handleListRules)
	protected.HandleFunc("POST /api/rules", s.handleAddRule)
	protected.HandleFunc("DELETE /api/rules/{id}", s.handleDeleteRule)

	// Reports.
	protected.HandleFunc("GET /api/reports", s.handleListReports)
	protected.HandleFunc("GET /api/reports/{id}", s.handleGetReport)
	protected.HandleFunc("POST /api/reports/generate", s.handleGenerateReport)

	// Activity.
	protected.HandleFunc("GET /api/activity", s.handleActivity)

	// System.
	protected.HandleFunc("GET /api/system/status", s.handleSystemStatus)
	protected.HandleFunc("GET /api/system/containers", s.handleListContainers)
	protected.HandleFunc(
		"POST /api/system/containers/{name}/{action}",
		s.handleContainerAction,
	)
	protected.HandleFunc("POST /api/system/password", s.handleChangePassword)
	protected.HandleFunc("GET /api/system/settings", s.handleGetSettings)
	protected.HandleFunc("PUT /api/system/settings", s.handleUpdateSettings)

	mux.Handle("/api/", s.requireSession(protected))

	// Embedded frontend.
	static, err := fs.Sub(webFS, "web")
	if err != nil {
		return fmt.Errorf("failed to load embedded web assets: %w", err)
	}

	mux.Handle("/", http.FileServer(http.FS(static)))

	return http.ListenAndServe(addr, mux)
}

// -----------------------------------------------------------------------------
// Events
// -----------------------------------------------------------------------------

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	events, err := s.store.RecentEvents(200)
	if err != nil {
		http.Error(w, "failed to load events", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(events)
}

func (s *Server) handleEventNoise(w http.ResponseWriter, r *http.Request) {
	noise, err := s.store.ListNoise(200)
	if err != nil {
		http.Error(w, "failed to load noise", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(noise)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(
			w,
			"streaming unsupported",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := s.bus.Subscribe(5000)

	for {
		select {
		case e := <-ch:
			data, err := json.Marshal(e)
			if err != nil {
				slog.Warn("failed to marshal stream event", "error", err)
				continue
			}

			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}

			flusher.Flush()

		case <-r.Context().Done():
			return
		}
	}
}

// -----------------------------------------------------------------------------
// Sources
// -----------------------------------------------------------------------------

func (s *Server) handleListSources(w http.ResponseWriter, r *http.Request) {
	sourceType := r.URL.Query().Get("type")

	list, err := s.store.ListSources(sourceType)
	if err != nil {
		http.Error(
			w,
			"failed to load sources",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}

func (s *Server) handleSourceTypes(w http.ResponseWriter, r *http.Request) {
	// Source implementations now live in the worker, so the controller
	// no longer has a map[string]ManagedSource to enumerate.
	//
	// Build the list from the configured sources in the database.
	// This keeps the endpoint useful without coupling the API to worker
	// implementation details.
	sources, err := s.store.ListSources("")
	if err != nil {
		http.Error(
			w,
			"failed to load source types",
			http.StatusInternalServerError,
		)
		return
	}

	seen := make(map[string]struct{})

	for _, source := range sources {
		seen[source.Type] = struct{}{}
	}

	types := make([]string, 0, len(seen))
	for sourceType := range seen {
		types = append(types, sourceType)
	}

	sort.Strings(types)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(types)
}

type addSourceRequest struct {
	Type string `json:"type"`
	Path string `json:"path"`
}

func (s *Server) handleAddSource(w http.ResponseWriter, r *http.Request) {
	var req addSourceRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(
			w,
			"invalid request body",
			http.StatusBadRequest,
		)
		return
	}

	if req.Type == "" || req.Path == "" {
		http.Error(
			w,
			"type and path are required",
			http.StatusBadRequest,
		)
		return
	}

	// The controller no longer owns source implementations.
	// It persists configuration and tells the worker to reconcile.
	id, err := s.store.AddSource(req.Type, req.Path)
	if err != nil {
		http.Error(
			w,
			"failed to save source",
			http.StatusInternalServerError,
		)
		return
	}

	if err := s.worker.Reload(r.Context()); err != nil {
		// The DB write succeeded. A reload failure does not mean that the
		// configuration should be rolled back; the worker can reconcile
		// again later.
		slog.Warn(
			"worker reload after add-source failed",
			"error", err,
			"type", req.Type,
			"path", req.Path,
			"id", id,
		)
	}

	slog.Info(
		"source added via api",
		"type", req.Type,
		"path", req.Path,
		"id", id,
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":   id,
		"type": req.Type,
		"path": req.Path,
	})
}

func (s *Server) handleDeleteSource(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(
			w,
			"invalid id",
			http.StatusBadRequest,
		)
		return
	}

	// Validate that the source exists before deleting it.
	_, err = s.store.GetSource(id)
	if err != nil {
		http.Error(
			w,
			"source not found",
			http.StatusNotFound,
		)
		return
	}

	if err := s.store.RemoveSource(id); err != nil {
		http.Error(
			w,
			"failed to remove source",
			http.StatusInternalServerError,
		)
		return
	}

	if err := s.worker.Reload(r.Context()); err != nil {
		slog.Warn(
			"worker reload after delete-source failed",
			"error", err,
			"id", id,
		)
	}

	slog.Info(
		"source removed via api",
		"id", id,
	)

	w.WriteHeader(http.StatusNoContent)
}

// -----------------------------------------------------------------------------
// Rules
// -----------------------------------------------------------------------------

func (s *Server) handleListRules(w http.ResponseWriter, r *http.Request) {
	sourceType := r.URL.Query().Get("type")

	list, err := s.store.ListRules(sourceType)
	if err != nil {
		http.Error(
			w,
			"failed to load rules",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}

type addRuleRequest struct {
	Type      string `json:"type"`
	Pattern   string `json:"pattern"`
	Severity  string `json:"severity"`
	EventType string `json:"event_type"`
	Priority  int    `json:"priority"`
}

func (s *Server) handleAddRule(w http.ResponseWriter, r *http.Request) {
	var req addRuleRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(
			w,
			"invalid request body",
			http.StatusBadRequest,
		)
		return
	}

	if req.Type == "" ||
		req.Pattern == "" ||
		req.Severity == "" ||
		req.EventType == "" {
		http.Error(
			w,
			"type, pattern, severity, and event_type are required",
			http.StatusBadRequest,
		)
		return
	}

	// Validate the regex before persisting it.
	if _, err := regexp.Compile(req.Pattern); err != nil {
		http.Error(
			w,
			fmt.Sprintf("invalid regex pattern: %v", err),
			http.StatusBadRequest,
		)
		return
	}

	id, err := s.store.AddRule(
		req.Type,
		req.Pattern,
		req.Severity,
		req.EventType,
		req.Priority,
	)
	if err != nil {
		http.Error(
			w,
			"failed to save rule",
			http.StatusInternalServerError,
		)
		return
	}

	// Rules are owned by the worker's RuleEngine now.
	if err := s.worker.Reload(r.Context()); err != nil {
		slog.Warn(
			"worker reload after add-rule failed",
			"error", err,
			"type", req.Type,
			"id", id,
		)
	}

	slog.Info(
		"rule added via api",
		"type", req.Type,
		"pattern", req.Pattern,
		"id", id,
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": id,
	})
}

func (s *Server) handleDeleteRule(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(
			w,
			"invalid id",
			http.StatusBadRequest,
		)
		return
	}

	// Validate that the rule exists before deleting it.
	_, err = s.store.GetRule(id)
	if err != nil {
		http.Error(
			w,
			"rule not found",
			http.StatusNotFound,
		)
		return
	}

	if err := s.store.RemoveRule(id); err != nil {
		http.Error(
			w,
			"failed to remove rule",
			http.StatusInternalServerError,
		)
		return
	}

	// The worker reloads the complete rule configuration from SQLite.
	if err := s.worker.Reload(r.Context()); err != nil {
		slog.Warn(
			"worker reload after delete-rule failed",
			"error", err,
			"id", id,
		)
	}

	slog.Info(
		"rule removed via api",
		"id", id,
	)

	w.WriteHeader(http.StatusNoContent)
}

// -----------------------------------------------------------------------------
// Reports
// -----------------------------------------------------------------------------

func (s *Server) handleListReports(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListReports(50)
	if err != nil {
		http.Error(
			w,
			"failed to load reports",
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(list)
}

func (s *Server) handleGetReport(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		http.Error(
			w,
			"invalid id",
			http.StatusBadRequest,
		)
		return
	}

	report, err := s.store.GetReport(id)
	if err != nil {
		http.Error(
			w,
			"report not found",
			http.StatusNotFound,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(report)
}

func (s *Server) handleGenerateReport(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(
		r.Context(),
		90*time.Second,
	)
	defer cancel()

	// Report generation now happens inside the worker.
	id, err := s.worker.GenerateReport(ctx)
	if err != nil {
		http.Error(
			w,
			fmt.Sprintf("report generation failed: %v", err),
			http.StatusServiceUnavailable,
		)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": id,
	})
}

// -----------------------------------------------------------------------------
// System status
// -----------------------------------------------------------------------------

func (s *Server) handleSystemStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	var workerHealth workerclient.HealthStatus
	var llmHealth workerclient.LLMHealth

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		workerHealth = s.worker.Health(ctx)
	}()
	go func() {
		defer wg.Done()
		llmHealth = s.worker.LLMHealth(ctx)
	}()
	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"controller_state": "running",
		"worker":           workerHealth,
		"llm":              llmHealth,
		"self_container":   s.selfContainer,
	})
}
