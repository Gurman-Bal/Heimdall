package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"
)

var processStartedAt = time.Now()

// performContainerAction is the single entry point for all container
// control. There is no free-text parsing anywhere in this path - action is
// one of exactly three known strings, checked with a switch, nothing else
// can reach dockerctl.
func (s *Server) performContainerAction(ctx context.Context, name, action string) error {
	isSelf := name == s.selfContainer

	switch action {
	case "restart":
		if isSelf {
			s.status.Set("restarting")
		}
		slog.Warn("container restart requested", "target", name)
		err := s.dockerctl.Restart(ctx, name)
		if err != nil && isSelf {
			s.status.Set("running")
		}
		return err

	case "stop":
		if isSelf {
			return fmt.Errorf("cannot stop %s from its own UI - you'd have no way to start it back up from here; run `docker compose start %s` on the host instead", s.selfContainer, s.selfContainer)
		}
		slog.Warn("container stop requested", "target", name)
		return s.dockerctl.Stop(ctx, name)

	case "start":
		slog.Info("container start requested", "target", name)
		err := s.dockerctl.Start(ctx, name)
		if err == nil && isSelf {
			s.status.Set("running")
		}
		return err

	default:
		return fmt.Errorf("unknown action %q - allowed: restart, stop, start", action)
	}
}

func (s *Server) handleListContainers(w http.ResponseWriter, r *http.Request) {
	names := s.dockerctl.Allowed()
	sort.Strings(names)

	type containerInfo struct {
		Name   string `json:"name"`
		IsSelf bool   `json:"is_self"`
	}

	out := make([]containerInfo, len(names))
	for i, n := range names {
		out[i] = containerInfo{Name: n, IsSelf: n == s.selfContainer}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (s *Server) handleContainerAction(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	action := r.PathValue("action")

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := s.performContainerAction(ctx, name, action); err != nil {
		slog.Warn("container action rejected or failed", "name", name, "action", action, "error", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	slog.Info("container action executed", "name", name, "action", action)
	w.WriteHeader(http.StatusOK)
}

type changePasswordRequest struct {
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
}

func (s *Server) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	var req changePasswordRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.auth.ChangePassword(req.CurrentPassword, req.NewPassword); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

//func (s *Server) handleSystemStatus(w http.ResponseWriter, r *http.Request) {
//	types := make([]string, 0, len(s.sources))
//	for t := range s.sources {
//		types = append(types, t)
//	}
//	sort.Strings(types)
//
//	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
//	defer cancel()
//
//	status := map[string]any{
//		"state":            s.status.Get(),
//		"uptime_seconds":   int(time.Since(processStartedAt).Seconds()),
//		"registered_types": types,
//		"events_dropped":   s.bus.DroppedCount(),
//		"events_spilled":   s.spool.SpilledCount(),
//		"spool_backlog":    s.spool.BacklogSize(),
//		"self_container":   s.selfContainer,
//		"llm":              s.reporter.Health(ctx),
//	}
//	w.Header().Set("Content-Type", "application/json")
//	json.NewEncoder(w).Encode(status)
//}

func (s *Server) handleActivity(w http.ResponseWriter, r *http.Request) {
	since := time.Now().Add(-24 * time.Hour)
	if v := r.URL.Query().Get("since"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			since = time.Now().Add(-d)
		}
	}

	entries, err := s.activityLog.Recent(since, 1000)
	if err != nil {
		http.Error(w, "failed to load activity", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

type settingsResponse struct {
	SessionTimeoutSeconds int `json:"session_timeout_seconds"`
}

func (s *Server) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(settingsResponse{
		SessionTimeoutSeconds: int(s.sessions.Timeout().Seconds()),
	})
}

type updateSettingsRequest struct {
	SessionTimeoutSeconds int `json:"session_timeout_seconds"`
}

func (s *Server) handleUpdateSettings(w http.ResponseWriter, r *http.Request) {
	var req updateSettingsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.SessionTimeoutSeconds < 60 {
		http.Error(w, "session timeout must be at least 60 seconds", http.StatusBadRequest)
		return
	}

	if err := s.store.SetSetting("session_timeout_seconds", strconv.Itoa(req.SessionTimeoutSeconds)); err != nil {
		http.Error(w, "failed to save setting", http.StatusInternalServerError)
		return
	}

	s.sessions.SetTimeout(time.Duration(req.SessionTimeoutSeconds) * time.Second)
	slog.Info("session timeout updated", "seconds", req.SessionTimeoutSeconds)
	w.WriteHeader(http.StatusOK)
}
