package workerclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Client struct {
	baseURL    string
	token      string
	healthHTTP *http.Client // short timeout — health checks must fail fast
	longHTTP   *http.Client // no client-level timeout — governed by the caller's context instead
}

func New(baseURL, token string) *Client {
	return &Client{
		baseURL:    baseURL,
		token:      token,
		healthHTTP: &http.Client{Timeout: 3 * time.Second},
		longHTTP:   &http.Client{}, // deliberately no Timeout field — see doLong
	}
}

func (c *Client) newRequest(ctx context.Context, method, path string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("X-Internal-Token", c.token)
	}
	return req, nil
}

// doShort is for health-style checks: bounded by the client's own 3s
// timeout regardless of what the caller's context allows, because these
// should fail fast rather than hang the status endpoint.
func (c *Client) doShort(ctx context.Context, method, path string) (*http.Response, error) {
	req, err := c.newRequest(ctx, method, path)
	if err != nil {
		return nil, err
	}
	return c.healthHTTP.Do(req)
}

// doLong is for calls that can legitimately take a while (report
// generation via a local LLM). No client-level timeout — the request lives
// exactly as long as the caller's context.Context deadline allows, so a
// caller that passes a 90s context actually gets 90s, not silently 3s.
func (c *Client) doLong(ctx context.Context, method, path string) (*http.Response, error) {
	req, err := c.newRequest(ctx, method, path)
	if err != nil {
		return nil, err
	}
	return c.longHTTP.Do(req)
}

type HealthStatus struct {
	State         string `json:"state"`
	EventsDropped int64  `json:"events_dropped"`
	EventsSpilled int64  `json:"events_spilled"`
	SpoolBacklog  int    `json:"spool_backlog"`
	Reachable     bool   `json:"-"`
}

func (c *Client) Health(ctx context.Context) HealthStatus {
	resp, err := c.doShort(ctx, "GET", "/internal/health")
	if err != nil {
		slog.Warn("worker health check failed", "error", err)
		return HealthStatus{State: "unreachable"}
	}
	defer closeBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		slog.Warn("worker health check returned non-200", "status", resp.StatusCode)
		return HealthStatus{State: "unreachable"}
	}

	var h HealthStatus
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		slog.Warn("failed to decode worker health response", "error", err)
		return HealthStatus{State: "unreachable"}
	}
	h.Reachable = true
	return h
}

type LLMHealth struct {
	Reachable    bool      `json:"reachable"`
	Model        string    `json:"model"`
	LastReportAt time.Time `json:"last_report_at,omitzero"`
}

func (c *Client) LLMHealth(ctx context.Context) LLMHealth {
	resp, err := c.doShort(ctx, "GET", "/internal/llm-health")
	if err != nil {
		slog.Warn("llm health check failed", "error", err)
		return LLMHealth{}
	}
	defer closeBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		slog.Warn("llm health check returned non-200", "status", resp.StatusCode)
		return LLMHealth{}
	}

	var h LLMHealth
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		slog.Warn("failed to decode llm health response", "error", err)
		return LLMHealth{}
	}
	return h
}

// Reload can involve reconciling file offsets and recompiling rules — not
// as slow as an LLM call, but not guaranteed instant either. Uses doLong so
// it isn't cut off by the 3s health-check timeout.
func (c *Client) Reload(ctx context.Context) error {
	resp, err := c.doLong(ctx, "POST", "/internal/reload")
	if err != nil {
		slog.Warn("worker reload request failed", "error", err)
		return fmt.Errorf("worker unreachable: %w", err)
	}
	defer closeBody(resp.Body)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Warn("worker reload returned non-200", "status", resp.StatusCode, "body", string(body))
		return fmt.Errorf("worker returned %d", resp.StatusCode)
	}
	return nil
}

// GenerateReport is the call that was silently broken: it needs however
// long the LLM takes, governed entirely by the context the caller passes
// in (handleGenerateReport gives it 90s) — never cut short by a
// client-level timeout of a few seconds.
func (c *Client) GenerateReport(ctx context.Context) (int64, error) {
	slog.Info("requesting report generation from worker")

	resp, err := c.doLong(ctx, "POST", "/internal/reports/generate")
	if err != nil {
		slog.Error("report generation request to worker failed", "error", err)
		return 0, fmt.Errorf("worker unreachable: %w", err)
	}
	defer closeBody(resp.Body)

	// HTTP 204 No Content means no new events were available to generate a report
	if resp.StatusCode == http.StatusNoContent {
		slog.Info("worker skipped report generation: no new events")
		return 0, nil
	}

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		slog.Error("failed to read report generation response body", "error", readErr)
		return 0, readErr
	}

	if resp.StatusCode != http.StatusOK {
		slog.Error("worker rejected report generation request", "status", resp.StatusCode, "body", string(body))
		return 0, fmt.Errorf("worker returned %d: %s", resp.StatusCode, string(body))
	}

	var out struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		slog.Error("failed to decode report generation response", "error", err, "body", string(body))
		return 0, err
	}

	slog.Info("report generation completed via worker", "report_id", out.ID)
	return out.ID, nil
}

func closeBody(body io.ReadCloser) {
	if err := body.Close(); err != nil {
		slog.Warn("failed to close response body", "error", err)
	}
}
