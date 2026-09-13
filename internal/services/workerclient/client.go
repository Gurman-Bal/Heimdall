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
	baseURL string
	token   string
	http    *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{baseURL: baseURL, token: token, http: &http.Client{Timeout: 3 * time.Second}}
}

func (c *Client) do(ctx context.Context, method, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("X-Internal-Token", c.token)
	}
	return c.http.Do(req)
}

type HealthStatus struct {
	State         string `json:"state"`
	EventsDropped int64  `json:"events_dropped"`
	EventsSpilled int64  `json:"events_spilled"`
	SpoolBacklog  int    `json:"spool_backlog"`
	Reachable     bool   `json:"-"`
}

// Health never returns an error - if the worker is unreachable (mid-restart,
// which is expected and fine), Reachable is simply false. Callers show that
// state in the UI instead of failing the whole status request.
func (c *Client) Health(ctx context.Context) HealthStatus {
	resp, err := c.do(ctx, "GET", "/internal/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		return HealthStatus{State: "unreachable"}
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			slog.Error("failed to close storage", "error", err)
		}
	}(resp.Body)

	var h HealthStatus
	err = json.NewDecoder(resp.Body).Decode(&h)
	if err != nil {
		return HealthStatus{}
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
	resp, err := c.do(ctx, "GET", "/internal/llm-health")
	if err != nil || resp.StatusCode != http.StatusOK {
		return LLMHealth{}
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			slog.Error("failed to close storage", "error", err)
		}
	}(resp.Body)

	var h LLMHealth
	err = json.NewDecoder(resp.Body).Decode(&h)
	if err != nil {
		return LLMHealth{}
	}
	return h
}

func (c *Client) Reload(ctx context.Context) error {
	resp, err := c.do(ctx, "POST", "/internal/reload")
	if err != nil {
		return fmt.Errorf("worker unreachable: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			slog.Error("failed to close storage", "error", err)
		}
	}(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker returned %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) GenerateReport(ctx context.Context) (int64, error) {
	resp, err := c.do(ctx, "POST", "/internal/reports/generate")
	if err != nil {
		return 0, fmt.Errorf("worker unreachable: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			slog.Error("failed to close storage", "error", err)
		}
	}(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("worker returned %d", resp.StatusCode)
	}
	var out struct {
		ID int64 `json:"id"`
	}
	err = json.NewDecoder(resp.Body).Decode(&out)
	if err != nil {
		return 0, err
	}
	return out.ID, nil
}
