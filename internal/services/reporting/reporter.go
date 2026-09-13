package reporting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"heimdall/internal/core"
	"heimdall/internal/storage"
)

type Config struct {
	OllamaURL string // e.g. "http://localhost:11434"
	Model     string // e.g. "qwen2.5:0.5b"
}

type Reporter struct {
	store *storage.Store
	bus   *core.EventBus
	cfg   Config
	http  *http.Client
}

type HealthStatus struct {
	Reachable    bool      `json:"reachable"`
	Model        string    `json:"model"`
	LastReportAt time.Time `json:"last_report_at,omitzero"`
}

func New(store *storage.Store, bus *core.EventBus, cfg Config) *Reporter {
	if cfg.OllamaURL == "" {
		cfg.OllamaURL = "http://localhost:11434"
	}
	if cfg.Model == "" {
		cfg.Model = "qwen2.5:0.5b"
	}
	return &Reporter{store: store, bus: bus, cfg: cfg, http: &http.Client{Timeout: 90 * time.Second}}
}

type issue struct {
	Title        string `json:"title"`
	Severity     string `json:"severity"`
	Explanation  string `json:"explanation"`
	SuggestedFix string `json:"suggested_fix"`
}

type llmOutput struct {
	Summary string  `json:"summary"`
	Issues  []issue `json:"issues"`
}

func (r *Reporter) Generate(ctx context.Context, fallbackWindow time.Duration) (int64, error) {
	since, err := r.store.LastReportTime()
	if err != nil {
		return 0, fmt.Errorf("failed to check last report time: %w", err)
	}
	if since.IsZero() {
		since = time.Now().Add(-fallbackWindow)
	}

	events, err := r.store.EventsSince(since)
	if err != nil {
		return 0, fmt.Errorf("failed to load events: %w", err)
	}
	if len(events) == 0 {
		slog.Info("no new events since last report, skipping", "since", since)
		return 0, nil
	}

	prompt, countsLabel := buildPrompt(events)

	summary, issues, err := r.callOllama(ctx, prompt)
	if err != nil {
		return 0, fmt.Errorf("ollama call failed: %w", err)
	}

	issuesJSON, _ := json.Marshal(issues)
	now := time.Now()

	id, err := r.store.SaveReport(storage.ReportRecord{
		GeneratedAt: now,
		PeriodStart: since,
		PeriodEnd:   now,
		EventCount:  len(events),
		Summary:     summary,
		IssuesJSON:  string(issuesJSON),
		Model:       r.cfg.Model,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to save report: %w", err)
	}

	slog.Info("report generated", "id", id, "event_count", len(events), "issue_count", len(issues))

	r.bus.Publish(core.Event{
		Timestamp: now,
		Source:    "heimdall",
		Type:      "report_generated",
		Severity:  "info",
		Message:   fmt.Sprintf("Report #%d generated covering %d events (%s)", id, len(events), countsLabel),
	})

	return id, nil
}

// buildPrompt is deliberately terse - small local models degrade fast with
// long context, so we cap notable lines lower than we would for a hosted
// frontier model and keep instructions short and concrete.
func buildPrompt(events []core.Event) (prompt, countsLabel string) {
	var infoCount, warnCount, critCount int
	var notable []core.Event

	for _, e := range events {
		switch e.Severity {
		case "critical":
			critCount++
			notable = append(notable, e)
		case "warning":
			warnCount++
			notable = append(notable, e)
		default:
			infoCount++
		}
	}

	if len(notable) > 30 {
		notable = notable[len(notable)-30:]
	}

	var b strings.Builder
	_, err := fmt.Fprintf(&b, "Counts: %d info, %d warning, %d critical.\n\n", infoCount, warnCount, critCount)
	if err != nil {
		return "", ""
	}
	b.WriteString("Recent warning/critical log lines:\n")
	for _, e := range notable {
		_, err := fmt.Fprintf(&b, "[%s] %s/%s: %s\n", e.Severity, e.Source, e.Type, truncate(e.Message, 150))
		if err != nil {
			return "", ""
		}
	}

	countsLabel = fmt.Sprintf("%d info / %d warning / %d critical", infoCount, warnCount, critCount)
	return b.String(), countsLabel
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

const systemPrompt = `You monitor a home server. You will get event counts and log lines. Reply with ONLY this JSON, nothing else:
{"summary":"1-2 sentence plain summary","issues":[{"title":"short name","severity":"warning or critical","explanation":"why this is happening","suggested_fix":"one concrete manual step for a human to try"}]}
If nothing is wrong, use an empty issues array. Only report problems that appear in the log lines given.`

type ollamaChatRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaChatMessage `json:"messages"`
	Stream   bool                `json:"stream"`
	Format   string              `json:"format"` // "json" forces valid JSON output
	Options  map[string]any      `json:"options,omitempty"`
}

type ollamaChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaChatResponse struct {
	Message struct {
		Content string `json:"content"`
	} `json:"message"`
}

func (r *Reporter) callOllama(ctx context.Context, prompt string) (summary string, issues []issue, err error) {
	reqBody := ollamaChatRequest{
		Model: r.cfg.Model,
		Messages: []ollamaChatMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: prompt},
		},
		Stream: false,
		Format: "json",
		Options: map[string]any{
			"temperature": 0.2, // small models ramble less at low temperature
		},
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, "POST", r.cfg.OllamaURL+"/api/chat", bytes.NewReader(bodyBytes))
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("content-type", "application/json")

	resp, err := r.http.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("could not reach ollama at %s: %w", r.cfg.OllamaURL, err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("ollama returned %d: %s", resp.StatusCode, string(respBytes))
	}

	var apiResp ollamaChatResponse
	if err := json.Unmarshal(respBytes, &apiResp); err != nil {
		return "", nil, fmt.Errorf("failed to parse ollama response envelope: %w", err)
	}

	text := strings.TrimSpace(apiResp.Message.Content)

	var out llmOutput
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		slog.Warn("ollama response was not valid JSON, storing as raw text", "error", err, "raw", truncate(text, 300))
		return text, nil, nil
	}

	return out.Summary, out.Issues, nil
}

// Health pings Ollama directly (not through a chat completion, which would
// be slow and cost tokens) and reports when the last successful report ran,
// so the dashboard can tell "LLM unreachable" apart from "just hasn't run yet".
func (r *Reporter) Health(ctx context.Context) HealthStatus {
	status := HealthStatus{Model: r.cfg.Model}

	if last, err := r.store.LastReportTime(); err == nil && !last.IsZero() {
		status.LastReportAt = last
	}

	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, "GET", r.cfg.OllamaURL+"/api/tags", nil)
	if err != nil {
		return status
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return status
	}
	defer resp.Body.Close()
	status.Reachable = resp.StatusCode == http.StatusOK
	return status
}
