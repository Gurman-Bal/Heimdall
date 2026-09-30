package reporting

import (
	"strings"
	"testing"
	"time"

	"heimdall/internal/core"
)

func TestBuildPromptCountsAllSeverities(t *testing.T) {
	events := []core.Event{
		{Severity: "info", Source: "truenas", Type: "log", Message: "fine"},
		{Severity: "warning", Source: "truenas", Type: "warning", Message: "degraded"},
		{Severity: "critical", Source: "truenas", Type: "smart_warning", Message: "disk dying"},
	}

	prompt, label := buildPrompt(events)

	if !strings.Contains(prompt, "1 info, 1 warning, 1 critical") {
		t.Errorf("prompt missing expected counts, got: %s", prompt)
	}
	if !strings.Contains(prompt, "disk dying") {
		t.Error("prompt should include the critical event's message")
	}
	if strings.Contains(prompt, "fine") {
		t.Error("prompt should NOT include info-level messages verbatim (only counted)")
	}
	if label != "1 info / 1 warning / 1 critical" {
		t.Errorf("unexpected countsLabel: %s", label)
	}
}

func TestBuildPromptCapsNotableEventsAt30(t *testing.T) {
	var events []core.Event
	for i := 0; i < 50; i++ {
		events = append(events, core.Event{Severity: "warning", Source: "test", Type: "x", Message: "line"})
	}

	prompt, _ := buildPrompt(events)

	count := strings.Count(prompt, "[warning]")
	if count != 30 {
		t.Errorf("expected exactly 30 notable lines in prompt, got %d", count)
	}
}

func TestTruncate(t *testing.T) {
	short := "hello"
	if got := truncate(short, 10); got != short {
		t.Errorf("short string should be unchanged, got %q", got)
	}

	long := strings.Repeat("a", 20)
	got := truncate(long, 5)
	if got != "aaaaa…" {
		t.Errorf("expected truncated string with ellipsis, got %q", got)
	}
}

func TestHealthReturnsUnreachableWithoutOllama(t *testing.T) {
	r := New(nil, nil, Config{OllamaURL: "http://127.0.0.1:1", Model: "test"})
	status := r.Health(nil) // nil context is fine here — Health wraps it with its own timeout regardless
	if status.Reachable {
		t.Error("expected unreachable status for a bogus URL")
	}
}

var _ = time.Second // keep time import if unused elsewhere after edits
