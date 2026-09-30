package workerclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGenerateReportSurvivesSlowResponse(t *testing.T) {
	// Simulates a worker whose LLM call takes 4 seconds — longer than the
	// old 3s client-level timeout that caused report generation to always
	// fail. If this test passes, that regression can't silently come back.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(4 * time.Second)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"id": 42}`))
	}))
	defer server.Close()

	client := New(server.URL, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	id, err := client.GenerateReport(ctx)
	if err != nil {
		t.Fatalf("expected success despite slow response, got error: %v", err)
	}
	if id != 42 {
		t.Errorf("expected id 42, got %d", id)
	}
}

func TestHealthFailsFastOnSlowServer(t *testing.T) {
	// Health checks should NOT wait for a slow server — they should time
	// out at the client's short 3s ceiling regardless of context.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer server.Close()

	client := New(server.URL, "")

	start := time.Now()
	status := client.Health(context.Background())
	elapsed := time.Since(start)

	if status.Reachable {
		t.Error("expected unreachable given the timeout")
	}
	if elapsed > 4*time.Second {
		t.Errorf("health check took too long: %v (should fail around 3s)", elapsed)
	}
}
