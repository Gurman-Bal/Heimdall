package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	DBPath   string
	SpoolDir string
	APIAddr  string

	// ui-only
	AuthUsername           string
	AuthPassword           string
	ControllableContainers []string
	ControllerContainer    string
	SessionTimeout         time.Duration
	WorkerInternalURL      string

	// server-only
	DefaultLogDir      string
	OllamaURL          string
	LLMModel           string
	ReportInterval     time.Duration
	EventBufferSize    int
	BatchSize          int
	BatchFlushInterval time.Duration
	InternalAddr       string
	WorkerContainer    string

	// shared secret between ui and server internal API - not a
	// user-facing credential, just prevents anything else on the docker
	// network from hitting the worker's internal endpoints.
	InternalToken string

	ActivityRetention time.Duration
}

func Load() Config {
	if err := godotenv.Load(); err != nil {
		slog.Info(".env file not found, using environment/defaults")
	}

	cfg := Config{
		DBPath:   getEnv("HEIMDALL_DB_PATH", "./heimdall.db"),
		SpoolDir: getEnv("HEIMDALL_SPOOL_DIR", "./data/spool"),
		APIAddr:  getEnv("HEIMDALL_API_ADDR", ":8080"),

		AuthUsername:        getEnv("HEIMDALL_AUTH_USER", "admin"),
		AuthPassword:        getEnv("HEIMDALL_AUTH_PASS", ""),
		ControllerContainer: getEnv("HEIMDALL_CONTROLLER_CONTAINER", "heimdall-ui"),
		WorkerInternalURL:   getEnv("HEIMDALL_WORKER_URL", "http://heimdall-worker:9090"),

		DefaultLogDir:   getEnv("HEIMDALL_LOG_DIR", "./testlogs"),
		OllamaURL:       getEnv("HEIMDALL_OLLAMA_URL", "http://localhost:11434"),
		LLMModel:        getEnv("HEIMDALL_LLM_MODEL", "qwen2.5:0.5b"),
		InternalAddr:    getEnv("HEIMDALL_INTERNAL_ADDR", ":9090"),
		WorkerContainer: getEnv("HEIMDALL_WORKER_CONTAINER", "heimdall-server"),

		InternalToken: getEnv("HEIMDALL_INTERNAL_TOKEN", ""),
	}

	cfg.ControllableContainers = strings.Split(
		getEnv("HEIMDALL_CONTROLLABLE_CONTAINERS", "heimdall-ui,heimdall-server,heimdall-ollama"), ",")

	cfg.SessionTimeout = getDuration("HEIMDALL_SESSION_TIMEOUT", 30*time.Minute)
	cfg.ActivityRetention = getDuration("HEIMDALL_ACTIVITY_RETENTION", 48*time.Hour)
	cfg.ReportInterval = getDuration("HEIMDALL_REPORT_INTERVAL", time.Hour)
	cfg.BatchFlushInterval = getDuration("HEIMDALL_BATCH_FLUSH_INTERVAL", 500*time.Millisecond)

	cfg.EventBufferSize = getInt("HEIMDALL_EVENT_BUFFER_SIZE", 5000)
	cfg.BatchSize = getInt("HEIMDALL_BATCH_SIZE", 500)

	if cfg.InternalToken == "" {
		slog.Warn("HEIMDALL_INTERNAL_TOKEN not set - controller/worker internal API is unauthenticated on the docker network")
	}

	return cfg
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration env var, using default", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return d
}

func getInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscan(v, &n); err != nil || n <= 0 {
		slog.Warn("invalid int env var, using default", "key", key, "value", v, "default", fallback)
		return fallback
	}
	return n
}
