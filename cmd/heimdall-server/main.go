package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"heimdall/internal/config"
	"heimdall/internal/core"
	"heimdall/internal/ingest"
	_ "heimdall/internal/plugins/minecraft"
	_ "heimdall/internal/plugins/truenas"
	"heimdall/internal/serverapi"
	"heimdall/internal/services/reporting"
	"heimdall/internal/storage"
)

func seedDefaultRules(
	store *storage.Store,
	sourceType string,
) {
	existing, err := store.ListRules(sourceType)
	if err != nil {
		slog.Error(
			"failed to check existing rules",
			"type",
			sourceType,
			"error",
			err,
		)
		return
	}

	if len(existing) > 0 {
		return
	}

	defaults := ingest.DefaultRules(sourceType)

	for i, r := range defaults {
		if _, err := store.AddRule(
			sourceType,
			r.Pattern,
			r.Severity,
			r.EventType,
			(i+1)*10,
		); err != nil {
			slog.Error(
				"failed to seed rule",
				"type",
				sourceType,
				"error",
				err,
			)
		}
	}

	if len(defaults) > 0 {
		slog.Info(
			"seeded default rules",
			"type",
			sourceType,
			"count",
			len(defaults),
		)
	}
}

func loadRules(
	store *storage.Store,
	engine *core.RuleEngine,
	sourceType string,
) {
	cfgs, err := store.ListRules(sourceType)
	if err != nil {
		slog.Error(
			"failed to load rules",
			"type",
			sourceType,
			"error",
			err,
		)
		return
	}

	defs := make([]core.RuleDef, len(cfgs))

	for i, c := range cfgs {
		defs[i] = core.RuleDef{
			ID:        c.ID,
			Pattern:   c.Pattern,
			Severity:  c.Severity,
			EventType: c.EventType,
			Priority:  c.Priority,
		}
	}

	if errs := engine.Load(sourceType, defs); len(errs) > 0 {
		for _, e := range errs {
			slog.Error(
				"rule failed to compile",
				"type",
				sourceType,
				"error",
				e,
			)
		}
	}
}

func main() {
	slog.SetDefault(
		slog.New(
			slog.NewTextHandler(
				os.Stdout,
				&slog.HandlerOptions{
					Level: slog.LevelInfo,
				},
			),
		),
	)

	cfg := config.Load()

	store, err := storage.New(cfg.DBPath)
	if err != nil {
		slog.Error(
			"failed to open storage",
			"error",
			err,
		)
		os.Exit(1)
	}

	defer func() {
		if err := store.Close(); err != nil {
			slog.Error(
				"failed to close storage",
				"error",
				err,
			)
		}
	}()

	slog.Info(
		"worker: storage opened",
		"path",
		cfg.DBPath,
	)

	if existing, _ := store.ListSources("truenas"); len(existing) == 0 {
		for _, p := range []string{
			cfg.DefaultLogDir + "/messages",
			cfg.DefaultLogDir + "/auth.log",
			cfg.DefaultLogDir + "/middlewared.log",
		} {
			if _, err := store.AddSource("truenas", p); err != nil {
				slog.Error(
					"failed to add default source",
					"path",
					p,
					"error",
					err,
				)
				return
			}
		}
	}

	ruleEngine := core.NewRuleEngine()
	bus := core.NewEventBus()

	pending := core.NewPendingEventManager(
		store,
		bus,
		10*time.Second,
		5,
	)

	spool, err := core.NewEventSpool(
		cfg.EventBufferSize,
		cfg.SpoolDir,
		pending.ProcessBatch,
	)
	if err != nil {
		slog.Error(
			"failed to initialize event spool",
			"error",
			err,
		)
		os.Exit(1)
	}

	status := core.NewStatusTracker()

	scheduler := core.NewScheduler(
		spool,
		5*time.Second,
	)

	managed := map[string]serverapi.ManagedSource{}

	for _, sourceType := range ingest.Registered() {
		seedDefaultRules(store, sourceType)
		loadRules(store, ruleEngine, sourceType)

		cfgs, _ := store.ListSources(sourceType)

		var paths []string
		for _, c := range cfgs {
			paths = append(paths, c.Path)
		}

		src, ok := ingest.New(sourceType, paths, store, ruleEngine)
		if !ok {
			continue
		}

		scheduler.Register(src)
		managed[sourceType] = src

		slog.Info(
			"source type initialized",
			"type",
			sourceType,
			"path_count",
			len(paths),
		)
	}

	reporter := reporting.New(
		store,
		bus,
		reporting.Config{
			OllamaURL: cfg.OllamaURL,
			Model:     cfg.LLMModel,
		},
	)

	go func() {
		ticker := time.NewTicker(cfg.ReportInterval)
		defer ticker.Stop()

		for range ticker.C {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				90*time.Second,
			)

			if _, err := reporter.Generate(
				ctx,
				cfg.ReportInterval,
			); err != nil {
				slog.Error(
					"scheduled report generation failed",
					"error",
					err,
				)
			}

			cancel()
		}
	}()

	internalSrv := serverapi.New(
		store,
		ruleEngine,
		reporter,
		managed,
		spool,
		bus,
		status,
		cfg.InternalToken,
	)

	go func() {
		slog.Info(
			"worker internal api starting",
			"addr",
			cfg.InternalAddr,
		)

		if err := internalSrv.Start(cfg.InternalAddr); err != nil {
			slog.Error(
				"worker internal api failed",
				"error",
				err,
			)
			os.Exit(1)
		}
	}()

	stop := make(chan struct{})

	sig := make(chan os.Signal, 1)
	signal.Notify(
		sig,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	go scheduler.Run(stop)

	slog.Info(
		"worker started",
		"registered_types",
		ingest.Registered(),
	)

	<-sig

	status.Set("stopping")
	slog.Warn("worker shutdown signal received")

	close(stop)

	time.Sleep(1500 * time.Millisecond)

	slog.Warn("worker shutdown complete")
}
