package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/logging"
	"github.com/trolleksii/argocd-diff-reporter/internal/modules"
	"github.com/trolleksii/argocd-diff-reporter/internal/server"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

func main() {
	log := slog.Default()
	cfg, err := config.Load("config.yml")
	if err != nil {
		log.Error("failed to load config", "error", err)
	}

	logger, err := logging.New(cfg.Log)
	if err != nil {
		log.Error("failed to setup logger", "error", err)
		os.Exit(1)
	}
	slog.SetDefault(logger)

	registry := modules.NewRegistry()
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	err = nats.SetupNats(cfg.Nats, ctx, registry)
	if err != nil {
		log.Error("failed to start embedded NATS", "error", err)
	}

	var services []modules.Service
	switch cfg.Target {
	case "all":
		services = append(services,
			server.New(cfg.Server, logger),
			// repo worker and other components
		)
	case "server":
		services = append(services,
			server.New(cfg.Server, logger),
		)
	case "repoworker":
		// TBD
	default:
		log.Error("unknown module", "target", cfg.Target)
		os.Exit(1)
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, svc := range services {
		g.Go(func() error { return svc.Run(gCtx) })
	}
	if err := g.Wait(); err != nil {
		log.Error("app error", "error", err)
		os.Exit(1)
	}
}
