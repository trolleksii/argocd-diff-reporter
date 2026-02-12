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
	"github.com/trolleksii/argocd-diff-reporter/internal/registry"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/server"
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

	registry := registry.NewRegistry()
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	natsSvc, err := nats.New(cfg.Nats, ctx, logger, registry)
	if err != nil {
		logger.Error("failed to start NATS", "error", err)
		os.Exit(1)
	}

	services := []registry.Service{natsSvc}
	switch cfg.Target {
	case "all":
		services = append(services,
			server.NewServer(cfg.Server, logger, registry),
		)
	case "server":
		services = append(services,
			server.NewServer(cfg.Server, logger, registry),
		)
	case "repoworker":
		// TBD
	default:
		logger.Error("unknown module", "target", cfg.Target)
		os.Exit(1)
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, svc := range services {
		g.Go(func() error { return svc.Run(gCtx) })
	}
	if err := g.Wait(); err != nil {
		logger.Error("app error", "error", err)
		os.Exit(1)
	}
}
