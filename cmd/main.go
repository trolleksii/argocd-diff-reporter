package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/trolleksii/argocd-diff-reporter/internal/bus"
	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/logging"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/registry"
	"github.com/trolleksii/argocd-diff-reporter/internal/server"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/ui"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/webhook"
	"github.com/trolleksii/argocd-diff-reporter/internal/store"
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

	rg := registry.NewRegistry()
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	natsSvc, err := nats.New(cfg.Nats, ctx, logger, rg)
	if err != nil {
		logger.Error("failed to start NATS", "error", err)
		os.Exit(1)
	}

	// feels excessive
	js := nats.GetJetstream(rg)
	bus := bus.NewBus(js)

	st := store.NewStore(
		nats.GetKVStore(rg),
		nats.GetObjectStore(rg),
	)

	httpSvc := server.NewServer(cfg.Server, logger,
		webhook.Route(cfg.Webhook, logger, bus),
		ui.Route(logger, st),
	)
	services := []registry.Service{
		natsSvc,
		httpSvc,
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
