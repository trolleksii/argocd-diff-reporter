package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/githubauth"
	"github.com/trolleksii/argocd-diff-reporter/internal/logging"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/server"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/mock"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/ui"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/webhook"
	wrk "github.com/trolleksii/argocd-diff-reporter/internal/workers"
	"github.com/trolleksii/argocd-diff-reporter/internal/workers/argo"
	coord "github.com/trolleksii/argocd-diff-reporter/internal/workers/coordinator"
	"github.com/trolleksii/argocd-diff-reporter/internal/workers/gitrepomanager"
	"github.com/trolleksii/argocd-diff-reporter/internal/workers/helmmanager"
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

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	natsSrv, err := nats.New(ctx, cfg.Nats, logger)
	if err != nil {
		logger.Error("failed to start NATS", "error", err)
		os.Exit(1)
	}
	bus := natsSrv.NewBus()
	store := natsSrv.NewStore()

	if err := bus.EnsureStream(ctx, "pr-diffs", []string{
		"webhook.>",
		"git.>",
		"argo.>",
		"helm.>",
		"kustomize.>",
		"dir.>",
		"coordinator.>",
		"diff.>",
	}); err != nil {
		logger.Error("failed to ensure stream", "error", err)
		os.Exit(1)
	}

	httpSrv := server.New(cfg.Server, logger,
		webhook.NewRouteFunc(cfg.Webhook, bus),
		ui.NewRouteFunc(store),
		mock.NewRouteFunc(bus),
	)

	auth, err := githubauth.New(ctx, cfg.Github, logger)
	if err != nil {
		logger.Error("failed to create github auth", "error", err)
		os.Exit(1)
	}

	gitWorker := gitrepomanager.New(cfg.Workers.GitWorker, logger, auth, bus)
	argoWorker := argo.New(cfg.ArgoCD, logger, bus)
	helmWorker := helmmanager.New(cfg.Workers.HelmWorker, logger, bus, store)
	coordinator := coord.New(logger, bus, store)

	workers := []wrk.Worker{
		natsSrv,
		httpSrv,
		gitWorker,
		argoWorker,
		helmWorker,
		coordinator,
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, w := range workers {
		g.Go(func() error { return w.Run(gCtx) })
	}
	if err := g.Wait(); err != nil {
		logger.Error("app error", "error", err)
		os.Exit(1)
	}
}
