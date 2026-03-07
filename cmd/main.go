package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/trolleksii/argocd-diff-reporter/internal/argo"
	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/coordinator"
	"github.com/trolleksii/argocd-diff-reporter/internal/githubauth"
	"github.com/trolleksii/argocd-diff-reporter/internal/gitrepomanager"
	"github.com/trolleksii/argocd-diff-reporter/internal/logging"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/server"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/mock"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/ui"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/webhook"
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

	natsSrv, err := nats.New(cfg.Nats, ctx, logger)
	if err != nil {
		logger.Error("failed to start NATS", "error", err)
		os.Exit(1)
	}
	b := natsSrv.NewBus()
	st := natsSrv.NewStore()

	if err := b.EnsureStream(ctx, "pr-diffs", []string{
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
		webhook.Route(cfg.Webhook, logger, b),
		ui.Route(logger, st),
		mock.Route(logger, b),
	)

	auth, err := githubauth.New(ctx, logger, cfg.Github)
	if err != nil {
		logger.Error("failed to create github auth", "error", err)
		os.Exit(1)
	}

	gitWorker := gitrepomanager.New(cfg.Workers.GitWorker, auth, b, logger)
	argoWorker := argo.New(cfg.ArgoCD, b, logger)
	coord := coordinator.NewCoordinator(b, st, logger)

	services := []Worker{
		natsSrv,
		httpSrv,
		gitWorker,
		argoWorker,
		coord,
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

type Worker interface {
	Run(ctx context.Context) error
}
