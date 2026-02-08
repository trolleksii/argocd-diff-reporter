package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/logging"
	"github.com/trolleksii/argocd-diff-reporter/internal/modules"
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

	mm := modules.NewManager()

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch cfg.Target {
	case "all":
		mm.Register("server", server.InitServer(cfg.Server, logger))
	case "server":
		mm.Register("server", server.InitServer(cfg.Server, logger))
	default:
		logger.Error("unknown module", "target", cfg.Target)
		os.Exit(1)
	}

	if err := mm.Run(ctx); err != nil {
		logger.Error("app error", "error", err)
		os.Exit(1)
	}
}
