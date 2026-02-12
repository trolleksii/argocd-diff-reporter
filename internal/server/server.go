package server

import (
	"context"
	"net/http"
	"time"

	"log/slog"

	"github.com/trolleksii/argocd-diff-reporter/internal/registry"
	"github.com/trolleksii/argocd-diff-reporter/internal/config"
)

type Server struct {
	log        *slog.Logger
	httpServer *http.Server
	mux        *http.ServeMux
	registry   *registry.Registry
}

func NewServer(cfg config.ServerConfig, log *slog.Logger, registry *registry.Registry) *Server {
	mux := http.NewServeMux()

	return &Server{
		log: log.With("component", "server"),
		httpServer: &http.Server{
			Addr:         cfg.Addr,
			Handler:      mux,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		mux: mux,
		registry: registry,
	}
}

func (s *Server) RegisterHandlers(handlers map[string]http.Handler) {
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	for pattern, handler := range handlers {
		s.mux.Handle(pattern, handler)
	}
}

func (s *Server) Run(ctx context.Context) error {
	s.log.Info("starting HTTP server", "addr", s.httpServer.Addr)

	errCh := make(chan error, 1)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.log.Info("shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(shutdownCtx)
	}
}
