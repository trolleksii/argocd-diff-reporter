package server

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
)

type Server struct {
	log        *slog.Logger
	httpServer *http.Server
}

// MuxOption configures the http.ServeMux.
type MuxOption func(*http.ServeMux, *slog.Logger)

func New(cfg config.ServerConfig, log *slog.Logger, opts ...MuxOption) *Server {
	mux := http.NewServeMux()

	for _, fn := range opts {
		fn(mux, log)
	}

	return &Server{
		log: log.With("component", "server"),
		httpServer: &http.Server{
			Addr:         cfg.Addr,
			Handler:      mux,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
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
