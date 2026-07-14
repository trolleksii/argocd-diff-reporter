package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"time"
)

// PprofServer serves net/http/pprof on a dedicated listener so profiling
// endpoints stay off the public mux and aren't subject to its write timeout
// (CPU/trace profiles stream for 30s+).
type PprofServer struct {
	log        *slog.Logger
	httpServer *http.Server
}

// NewPprof returns a pprof debug server, or nil if addr is empty.
func NewPprof(addr string, log *slog.Logger) *PprofServer {
	if addr == "" {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return &PprofServer{
		log: log.With("component", "pprof"),
		httpServer: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

func (s *PprofServer) Run(ctx context.Context) error {
	s.log.Info("starting pprof server", "addr", s.httpServer.Addr)

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
		s.log.Info("shutting down pprof server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(shutdownCtx)
	}
}
