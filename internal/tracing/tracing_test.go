package tracing

import (
	"context"
	"log/slog"
	"testing"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
)

func TestInitTracerUnsupportedProtocol(t *testing.T) {
	_, err := InitTracer(context.Background(), config.TracingConfig{
		Endpoint: "localhost:4317",
		Protocol: "udp",
	}, slog.Default())
	if err == nil {
		t.Fatal("expected error for unsupported protocol")
	}
}
