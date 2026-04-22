package testutil

import (
	"io"
	"log/slog"
)

// NoopLogger returns a slog.Logger that discards all output. Use it in tests
// where log output is irrelevant and would only add noise.
func NoopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
