package testutil

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

// StartNATS starts an embedded NATS server with JetStream enabled and
// in-memory storage. It returns a ready-to-use *nats.Bus, *nats.Store, and a
// cleanup function that drains the connection and shuts down the server.
// Cleanup is also registered via t.Cleanup so it runs automatically when the
// test ends.
func StartNATS(t *testing.T) (*internalnats.Bus, *internalnats.Store, func()) {
	t.Helper()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Use t.TempDir() so each test gets its own isolated JetStream store that
	// is automatically removed when the test ends. An empty StoreDir would
	// cause the NATS server to fall back to os.TempDir(), which is shared
	// across all tests and leads to stream-subject conflicts.
	// DontListen: true is set inside nats.New, so no port is allocated.
	cfg := config.NatsConfig{
		Domain:     "test",
		ServerName: "test-server",
		StoreDir:   t.TempDir(),
	}

	ctx, cancel := context.WithCancel(context.Background())

	n, err := internalnats.New(ctx, cfg, log)
	require.NoError(t, err, "StartNATS: failed to start embedded NATS server")

	// Run the NATS lifecycle in the background; cancelling ctx triggers
	// connection drain and server shutdown.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = n.Run(ctx)
	}()

	cleanup := func() {
		cancel()
		<-done
	}
	t.Cleanup(cleanup)

	return n.NewBus(), n.NewStore(), cleanup
}

func NoopAck() error { return nil }
func NoopNak() error { return nil }

func SubscribeOnce(t *testing.T, bus *internalnats.Bus, subject string) <-chan internalnats.Headers {
	t.Helper()
	hdrCh, _ := subscribeOnce(t, bus, subject, false)
	return hdrCh
}

func SubscribeOnceBody(t *testing.T, bus *internalnats.Bus, subject string) <-chan []byte {
	t.Helper()
	_, bodyCh := subscribeOnce(t, bus, subject, true)
	return bodyCh
}

func SubscribeOnceWithBody(t *testing.T, bus *internalnats.Bus, subject string) (<-chan internalnats.Headers, <-chan []byte) {
	t.Helper()
	return subscribeOnce(t, bus, subject, true)
}

func SubscribeN(t *testing.T, bus *internalnats.Bus, subject string, n int) <-chan internalnats.Headers {
	t.Helper()
	ch := make(chan internalnats.Headers, n)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = bus.Consume(ctx, internalnats.ConsumerConfig{
			Name: fmt.Sprintf("test-subn-%s-%d",
				strings.ReplaceAll(subject, ".", "-"),
				time.Now().UnixNano()),
			Routes: []internalnats.Route{
				{Subjects: []string{subject}, Handler: func(_ context.Context, hdrs internalnats.Headers, _ []byte, ack, _ func() error) {
					_ = ack()
					select {
					case ch <- hdrs:
					default:
					}
				}},
			},
			MaxDeliver: 3,
			AckWait:    5 * time.Second,
		})
	}()
	<-ready
	time.Sleep(20 * time.Millisecond)
	return ch
}

func subscribeOnce(t *testing.T, bus *internalnats.Bus, subject string, captureBody bool) (<-chan internalnats.Headers, <-chan []byte) {
	t.Helper()
	hdrCh := make(chan internalnats.Headers, 1)
	var bodyCh chan []byte
	if captureBody {
		bodyCh = make(chan []byte, 1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ready := make(chan struct{})
	go func() {
		close(ready)
		_ = bus.Consume(ctx, internalnats.ConsumerConfig{
			Name: fmt.Sprintf("test-sub-%s-%d",
				strings.ReplaceAll(subject, ".", "-"),
				time.Now().UnixNano()),
			Routes: []internalnats.Route{
				{Subjects: []string{subject}, Handler: func(_ context.Context, hdrs internalnats.Headers, data []byte, ack, _ func() error) {
					_ = ack()
					select {
					case hdrCh <- hdrs:
					default:
					}
					if bodyCh != nil {
						select {
						case bodyCh <- data:
						default:
						}
					}
				}},
			},
			MaxDeliver: 3,
			AckWait:    5 * time.Second,
		})
	}()
	<-ready
	time.Sleep(20 * time.Millisecond)
	return hdrCh, bodyCh
}
