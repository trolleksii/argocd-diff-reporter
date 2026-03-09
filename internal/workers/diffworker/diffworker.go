package diffworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/diffworker")

type DiffWorker struct {
	bus   *nats.Bus
	store *nats.Store
	log   *slog.Logger
}

func New(log *slog.Logger, b *nats.Bus, s *nats.Store) *DiffWorker {
	return &DiffWorker{
		bus:   b,
		store: s,
		log:   log.With("component", "diffworker"),
	}
}

func (c *DiffWorker) Run(ctx context.Context) error {
	c.log.Info("starting diffworker...")
	err := c.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "diffworker",
		MaxDeliver: 3,
		AckWait:    10 * time.Second,
		Concurrency: 4,
		Handlers: map[string]nats.Handler{
			"coordinator.app.ready": c.handleDiffReport,
		},
	})
	if err != nil {
		return fmt.Errorf("diffworker: consume: %w", err)
	}
	return nil
}

func (c *DiffWorker) handleDiffReport(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleRenderedManifest",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()
	number := headers.Get("prNum")
	baseSha := headers.Get("baseSha")
	headSha := headers.Get("headSha")
	fileName := headers.Get("fileName")
	appName := headers.Get("application")
	headers.Set("Nats-Msg-Id", baseSha+headSha)
	oldManifest := headers["before"]
	newManifest := headers["after"]
	reportId := fmt.Sprintf("", number, baseSha, headSha, fileName, appName)
	c.log.Info("report", "id", reportId, "old", oldManifest, "new", newManifest)
	span.SetStatus(codes.Ok, "")
	ack()
}
