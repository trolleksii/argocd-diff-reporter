package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/coordinator")

type Coordinator struct {
	bus   *nats.Bus
	store *nats.Store
	log   *slog.Logger
}

func New(log *slog.Logger, b *nats.Bus, s *nats.Store) *Coordinator {
	return &Coordinator{
		bus:   b,
		store: s,
		log:   log.With("component", "coordinator"),
	}
}

func (c *Coordinator) Run(ctx context.Context) error {
	c.log.Info("starting coordinator...")
	err := c.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "coordinator",
		MaxDeliver: 3,
		AckWait:    10 * time.Second,
		Handlers: map[string]nats.Handler{
			"helm.manifest.rendered":      c.handleRenderedManifest,
			"helm.manifest.render.failed": c.logDetails,
			"helm.chart.fetch.failed":     c.logDetails,
			"git.chart.fetch.failed":      c.logDetails,
			"argo.file.parsing.failed":    c.logDetails,
			"argo.app.genreation.failed":  c.logDetails,
		},
	})
	if err != nil {
		return fmt.Errorf("coordinator: consume: %w", err)
	}
	return nil
}

func (c *Coordinator) logDetails(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	c.log.Info("coordinator got a message", "headers", headers)
	ack()
}

func (c *Coordinator) handleRenderedManifest(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleRenderedManifest",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	number := headers.Get("prNum")
	baseSha := headers.Get("baseSha")
	headSha := headers.Get("headSha")
	ref := headers.Get("ref")
	fileName := headers.Get("fileName")
	appName := headers.Get("application")
	manifestLocation := headers.Get("manifestLocation")
	c.log.Debug("new helm.manifest.rendered event", "appName", appName, "sha", ref)
	baseKey := fmt.Sprintf("%s.%s.%s.%s", number, baseSha, fileName, appName)
	headKey := fmt.Sprintf("%s.%s.%s.%s", number, headSha, fileName, appName)
	headers.Set("Nats-Msg-Id", baseSha+headKey)
	if ref == headSha {
		c.store.SetValue(ctx, headKey, manifestLocation)
		if _, err := nats.GetValue[string](ctx, c.store, baseKey); err == nil {
			headers["before"] = baseKey
			headers["after"] = manifestLocation
			c.bus.Publish(ctx, "coordinator.app.ready", headers, nil)
		}
	} else {
		if _, err := nats.GetValue[string](ctx, c.store, headKey); err == nil {
			headers["before"] = manifestLocation
			headers["after"] = headKey
			c.bus.Publish(ctx, "coordinator.app.ready", headers, nil)
		}
		c.store.SetValue(ctx, baseKey, manifestLocation)
	}
	span.SetStatus(codes.Ok, "")
	ack()
}
