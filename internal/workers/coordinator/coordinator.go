package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

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
			"helm.manifest.rendered": c.logDetails,
			"helm.manifest.render.failed": c.logDetails,
			"helm.chart.fetch.failed": c.logDetails,
			"git.chart.fetch.failed": c.logDetails,
			"argo.file.parsing.failed": c.logDetails,
			"argo.app.genreation.failed": c.logDetails,
		},
	})
	if err != nil {
		return fmt.Errorf("coordinator: consume: %w", err)
	}
	return nil
}

func (c *Coordinator) logDetails(ctx context.Context, headers map[string]string, _ []byte, ack, nak func() error) {
	c.log.Info("coordinator got a message", "headers", headers)
	ack()
}
