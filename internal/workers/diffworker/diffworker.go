package diffworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/reports"
	"github.com/trolleksii/argocd-diff-reporter/internal/templates"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/diffworker")

type DiffWorker struct {
	tplCat templates.Catalog
	bus    *nats.Bus
	store  *nats.Store
	log    *slog.Logger
}

func New(log *slog.Logger, b *nats.Bus, s *nats.Store) *DiffWorker {
	return &DiffWorker{
		tplCat: templates.NewCatalog(),
		bus:    b,
		store:  s,
		log:    log.With("component", "diffworker"),
	}
}

func (c *DiffWorker) Run(ctx context.Context) error {
	c.log.Info("starting diffworker...")
	err := c.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "diffworker",
		MaxDeliver:  3,
		AckWait:     10 * time.Second,
		Concurrency: 10,
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
		"handleDiffReport",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	number := headers.Get("prNum")
	baseSha := headers.Get("baseSha")
	headSha := headers.Get("headSha")
	fileName := headers.Get("fileName")
	appName := headers.Get("application")
	c.log.Debug("new coordinator.app.ready event", "appName", appName)
	headers.Set("Nats-Msg-Id", baseSha+headSha)

	data, err := nats.GetObject[string](ctx, c.store, headers["before"])
	if err != nil {
		c.log.Error("failed to find from manifest", "error", err, "id", headers["before"])
		nak()
		return
	}
	fromDoc, err := reports.LoadManifest(appName, []byte(data))
	if err != nil {
		c.log.Error("failed to load from manifest", "error", err, "id", headers["before"])
		nak()
		return
	}

	data, err = nats.GetObject[string](ctx, c.store, headers["after"])
	if err != nil {
		c.log.Error("failed to find to manifest", "error", err, "id", headers["after"])
		nak()
		return
	}
	toDoc, err := reports.LoadManifest(appName, []byte(data))
	if err != nil {
		c.log.Error("failed to load to manifest", "error", err, "id", headers["after"])
		nak()
		return
	}
	report := models.Report{
		PRNumber: number,
		BaseSHA:  baseSha,
		HeadSHA:  headSha,
		File:     fileName,
		AppName:  appName,
	}

	excludedPaths := []string{"/metadata/labels/helm.sh/chart", "/spec/template/metadata/labels/helm.sh/chart"}
	key := fmt.Sprintf("%s.%s.%s.%s.%s", number, baseSha, headSha, fileName, appName)
	reports.WriteDiffReport(c.tplCat, fromDoc, toDoc, excludedPaths, &report)
	c.store.PutReport(ctx, key, report)
	headers["reportId"] = key
	c.bus.Publish(ctx, "diff.report.generated", headers, nil)
	span.SetStatus(codes.Ok, "")
	ack()
}
