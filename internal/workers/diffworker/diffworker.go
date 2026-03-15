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

func (w *DiffWorker) Run(ctx context.Context) error {
	w.log.Info("starting diffworker...")
	err := w.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "diffworker",
		MaxDeliver:  3,
		AckWait:     10 * time.Second,
		Concurrency: 10,
		Handlers: map[string]nats.Handler{
			"coordinator.app.ready": w.handleDiffReport,
		},
	})
	if err != nil {
		return fmt.Errorf("diffworker: consume: %w", err)
	}
	return nil
}

func (w *DiffWorker) handleDiffReport(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleDiffReport",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	number := headers["pr.number"]
	baseSha := headers["pr.sha.base"]
	headSha := headers["pr.sha.head"]
	appName := headers["app.name"]
	origin := headers["app.origin"]
	w.log.Debug("new coordinator.app.ready event", "appName", appName)
	headers.Set("Nats-Msg-Id", baseSha+headSha+origin+appName)

	data, err := nats.GetObject[string](ctx, w.store, headers["app.from"])
	if err != nil {
		w.log.Error("failed to find from manifest", "error", err, "id", headers["app.from"])
		nak()
		return
	}
	fromDoc, err := reports.LoadManifest(appName, []byte(data))
	if err != nil {
		w.log.Error("failed to load from manifest", "error", err, "id", headers["app.from"])
		nak()
		return
	}

	data, err = nats.GetObject[string](ctx, w.store, headers["app.to"])
	if err != nil {
		w.log.Error("failed to find to manifest", "error", err, "id", headers["app.to"])
		nak()
		return
	}
	toDoc, err := reports.LoadManifest(appName, []byte(data))
	if err != nil {
		w.log.Error("failed to load to manifest", "error", err, "id", headers["app.to"])
		nak()
		return
	}
	report := models.Report{
		Owner:    owner,
		Repo:     repo,
		PRNumber: number,
		BaseSHA:  baseSha,
		HeadSHA:  headSha,
		File:     origin,
		AppName:  appName,
	}

	excludedPaths := []string{"/metadata/labels/helm.sh/chart", "/spec/template/metadata/labels/helm.sh/chart"}
	key := fmt.Sprintf("%s.%s.%s.%s.%s.%s.%s", owner, repo, number, baseSha, headSha, origin, appName)
	reports.WriteDiffReport(w.tplCat, fromDoc, toDoc, excludedPaths, &report)
	if err := w.store.StoreObject(ctx, key, report); err != nil {
		w.log.Error("failed to store report", "error", err)
	}
	headers["report.id"] = key
	d, err := nats.Marshal(report.DiffStats)
	if err != nil {
		w.log.Error("failed to marshal diffstats message", "error", err)
		return
	}
	w.bus.Publish(ctx, "diff.report.generated", headers, d)
	span.SetStatus(codes.Ok, "")
	ack()
}
