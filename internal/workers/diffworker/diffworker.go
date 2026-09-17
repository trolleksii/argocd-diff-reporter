package diffworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/keys"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/reports"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/notifications"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/templates"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/diffworker")

type DiffWorker struct {
	tplCat   templates.Catalog
	bus      *nats.Bus
	store    *nats.Store
	log      *slog.Logger
	notifier *notifications.NotificationServer
}

func New(log *slog.Logger, b *nats.Bus, s *nats.Store, n *notifications.NotificationServer) *DiffWorker {
	return &DiffWorker{
		tplCat:   templates.NewCatalog(),
		bus:      b,
		store:    s,
		notifier: n,
		log:      log.With("component", "diffworker"),
	}
}

func (w *DiffWorker) Run(ctx context.Context) error {
	w.log.InfoContext(ctx, "starting diffworker...")
	err := w.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "diffworker",
		MaxDeliver:  3,
		AckWait:     10 * time.Second,
		Concurrency: 10,
		Routes: []nats.Route{
			{Subjects: []string{subjects.CoordinatorAppReady}, Handler: w.prepareDiffReport},
		},
	})
	if err != nil {
		return fmt.Errorf("diffworker: consume: %w", err)
	}
	return nil
}

func (w *DiffWorker) prepareDiffReport(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"prepareDiffReport",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers.Get("pr.owner")
	repo := headers.Get("pr.repo")
	number := headers.Get("pr.number")
	baseSha := headers.Get("pr.sha.base")
	headSha := headers.Get("pr.sha.head")
	appName := headers.Get("app.name")
	origin := headers.Get("app.origin")
	runId := headers.Get("RunId")
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("pr.baseSha", baseSha),
		attribute.String("pr.headSha", headSha),
		attribute.String("app.name", appName),
		attribute.String("app.origin", origin),
	)
	w.log.DebugContext(ctx, "new coordinator.app.ready event", "appName", appName)
	headers.Set(keys.MsgIDHeader, keys.MsgIDReport(owner, repo, number, runId, origin, appName))

	baseLoc := headers.Get("manifest.base.location")
	headLoc := headers.Get("manifest.head.location")
	var data string
	var err error
	if baseLoc == "" {
		data = "---"
	} else {
		data, err = nats.GetObject[string](ctx, w.store, baseLoc)
		if err != nil {
			w.log.ErrorContext(ctx, "failed to find base manifest", "error", err, "id", baseLoc)
			nak()
			return
		}
	}
	fromDoc, err := reports.LoadManifest(appName, []byte(data))
	if err != nil {
		w.log.ErrorContext(ctx, "failed to load base manifest", "error", err, "id", baseLoc)
		nak()
		return
	}

	if headLoc == "" {
		data = "---"
	} else {
		data, err = nats.GetObject[string](ctx, w.store, headLoc)
		if err != nil {
			w.log.ErrorContext(ctx, "failed to find head manifest", "error", err, "id", headLoc)
			nak()
			return
		}
	}
	toDoc, err := reports.LoadManifest(appName, []byte(data))
	if err != nil {
		w.log.ErrorContext(ctx, "failed to load head manifest", "error", err, "id", headLoc)
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
	key := keys.Report(owner, repo, number, baseSha, headSha, origin, appName)
	reports.WriteDiffReport(w.tplCat, fromDoc, toDoc, excludedPaths, &report)
	if err := w.store.StoreObject(ctx, key, report); err != nil {
		w.log.ErrorContext(ctx, "failed to store report", "error", err)
	}
	headers.Set("report.id", key)
	d, err := nats.Marshal(report.DiffStats)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to marshal diffstats message", "error", err)
		return
	}
	w.bus.Publish(ctx, subjects.DiffReportGenerated, headers, d)
	nk := fmt.Sprintf("report:%s.%s.%s.%s.%s", owner, repo, number, origin, appName)
	w.notifier.Notify(nk, fmt.Sprintf("%s:%s:%s:%s", baseSha, headSha, origin, appName))
	span.SetStatus(codes.Ok, "")
	ack()
}
