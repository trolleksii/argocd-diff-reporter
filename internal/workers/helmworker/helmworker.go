package helmworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/helm"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/helmworker")

type HelmWorker struct {
	cfg   config.HelmWorkerConfig
	log   *slog.Logger
	bus   *nats.Bus
	store *nats.Store
	cache *helm.HelmChartCache
	creds *helm.CredsProvider
}

func New(cfg config.HelmWorkerConfig, log *slog.Logger, b *nats.Bus, s *nats.Store) *HelmWorker {
	return &HelmWorker{
		cfg:   cfg,
		log:   log.With("worker", "helm"),
		bus:   b,
		store: s,
	}
}

func (w *HelmWorker) Run(ctx context.Context) error {
	w.log.Info("starting helm worker...")
	c, err := helm.NewChartDiskCache(w.cfg.ChartCacheDir)
	if err != nil {
		return err
	}
	w.cache = c
	err = w.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "helmworker",
		MaxDeliver:  3,
		AckWait:     3 * time.Second,
		Concurrency: 8,
		Handlers: map[string]nats.Handler{
			subjects.ArgoHelmOCIParsed:   w.handleChartFetch(helm.FetchChartOCI),
			subjects.ArgoHelmHTTPParsed:  w.handleChartFetch(helm.FetchChartHTTPS),
			subjects.ArgoHelmEmptyParsed: w.handleEmptyManifest,
			subjects.HelmChartFetched:    w.handleChartRender,
			subjects.GitChartFetched:     w.handleChartRender,
		},
	})
	if err != nil {
		return fmt.Errorf("helm worker failed to consume: %w", err)
	}
	return nil
}

func (w *HelmWorker) handleChartFetch(fetchFn func(string, string, helm.CredsProvider, *helm.HelmChartCache) (string, error)) nats.Handler {
	return func(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
		ctx, span := tracer.Start(
			otel.GetTextMapPropagator().Extract(ctx, headers),
			"handleChartFetch",
		)
		otel.GetTextMapPropagator().Inject(ctx, headers)
		defer span.End()

		spec, err := nats.Unmarshal[models.AppSpec](data)
		if err != nil {
			w.log.Error("failed to unmarshal pr object", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		span.SetAttributes(
			attribute.String("pr.owner", headers["pr.owner"]),
			attribute.String("pr.repo", headers["pr.repo"]),
			attribute.String("pr.number", headers["pr.number"]),
			attribute.String("app.name", spec.AppName),
			attribute.String("app.origin", headers["app.origin"]),
		)
		w.log.Debug("new argo.helm.(oci|http}.parsed event",
			"app", spec.AppName,
			"repo", spec.Source.RepoURL,
			"chart", spec.Source.ChartName,
			"path", spec.Source.Path,
			"revision", spec.Source.Revision)
		appOrigin := headers["app.origin"]
		chartRef := fmt.Sprintf("%s/%s", spec.Source.RepoURL, spec.Source.ChartName)
		chartLocation, err := fetchFn(chartRef, spec.Source.Revision, nil, w.cache)
		if err != nil {
			headers["error.origin.file"] = appOrigin
			headers["error.origin.app"] = spec.AppName
			headers["error.msg"] = err.Error()
			w.log.Error("failed to feth the chart", "error", err)
			w.bus.Publish(ctx, subjects.HelmChartFetchFailed, headers, nil)
			ack()
			return
		}
		headers["chart.location"] = chartLocation
		w.bus.Publish(ctx, subjects.HelmChartFetched, headers, data)
		span.SetStatus(codes.Ok, "")
		ack()
	}
}

func (w *HelmWorker) handleChartRender(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleChartRender",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	number := headers["pr.number"]
	sha := headers["sha.active"]
	origin := headers["app.origin"]
	spec, err := nats.Unmarshal[models.AppSpec](data)
	if err != nil {
		w.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("sha.active", sha),
		attribute.String("app.name", spec.AppName),
		attribute.String("app.origin", origin),
	)
	w.log.Debug("new *.chart.fetched event",
		"app", spec.AppName,
		"repo", spec.Source.RepoURL,
		"revision", spec.Source.Revision)

	chartDir := headers["chart.location"]
	manifest, err := helm.RenderChart(ctx, spec.Namespace, spec.Helm.ReleaseName, chartDir, spec.Source.Revision, spec.Helm.Values)
	if err != nil {
		headers["error.msg"] = err.Error()
		headers["error.origin.file"] = origin
		headers["error.origin.app"] = spec.AppName
		w.log.Error("failed to render the manifest", "error", err)
		span.SetStatus(codes.Error, err.Error())
		w.bus.Publish(ctx, subjects.HelmManifestRenderFailed, headers, nil)
		ack()
		return
	}
	key := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, spec.AppName)
	if err := w.store.StoreObject(ctx, key, manifest); err != nil {
		w.log.Error("failed to store the manifest", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers["manifest.location"] = key
	headers["app.name"] = spec.AppName
	w.bus.Publish(ctx, subjects.HelmManifestRendered, headers, nil)
	ack()
}

func (w *HelmWorker) handleEmptyManifest(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleEmptyManifest",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	number := headers["pr.number"]
	sha := headers["sha.active"]
	appName := headers["app.name"]
	origin := headers["app.origin"]
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("sha.active", sha),
		attribute.String("app.name", appName),
		attribute.String("app.origin", origin),
	)
	key := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	if err := w.store.StoreObject(ctx, key, "---"); err != nil {
		w.log.Error("failed to store the manifest", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers["manifest.location"] = key
	w.bus.Publish(ctx, subjects.HelmManifestRendered, headers, nil)
	ack()
}
