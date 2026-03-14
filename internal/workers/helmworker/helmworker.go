package helmworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/helm"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
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

func (m *HelmWorker) Run(ctx context.Context) error {
	m.log.Info("starting helm worker...")
	c, err := helm.NewChartDiskCache(m.cfg.ChartCacheDir)
	if err != nil {
		return err
	}
	m.cache = c
	err = m.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "helmworker",
		MaxDeliver:  3,
		AckWait:     3 * time.Second,
		Concurrency: 4,
		Handlers: map[string]nats.Handler{
			"argo.helm.oci.parsed":   m.handleChartFetch(helm.FetchChartOCI),
			"argo.helm.http.parsed":  m.handleChartFetch(helm.FetchChartHTTPS),
			"argo.helm.empty.parsed": m.handleEmptyManifest,
			"helm.chart.fetched":     m.handleChartRender,
			"git.chart.fetched":      m.handleChartRender,
		},
	})
	if err != nil {
		return fmt.Errorf("helm worker failed to consume: %w", err)
	}
	return nil
}

func (m *HelmWorker) handleChartFetch(fetchFn func(string, string, helm.CredsProvider, *helm.HelmChartCache) (string, error)) nats.Handler {
	return func(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
		ctx, span := tracer.Start(
			otel.GetTextMapPropagator().Extract(ctx, headers),
			"handleChartFetch",
		)
		otel.GetTextMapPropagator().Inject(ctx, headers)
		defer span.End()

		spec, err := nats.Unmarshal[models.AppSpec](data)
		if err != nil {
			m.log.Error("failed to unmarshal pr object", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		m.log.Debug("new argo.helm.(oci|http}.parsed event",
			"app", spec.AppName,
			"repo", spec.Source.RepoURL,
			"chart", spec.Source.ChartName,
			"path", spec.Source.Path,
			"revision", spec.Source.Revision)
		appOrigin := headers["app.origin"]
		chartRef := fmt.Sprintf("%s/%s", spec.Source.RepoURL, spec.Source.ChartName)
		chartLocation, err := fetchFn(chartRef, spec.Source.Revision, nil, m.cache)
		if err != nil {
			headers["error.origin.file"] = appOrigin
			headers["error.origin.app"] = spec.AppName
			headers["error.msg"] = err.Error()
			m.log.Error("failed to feth the chart", "error", err)
			m.bus.Publish(ctx, "helm.chart.fetch.failed", headers, nil)
			ack()
			return
		}
		headers["chart.location"] = chartLocation
		m.bus.Publish(ctx, "helm.chart.fetched", headers, data)
		span.SetStatus(codes.Ok, "")
		ack()
	}
}

func (m *HelmWorker) handleChartRender(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
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
		m.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	m.log.Debug("new *.chart.fetched event", "application",
		"app", spec.AppName,
		"repo", spec.Source.RepoURL,
		"chart", spec.Source.ChartName,
		"path", spec.Source.Path,
		"revision", spec.Source.Revision)

	chartDir := headers["chart.location"]
	manifest, err := helm.RenderChart(ctx, spec.Namespace, spec.Helm.ReleaseName, chartDir, spec.Source.Revision, spec.Helm.Values)
	if err != nil {
		headers["error.msg"] = err.Error()
		headers["error.origin.file"] = origin
		headers["error.origin.app"] = spec.AppName
		m.log.Error("failed to render the manifest", "error", err)
		span.SetStatus(codes.Error, err.Error())
		m.bus.Publish(ctx, "helm.manifest.render.failed", headers, nil)
		ack()
		return
	}
	key := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, spec.AppName)
	if err := m.store.StoreObject(ctx, key, manifest); err != nil {
		m.log.Error("failed to store the manifest", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers["manifest.location"] = key
	m.bus.Publish(ctx, "helm.manifest.rendered", headers, nil)
	ack()
}

func (m *HelmWorker) handleEmptyManifest(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
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
	key := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	if err := m.store.StoreObject(ctx, key, "---"); err != nil {
		m.log.Error("failed to store the manifest", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers["manifestLocation"] = key
	m.bus.Publish(ctx, "helm.manifest.rendered", headers, nil)
	ack()
}
