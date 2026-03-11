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

		chartRepo := headers["chartRepo"]
		chartRevision := headers["chartRevision"]
		chartName := headers["chartName"]
		chartRef := fmt.Sprintf("%s/%s", chartRepo, chartName)
		headers["Nats-Msg-Id"] = fmt.Sprintf("%s/%s/%s/%s/%s", headers["ref"], headers["fileName"], headers["application"], chartRef, chartRevision)
		m.log.Debug("new argo.helm.(oci|http}.parsed event", "ref", chartRef, "version", chartRevision)
		chartLocation, err := fetchFn(chartRef, chartRevision, nil, m.cache)
		if err != nil {
			headers["error"] = err.Error()
			m.log.Error("failed to feth the chart", "error", err)
			m.bus.Publish(ctx, "helm.chart.fetch.failed", headers, nil)
			ack()
			return
		}
		headers["chartLocation"] = chartLocation
		m.log.Debug("fetched the chart", "location", chartLocation)
		m.bus.Publish(ctx, "helm.chart.fetched", headers, data)
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

	namespace := headers["namespace"]
	releaseName := headers["releaseName"]
	chartPath := headers["chartLocation"]
	chartVersion := headers["chartRevision"]
	number := headers["prNum"]
	sha := headers["ref"]
	fileName := headers["fileName"]
	appName := headers["application"]
	m.log.Debug("new (git|helm).chart.fetched event", "application", appName)

	releaseValues, err := nats.Unmarshal[map[string]any](data)
	manifest, err := helm.RenderChart(ctx, namespace, releaseName, chartPath, chartVersion, releaseValues)
	if err != nil {
		headers["error"] = err.Error()
		m.log.Error("failed to render the manifest", "error", err)
		span.SetStatus(codes.Error, err.Error())
		m.bus.Publish(ctx, "helm.manifest.render.failed", headers, nil)
		ack()
		return
	}

	delete(headers, "Nats-Msg-Id")
	key := fmt.Sprintf("%s.%s.%s.%s", number, sha, fileName, appName)
	m.log.Debug("stored manifest", "key", key)
	if err := m.store.StoreObject(ctx, key, manifest); err != nil {
		m.log.Error("failed to store the manifest", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers["manifestLocation"] = key
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

	number := headers["prNum"]
	sha := headers["ref"]
	fileName := headers["fileName"]
	appName := headers["application"]
	delete(headers, "Nats-Msg-Id")
	key := fmt.Sprintf("%s.%s.%s.%s", number, sha, fileName, appName)
	m.log.Debug("stored manifest", "key", key)
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
