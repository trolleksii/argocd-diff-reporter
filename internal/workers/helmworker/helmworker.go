package helmworker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/helm"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

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
	err := m.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "helmworker",
		MaxDeliver: 3,
		AckWait:    3 * time.Second,
		Handlers: map[string]nats.Handler{
			"argo.helm.oci.parsed":   m.handleChartFetch(helm.FetchChartOCI),
			"argo.helm.http.parsed":  m.handleChartFetch(helm.FetchChartHTTPS),
			"helm.manifest.rendered": m.handleChartRender,
		},
	})
	if err != nil {
		return fmt.Errorf("gitrepomanager: consume: %w", err)
	}
	return nil
}

func (m *HelmWorker) handleChartFetch(fetchFn func(string, string, helm.CredsProvider, *helm.HelmChartCache) (string, error)) nats.Handler {
	return func(ctx context.Context, headers map[string]string, data []byte, ack, nak func() error) {
		m.log.Info("helm worker got a helm fetch event")
		chartRef := headers["ref"]
		chartVersion := headers["version"]
		chartLocation, err := fetchFn(chartRef, chartVersion, nil, m.cache)
		if err != nil {
			headers["error"] = err.Error()
			m.log.Error("failed to feth the chart", "error", err)
			m.bus.Publish(ctx, "helm.chart.fetch.failed", headers, nil)
			ack()
			return
		}
		headers["chartLocation"] = chartLocation
		m.bus.Publish(ctx, "helm.chart.fetched", headers, data)
		ack()
	}
}

func (m *HelmWorker) handleChartRender(ctx context.Context, headers map[string]string, data []byte, ack, nak func() error) {
	m.log.Info("helm worker got a helm render event")
	namespace := headers["namespace"]
	releaseName := headers["releaseName"]
	chartPath := headers["chartPath"]
	chartVersion := headers["chartVersion"]

	releaseValues, err := nats.Unmarshal[map[string]any](data)

	manifest, err := helm.RenderChart(ctx, namespace, releaseName, chartPath, chartVersion, releaseValues)
	if err != nil {
		headers["error"] = err.Error()
		m.log.Error("failed to render the manifest", "error", err)
		m.bus.Publish(ctx, "helm.manifest.render.failed", headers, nil)
		ack()
		return
	}
	sha := headers["sha"]
	fileName := headers["fileName"]
	appName := headers["application"]
	id := fmt.Sprintf("%s/%s/%s", sha, fileName, appName)
	if err := m.store.StoreObject(ctx, id, manifest); err != nil {
		nak()
		return
	}
	headers["manifestLocation"] = id
	m.bus.Publish(ctx, "helm.manifest.rendered", headers, data)
	ack()
}
