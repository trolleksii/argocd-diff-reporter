package helmmanager

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/helm"
)

type HelmManager struct {
	cfg   config.HelmWorkerConfig
	log   *slog.Logger
	bus   *nats.Bus
	store *nats.Store
	cache *helm.HelmChartCache
	creds *helm.CredsProvider
}

func New(cfg config.HelmWorkerConfig, log *slog.Logger, b *nats.Bus, s *nats.Store) *HelmManager {
	return &HelmManager{
		cfg:   cfg,
		log:   log.With("component", "helmmanager"),
		bus:   b,
		store: s,
	}
}

// Run starts consuming snapshot requests.
// Blocks until ctx is cancelled, then shuts down all repos.
func (m *HelmManager) Run(ctx context.Context) error {
	err := m.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "helmmanager",
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

func try(log *slog.Logger, msg string, fn func() error) {
	if err := fn(); err != nil {
		log.Error(msg, "error", err)
	}
}

func (m *HelmManager) handleChartFetch(fetchFn func(string, string, helm.CredsProvider, *helm.HelmChartCache) (string, error)) nats.Handler {
	return func(ctx context.Context, headers map[string]string, data []byte, ack, nak func() error) {
		chartRef := headers["ref"]
		chartVersion := headers["version"]
		chartLocation, err := fetchFn(chartRef, chartVersion, nil, m.cache)
		if err != nil {
			try(m.log, "failed to nak the message", nak)
			return
		}
		headers["chartLocation"] = chartLocation
		if err := m.bus.Publish(ctx, "helm.chart.fetched", headers, data); err != nil {
			try(m.log, "failed to nak the message", nak)
			return
		}
		try(m.log, "failed to ack the message", ack)
	}
}

func (m *HelmManager) handleChartRender(ctx context.Context, headers map[string]string, data []byte, ack, nak func() error) {
	namespace := headers["namespace"]
	releaseName := headers["releaseName"]
	chartPath := headers["chartPath"]
	chartVersion := headers["chartVersion"]

	releaseValues, err := nats.Unmarshal[map[string]any](data)

	manifest, err := helm.RenderChart(ctx, namespace, releaseName, chartPath, chartVersion, releaseValues)
	if err != nil {
		try(m.log, "failed to nak the message", nak)
		return
	}
	sha := headers["sha"]
	fileName := headers["fileName"]
	appName := headers["application"]
	id := fmt.Sprintf("%s/%s/%s", sha, fileName, appName)
	if err := m.store.StoreObject(ctx, id, manifest); err != nil {
		try(m.log, "failed to nak the message", nak)
		return
	}
	m.log.Info(manifest)
	headers["manifestLocation"] = id
	if err := m.bus.Publish(ctx, "helm.manifest.rendered", headers, data); err != nil {
		try(m.log, "failed to nak the message", nak)
		return
	}
	try(m.log, "failed to ack the message", ack)
}
