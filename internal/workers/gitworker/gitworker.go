package gitworker

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/githubauth"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/repository"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/gitworker")

type GitWorker struct {
	cfg  config.GitWorkerConfig
	auth *githubauth.GithubCredManager
	log  *slog.Logger
	bus  *nats.Bus

	mu    sync.RWMutex
	repos map[string]*repository.Repository
}

func New(cfg config.GitWorkerConfig, log *slog.Logger, auth *githubauth.GithubCredManager, b *nats.Bus) *GitWorker {
	return &GitWorker{
		cfg:   cfg,
		auth:  auth,
		log:   log.With("worker", "git"),
		bus:   b,
		repos: make(map[string]*repository.Repository),
	}
}

func (m *GitWorker) Run(ctx context.Context) error {
	m.log.Info("starting git worker...")
	err := m.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "gitrepomanager",
		MaxDeliver:  3,
		AckWait:     3 * time.Second,
		Concurrency: 2,
		Handlers: map[string]nats.Handler{
			"webhook.pr.changed":   m.handlePRChanged,
			"git.files.resolved":   m.handleFilesResolved,
			"argo.helm.git.parsed": m.handleChartFetch,
		},
	})
	if err != nil {
		return fmt.Errorf("gitrepomanager: consume: %w", err)
	}
	return nil
}

func (m *GitWorker) handlePRChanged(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handlePRChanged",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	m.log.Debug("new webhook.pr.changed event", "prNum", headers["prNum"], "owner", headers["owner"], "repo", headers["repository"] )
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", headers["owner"], headers["repository"])
	_, leafSpan := tracer.Start(ctx, "getOrCreateRepo")
	r, err := m.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		m.log.Error("failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		leafSpan.End()
		return
	}
	leafSpan.End()
	_, leafSpan = tracer.Start(ctx, "ListChangedFiles")
	changes, err := r.ListChangedFiles(headers["baseSha"], headers["headSha"])
	if err != nil {
		m.log.Error("failed to list changed files")
		span.SetStatus(codes.Error, err.Error())
		nak()
		leafSpan.End()
		return
	}
	leafSpan.End()
	var from, to []string
	// TODO: files that are created/deleted/moved will lack the counterpart for the diff to run
	// we need to do something about it
	for _, change := range changes {
		if change.From != "" {
			from = append(from, change.From)
		}
		if change.To != "" {
			to = append(to, change.To)
		}
	}

	_, leafSpan = tracer.Start(ctx, "PublishFiles")
	defer leafSpan.End()
	if len(from) > 0 {
		headers["ref"] = headers["baseSha"]
		data, err := nats.Marshal(FileGlobFilter(from, m.cfg.FileGlobs))
		if err != nil {
			m.log.Error("failed to marshal base files", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		span.SetStatus(codes.Ok, "files resolved")
		m.bus.Publish(ctx, "git.files.resolved", headers, data)
	}
	if len(to) > 0 {
		headers["ref"] = headers["headSha"]
		data, err = nats.Marshal(FileGlobFilter(to, m.cfg.FileGlobs))
		if err != nil {
			m.log.Error("failed to marshal head files", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		span.SetStatus(codes.Ok, "files resolved")
		m.bus.Publish(ctx, "git.files.resolved", headers, data)
	}
	ack()
}

func (m *GitWorker) handleFilesResolved(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleFilesResolved",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	m.log.Debug("new git.files.resolved event", "prNum", headers["prNum"], "owner", headers["owner"], "repo", headers["repository"], "sha", headers["ref"])
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", headers["owner"], headers["repository"])
	r, err := m.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		m.log.Error("failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	files, err := nats.Unmarshal[[]string](data)
	snapshotDir, err := r.GetOrCreateSnapshot(headers["ref"], "", files)
	if err != nil {
		m.log.Error("failed to create snapshot", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	span.SetStatus(codes.Ok, "files snapshotted")
	headers["snapshotDir"] = snapshotDir
	m.bus.Publish(ctx, "git.files.snapshotted", headers, data)
	ack()
}

func (m *GitWorker) handleChartFetch(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleChartFetch",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	m.log.Debug("new argo.helm.git.parsed event", "app", headers["application"], "repo", headers["chartRepo"], "revision", headers["chartRevision"])
	chartRepo := headers["chartRepo"]
	chartRevision := headers["chartRevision"]
	chartPath := headers["chartPath"]
	r, err := m.getOrCreateRepo(ctx, chartRepo)
	if err != nil {
		headers["error"] = err.Error()
		m.log.Error("failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		m.bus.Publish(ctx, "git.chart.fetch.failed", headers, nil)
		ack()
		return
	}
	snapshotDir, err := r.GetOrCreateSnapshot(chartRevision, chartPath, nil)
	if err != nil {
		headers["error"] = err.Error()
		m.log.Error("failed to create snapshot", "error", err)
		span.SetStatus(codes.Error, err.Error())
		m.bus.Publish(ctx, "git.chart.fetch.failed", headers, nil)
		ack()
		return
	}
	headers["snapshotDir"] = snapshotDir
	span.SetStatus(codes.Ok, "helm fetched")
	m.bus.Publish(ctx, "git.chart.fetched", headers, data)
	ack()
}

// getOrCreateRepo returns the entry for a repo URL, initializing it on first access.
func (m *GitWorker) getOrCreateRepo(ctx context.Context, repoURL string) (*repository.Repository, error) {
	m.mu.RLock()
	repo, ok := m.repos[repoURL]
	m.mu.RUnlock()
	if ok {
		return repo, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if repo, ok = m.repos[repoURL]; ok {
		return repo, nil
	}

	repo, err := repository.NewRepository(
		ctx,
		repoURL,
		m.cfg.CloneBaseDir,
		m.cfg.SnapshotBaseDir,
		m.auth,
		m.log.With("repo", repoURL),
	)
	if err != nil {
		return nil, fmt.Errorf("init repo %s: %w", repoURL, err)
	}

	m.repos[repoURL] = repo
	return repo, nil
}

func FileGlobFilter(files []string, globs []string) []string {
	var result []string
	for _, f := range files {
		for _, g := range globs {
			res, err := filepath.Match(g, f)
			if res && err == nil {
				result = append(result, f)
				break
			}
		}
	}
	return result
}
