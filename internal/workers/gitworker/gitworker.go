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
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
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

	m.log.Debug("new webhook.pr.changed event", "prNum", headers["prNum"], "owner", headers["owner"], "repo", headers["repository"])
	pr, err := nats.Unmarshal[models.PullRequest](data)
	if err != nil {
		m.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", pr.Owner, pr.Repo)
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
	changes, err := r.ListChangedFiles(pr.BaseSHA, pr.HeadSHA)
	if err != nil {
		m.log.Error("failed to list changed files")
		span.SetStatus(codes.Error, err.Error())
		nak()
		leafSpan.End()
		return
	}
	leafSpan.End()
	var from, to []models.FileProcessingSpec

	// here we handle corner cases of file renames where base report should be stored under head filename
	// so that coordinator would detect both files
	// and the creation/deletion case, where empty manifest need to be produced for coordinator to initiate diff report
	for _, change := range filterChanges(changes, m.cfg.FileGlobs) {
		if change.From != "" {
			fc := models.FileProcessingSpec{
				FileName:     change.From,
				ArtifactName: change.From,
			}
			if change.To == "" {
				fc.EmptyArtifactSHA = headers["headSha"]
			} else if change.From != change.To {
				fc.ArtifactName = change.To
			}
			from = append(from, fc)
		}
		if change.To != "" {
			fc := models.FileProcessingSpec{
				FileName: change.To,
			}
			if change.From == "" {
				fc.EmptyArtifactSHA = headers["baseSha"]
			}
			to = append(to, fc)
		}
	}

	_, leafSpan = tracer.Start(ctx, "PublishFiles")
	defer leafSpan.End()
	if len(from) > 0 {
		headers["ref"] = headers["baseSha"]
		data, err := nats.Marshal(from)
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
		data, err = nats.Marshal(to)
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
	fileChanges, err := nats.Unmarshal[[]models.FileProcessingSpec](data)
	var files = make([]string, len(fileChanges))
	for i, fc := range fileChanges {
		files[i] = fc.FileName
	}
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

func filterChanges(changes []repository.Change, globs []string) []repository.Change {
	// keep changes where at lest one side matches the glob
	var result []repository.Change
	for _, c := range changes {
		if !globMatches(c.From, globs) {
			c.From = ""
		}
		if !globMatches(c.To, globs) {
			c.To = ""
		}
		if c.To != "" || c.From != "" {
			result = append(result, c)
		}
	}
	return result
}

func globMatches(file string, globs []string) bool {
	for _, g := range globs {
		if res, err := filepath.Match(g, file); res && err == nil {
			return true
		}
	}
	return false
}
