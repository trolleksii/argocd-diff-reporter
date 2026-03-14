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

	pr, err := nats.Unmarshal[models.PullRequest](data)
	if err != nil {
		m.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	m.log.Debug("new webhook.pr.changed event",
		"prNum", pr.Number,
		"owner", pr.Owner,
		"repo", pr.Repo)

	// maybe not the best way but at least I don't need to unmarshal/marshal on every hop
	// these will be attached to every message in the chain
	headers["pr.number"] = pr.Number
	headers["pr.owner"] = pr.Owner
	headers["pr.repo"] = pr.Repo
	headers["pr.sha.base"] = pr.BaseSHA
	headers["pr.sha.head"] = pr.HeadSHA

	_, leafSpan := tracer.Start(ctx, "getOrCreateRepo")
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", pr.Owner, pr.Repo)
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

	_, leafSpan = tracer.Start(ctx, "FilterAndPublishFiles")
	from, to := filterAndSplitChanges(changes, m.cfg.FileGlobs)
	defer leafSpan.End()
	if len(from) > 0 {
		data, err := nats.Marshal(from)
		if err != nil {
			m.log.Error("failed to marshal base files", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		headers["sha.active"] = pr.BaseSHA
		headers["sha.complimentary"] = pr.HeadSHA
		span.SetStatus(codes.Ok, "files resolved")
		m.bus.Publish(ctx, "git.files.resolved", headers, data)
	}
	if len(to) > 0 {
		data, err = nats.Marshal(to)
		if err != nil {
			m.log.Error("failed to marshal head files", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		headers["sha.active"] = pr.HeadSHA
		headers["sha.complimentary"] = pr.BaseSHA
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

	num := headers["pr.number"]
	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	sha := headers["sha.active"]
	m.log.Debug("new git.files.resolved event",
		"prNum", num,
		"owner", owner,
		"repo", repo,
		"sha", sha)

	specs, err := nats.Unmarshal[[]models.FileProcessingSpec](data)
	if err != nil {
		m.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", owner, repo)
	r, err := m.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		m.log.Error("failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	var files = make([]string, len(specs))
	for i, fc := range specs {
		files[i] = fc.FileName
	}
	snapshotPath, err := r.GetOrCreateSnapshot(sha, "", files)
	if err != nil {
		m.log.Error("failed to create snapshot", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers["pr.files.snapshot"] = snapshotPath
	span.SetStatus(codes.Ok, "files snapshotted")
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

	spec, err := nats.Unmarshal[models.AppSpec](data)
	if err != nil {
		m.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	m.log.Debug("new argo.helm.git.parsed event",
		"app", spec.AppName,
		"repo", spec.Source.RepoURL,
		"revision", spec.Source.Revision)

	appOrigin := headers["app.origin"]
	r, err := m.getOrCreateRepo(ctx, spec.Source.RepoURL)
	if err != nil {
		headers["error.origin.file"] = appOrigin
		headers["error.origin.app"] = spec.AppName
		headers["error.msg"] = err.Error()
		m.log.Error("failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		m.bus.Publish(ctx, "git.chart.fetch.failed", headers, nil)
		ack()
		return
	}
	chartDir, err := r.GetOrCreateSnapshot(spec.Source.Revision, spec.Source.Path, nil)
	if err != nil {
		headers["error.origin.file"] = appOrigin
		headers["error.origin.app"] = spec.AppName
		headers["error.msg"] = err.Error()
		m.log.Error("failed to create snapshot", "error", err)
		span.SetStatus(codes.Error, err.Error())
		m.bus.Publish(ctx, "git.chart.fetch.failed", headers, nil)
		ack()
		return
	}
	headers["chart.location"] = chartDir
	m.bus.Publish(ctx, "git.chart.fetched", headers, data)
	span.SetStatus(codes.Ok, "")
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

func filterAndSplitChanges(changes []repository.Change, globs []string) (from, to []models.FileProcessingSpec) {
	// keep changes where at lest one side matches the glob
	for _, c := range changes {
		if !globMatches(c.From, globs) {
			c.From = ""
		}
		if !globMatches(c.To, globs) {
			c.To = ""
		}
		if c.From != "" {
			fc := models.FileProcessingSpec{
				FileName:     c.From,
				ArtifactName: c.From,
			}
			if c.To == "" {
				fc.EmptyCounterpart = true
			} else if c.From != c.To {
				fc.ArtifactName = c.To
			}
			from = append(from, fc)
		}
		if c.To != "" {
			fc := models.FileProcessingSpec{
				FileName: c.To,
			}
			if c.From == "" {
				fc.EmptyCounterpart = true
			}
			to = append(to, fc)
		}
	}
	return
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
