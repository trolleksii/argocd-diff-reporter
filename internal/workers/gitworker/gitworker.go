package gitworker

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/repository"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
)

// RepositoryProvider abstracts the repository operations used by GitWorker.
type RepositoryProvider interface {
	ListChangedFiles(base, head string) ([]repository.Change, error)
	GetOrCreateSnapshot(ref, repoDir string, files []string) (string, error)
}

// AuthProvider abstracts the authentication credential methods used by GitWorker.
type AuthProvider interface {
	GetBasicHTTPAuth() (*githttp.BasicAuth, error)
}

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/gitworker")

type GitWorker struct {
	cfg  config.GitWorkerConfig
	auth AuthProvider
	log  *slog.Logger
	bus  *nats.Bus

	mu    sync.RWMutex
	repos map[string]RepositoryProvider
}

func New(cfg config.GitWorkerConfig, log *slog.Logger, auth AuthProvider, b *nats.Bus) *GitWorker {
	return &GitWorker{
		cfg:   cfg,
		auth:  auth,
		log:   log.With("worker", "git"),
		bus:   b,
		repos: make(map[string]RepositoryProvider),
	}
}

func (w *GitWorker) Run(ctx context.Context) error {
	w.log.Info("starting git worker...")
	err := w.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "gitrepomanager",
		MaxDeliver:  3,
		AckWait:     3 * time.Second,
		Concurrency: 2,
		Routes: []nats.Route{
			{Subjects: []string{subjects.WebhookPRChanged}, Handler: w.handlePRChanged},
			{Subjects: []string{subjects.GitFilesResolved}, Handler: w.handleFilesResolved},
			{Subjects: []string{subjects.ArgoHelmGitParsed}, Handler: w.handleHelmGitParsed},
			{Subjects: []string{subjects.ArgoDirectoryGitParsed}, Handler: w.handleDirectoryGitParsed},
		},
	})
	if err != nil {
		return fmt.Errorf("gitrepomanager: consume: %w", err)
	}
	return nil
}

func (w *GitWorker) handlePRChanged(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handlePRChanged",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	pr, err := nats.Unmarshal[models.PullRequest](data)
	if err != nil {
		w.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	span.SetAttributes(
		attribute.String("pr.owner", pr.Owner),
		attribute.String("pr.repo", pr.Repo),
		attribute.String("pr.number", pr.Number),
		attribute.String("pr.baseSha", pr.BaseSHA),
		attribute.String("pr.headSha", pr.HeadSHA),
	)
	w.log.Debug("new webhook.pr.changed event",
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
	r, err := w.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		w.log.Error("failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		leafSpan.End()
		return
	}
	leafSpan.End()

	_, leafSpan = tracer.Start(ctx, "ListChangedFiles")
	changes, err := r.ListChangedFiles(pr.BaseSHA, pr.HeadSHA)
	if err != nil {
		w.log.Error("failed to list changed files")
		span.SetStatus(codes.Error, err.Error())
		nak()
		leafSpan.End()
		return
	}
	leafSpan.End()

	_, leafSpan = tracer.Start(ctx, "FilterAndPublishFiles")
	from, to := filterAndSplitChanges(changes, w.cfg.FileGlobs)
	defer leafSpan.End()
	if len(from) > 0 {
		data, err := nats.Marshal(from)
		if err != nil {
			w.log.Error("failed to marshal base files", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		headers["sha.active"] = pr.BaseSHA
		headers["sha.complementary"] = pr.HeadSHA
		span.SetStatus(codes.Ok, "files resolved")
		w.bus.Publish(ctx, subjects.GitFilesResolved, headers, data)
	}
	if len(to) > 0 {
		data, err = nats.Marshal(to)
		if err != nil {
			w.log.Error("failed to marshal head files", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		headers["sha.active"] = pr.HeadSHA
		headers["sha.complementary"] = pr.BaseSHA
		span.SetStatus(codes.Ok, "files resolved")
		w.bus.Publish(ctx, subjects.GitFilesResolved, headers, data)
	}
	ack()
}

func (w *GitWorker) handleFilesResolved(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
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
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", num),
		attribute.String("sha.active", sha),
	)
	w.log.Debug("new git.files.resolved event",
		"prNum", num,
		"owner", owner,
		"repo", repo,
		"sha", sha)

	specs, err := nats.Unmarshal[[]models.FileProcessingSpec](data)
	if err != nil {
		w.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", owner, repo)
	r, err := w.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		w.log.Error("failed to find git repo", "error", err)
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
		w.log.Error("failed to create snapshot", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers["pr.files.snapshot"] = snapshotPath
	span.SetStatus(codes.Ok, "files snapshotted")
	w.bus.Publish(ctx, subjects.GitFilesSnapshotted, headers, data)
	ack()
}

func (w *GitWorker) handleHelmGitParsed(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	w.fetchSource(ctx, headers, data, ack, nak, subjects.GitChartFetched, subjects.GitChartFetchFailed)
}

func (w *GitWorker) handleDirectoryGitParsed(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	w.fetchSource(ctx, headers, data, ack, nak, subjects.GitDirectoryFetched, subjects.GitDirectoryFetchFailed)
}

func (w *GitWorker) fetchSource(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error, successSubject, failSubject string) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"fetchSource",
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
	w.log.Debug("new snapshot fetch event",
		"app", spec.AppName,
		"repo", spec.Source.RepoURL,
		"revision", spec.Source.Revision)

	appOrigin := headers["app.origin"]
	r, err := w.getOrCreateRepo(ctx, spec.Source.RepoURL)
	if err != nil {
		headers["error.origin.file"] = appOrigin
		headers["error.origin.app"] = spec.AppName
		headers["error.msg"] = err.Error()
		w.log.Error("failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		w.bus.Publish(ctx, failSubject, headers, nil)
		ack()
		return
	}
	snapshotDir, err := r.GetOrCreateSnapshot(spec.Source.Revision, spec.Source.Path, nil)
	if err != nil {
		headers["error.origin.file"] = appOrigin
		headers["error.origin.app"] = spec.AppName
		headers["error.msg"] = err.Error()
		w.log.Error("failed to create snapshot", "error", err)
		span.SetStatus(codes.Error, err.Error())
		w.bus.Publish(ctx, failSubject, headers, nil)
		ack()
		return
	}
	headers["chart.location"] = snapshotDir
	w.bus.Publish(ctx, successSubject, headers, data)
	span.SetStatus(codes.Ok, "")
	ack()
}

// getOrCreateRepo returns the entry for a repo URL, initializing it on first access.
func (w *GitWorker) getOrCreateRepo(ctx context.Context, repoURL string) (RepositoryProvider, error) {
	w.mu.RLock()
	repo, ok := w.repos[repoURL]
	w.mu.RUnlock()
	if ok {
		return repo, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if repo, ok = w.repos[repoURL]; ok {
		return repo, nil
	}

	repo, err := repository.NewRepository(
		ctx,
		repoURL,
		w.cfg.CloneBaseDir,
		w.cfg.SnapshotBaseDir,
		w.auth,
		w.log.With("repo", repoURL),
	)
	if err != nil {
		return nil, fmt.Errorf("init repo %s: %w", repoURL, err)
	}

	w.repos[repoURL] = repo
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
				fc.HasNoCounterpart = true
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
				fc.HasNoCounterpart = true
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
