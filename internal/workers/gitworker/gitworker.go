package gitworker

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/keys"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/repository"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/tracing"
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
	w.log.InfoContext(ctx, "starting git worker...")
	err := w.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "gitrepomanager",
		MaxDeliver: 3,
		// First-time clone of a large repo can take minutes; AckWait must
		// outlast it or the message is redelivered and eventually dropped.
		AckWait:     5 * time.Minute,
		Concurrency: 2,
		Routes: []nats.Route{
			{Subjects: []string{subjects.WebhookPRChanged}, Handler: w.resolvePRFileChanges},
			{Subjects: []string{subjects.GitFilesResolved}, Handler: w.snapshotChangedFiles},
			{Subjects: []string{subjects.ArgoHelmGitParsed}, Handler: w.fetchHelmChartFromGitRepo},
			{Subjects: []string{subjects.ArgoDirectoryGitParsed}, Handler: w.fetchDirectoryFromGitRepo},
		},
	})
	if err != nil {
		return fmt.Errorf("gitrepomanager: consume: %w", err)
	}
	return nil
}

func (w *GitWorker) resolvePRFileChanges(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"resolvePRFileChanges",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	pr, err := nats.Unmarshal[models.PullRequest](data)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to unmarshal pr object", "error", err)
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
	w.log.DebugContext(ctx, "new webhook.pr.changed event",
		"prNum", pr.Number,
		"owner", pr.Owner,
		"repo", pr.Repo)

	// maybe not the best way but at least I don't need to unmarshal/marshal on every hop
	// these will be attached to every message in the chain
	headers.Set("pr.number", pr.Number)
	headers.Set("pr.owner", pr.Owner)
	headers.Set("pr.repo", pr.Repo)
	headers.Set("pr.sha.base", pr.BaseSHA)
	headers.Set("pr.sha.head", pr.HeadSHA)

	_, leafSpan := tracing.StartDetail(ctx, tracer, "getOrCreateRepo")
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", pr.Owner, pr.Repo)
	r, err := w.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		leafSpan.End()
		return
	}
	leafSpan.End()

	_, leafSpan = tracing.StartDetail(ctx, tracer, "ListChangedFiles")
	changes, err := r.ListChangedFiles(pr.BaseSHA, pr.HeadSHA)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to list changed files")
		span.SetStatus(codes.Error, err.Error())
		nak()
		leafSpan.End()
		return
	}
	leafSpan.End()

	_, leafSpan = tracing.StartDetail(ctx, tracer, "FilterAndPublishFiles")
	from, to := filterAndSplitChanges(changes, w.cfg.FileGlobs)
	defer leafSpan.End()
	if len(from) == 0 && len(to) == 0 {
		w.log.InfoContext(ctx, "no changed files match fileGlobs, nothing to report",
			"prNum", pr.Number, "changedFiles", len(changes), "globs", w.cfg.FileGlobs)
		ack()
		return
	}

	headers.Set(keys.MsgIDHeader, keys.MsgIDSides(headers["RunId"]))
	w.bus.Publish(ctx, subjects.GitFilesMatched, headers, data)
	// Drop the id before the next hops reuse these headers, or JetStream dedups them away.
	delete(headers, keys.MsgIDHeader)

	headers.Set("file.withBase", strconv.FormatBool(len(from) > 0))
	headers.Set("file.withHead", strconv.FormatBool(len(to) > 0))

	if len(from) > 0 {
		data, err := nats.Marshal(from)
		if err != nil {
			w.log.ErrorContext(ctx, "failed to marshal base files", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		headers.Set("sha.active", pr.BaseSHA)
		headers.Set(keys.MsgIDHeader, keys.MsgIDFiles(pr.Owner, pr.Repo, pr.Number, headers["RunId"], pr.BaseSHA))
		span.SetStatus(codes.Ok, "files resolved")
		w.bus.Publish(ctx, subjects.GitFilesResolved, headers, data)
	}
	if len(to) > 0 {
		data, err = nats.Marshal(to)
		if err != nil {
			w.log.ErrorContext(ctx, "failed to marshal head files", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		headers.Set("sha.active", pr.HeadSHA)
		headers.Set(keys.MsgIDHeader, keys.MsgIDFiles(pr.Owner, pr.Repo, pr.Number, headers["RunId"], pr.HeadSHA))
		span.SetStatus(codes.Ok, "files resolved")
		w.bus.Publish(ctx, subjects.GitFilesResolved, headers, data)
	}
	ack()
}

func (w *GitWorker) snapshotChangedFiles(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"snapshotChangedFiles",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	num := headers.Get("pr.number")
	owner := headers.Get("pr.owner")
	repo := headers.Get("pr.repo")
	sha := headers.Get("sha.active")
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", num),
		attribute.String("sha.active", sha),
	)
	w.log.DebugContext(ctx, "new git.files.resolved event",
		"prNum", num,
		"owner", owner,
		"repo", repo,
		"sha", sha)

	files, err := nats.Unmarshal[[]string](data)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", owner, repo)
	r, err := w.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	snapshotPath, err := r.GetOrCreateSnapshot(sha, "", files)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to create snapshot", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers.Set("pr.files.snapshot", snapshotPath)
	headers.Set(keys.MsgIDHeader, keys.MsgIDSnapshot(owner, repo, num, headers.Get("RunId"), sha))
	span.SetStatus(codes.Ok, "files snapshotted")
	w.bus.Publish(ctx, subjects.GitFilesSnapshotted, headers, data)
	ack()
}

func (w *GitWorker) fetchHelmChartFromGitRepo(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	w.fetchSource(ctx, headers, data, ack, nak, subjects.GitChartFetched, subjects.ManifestRenderFinished)
}

func (w *GitWorker) fetchDirectoryFromGitRepo(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	w.fetchSource(ctx, headers, data, ack, nak, subjects.GitDirectoryFetched, subjects.ManifestRenderFinished)
}

func (w *GitWorker) fetchSource(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error, successSubject, failSubject string) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"fetchSource",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	spec, err := nats.Unmarshal[models.ArgoAppSpec](data)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to unmarshal pr object", "error", err)
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
	w.log.DebugContext(ctx, "new snapshot fetch event",
		"app", spec.AppName,
		"repo", spec.Source.RepoURL,
		"revision", spec.Source.Revision)
	owner, repo, number := headers["pr.owner"], headers["pr.repo"], headers["pr.number"]
	runId, sha, origin := headers["RunId"], headers["sha.active"], headers["app.origin"]

	r, err := w.getOrCreateRepo(ctx, spec.Source.RepoURL)
	if err != nil {
		headers.Set("error.msg", err.Error())
		w.log.ErrorContext(ctx, "failed to find git repo", "error", err)
		span.SetStatus(codes.Error, err.Error())
		headers.Set(keys.MsgIDHeader, keys.MsgIDRender(owner, repo, number, runId, sha, origin, spec.AppName))
		w.bus.Publish(ctx, failSubject, headers, nil)
		ack()
		return
	}
	snapshotDir, err := r.GetOrCreateSnapshot(spec.Source.Revision, spec.Source.Path, nil)
	if err != nil {
		headers.Set("error.msg", err.Error())
		w.log.ErrorContext(ctx, "failed to create snapshot", "error", err)
		span.SetStatus(codes.Error, err.Error())
		headers.Set(keys.MsgIDHeader, keys.MsgIDRender(owner, repo, number, runId, sha, origin, spec.AppName))
		w.bus.Publish(ctx, failSubject, headers, nil)
		ack()
		return
	}
	headers.Set("chart.location", filepath.Join(snapshotDir, spec.Source.Path))
	headers.Set(keys.MsgIDHeader, keys.MsgIDFetched(owner, repo, number, runId, sha, origin, spec.AppName))
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

func filterAndSplitChanges(changes []repository.Change, globs []string) (from, to []string) {
	for _, c := range changes {
		if globMatches(c.From, globs) {
			from = append(from, c.From)
		}
		if globMatches(c.To, globs) {
			to = append(to, c.To)
		}
	}
	return
}

func globMatches(file string, globs []string) bool {
	for _, g := range globs {
		if res, err := filepath.Match(g, file); res && err == nil {
			return true
		}
	}
	return false
}
