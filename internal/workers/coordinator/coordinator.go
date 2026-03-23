package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/notifications"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/coordinator")

type Coordinator struct {
	bus      *nats.Bus
	store    *nats.Store
	log      *slog.Logger
	index    *Index
	notifier *notifications.NotificationServer
	// mutex to serialize kv operaitons
	mu sync.Mutex
}

func New(log *slog.Logger, b *nats.Bus, s *nats.Store, n *notifications.NotificationServer) *Coordinator {
	return &Coordinator{
		bus:      b,
		store:    s,
		notifier: n,
		log:      log.With("component", "coordinator"),
	}
}

func (c *Coordinator) Run(ctx context.Context) error {
	c.log.Info("starting coordinator...")
	storedState, err := nats.GetValue[[]models.PullRequest](ctx, c.store, "index")
	if err != nil {
		storedState = []models.PullRequest{}
	}
	c.index = NewIndex(10, storedState) // TODO: get the max capacity into the config
	c.store.SetValue(ctx, "index", c.index.GetElements())
	err = c.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "coordinator",
		MaxDeliver: 3,
		AckWait:    10 * time.Second,
		Handlers: map[string]nats.Handler{
			subjects.WebhookPRChanged:              c.handlePREvent,
			subjects.ArgoFileParseFailed:           c.handleFileErrors,
			subjects.ArgoTotalUpdated:              c.handleTotalAppUpdate,
			subjects.GitChartFetchFailed:           c.handleAppErrors,
			subjects.HelmChartFetchFailed:          c.handleAppErrors,
			subjects.GitDirectoryFetchFailed:       c.handleAppErrors,
			subjects.DirectoryManifestRenderFailed: c.handleAppErrors,
			subjects.HelmManifestRenderFailed:      c.handleAppErrors,
			subjects.ArgoEmptyParsed:               c.handleEmptyManifest,
			subjects.DirectoryManifestRendered:     c.handleRenderedManifest,
			subjects.HelmManifestRendered:          c.handleRenderedManifest,
			subjects.EmptyManifestRendered:			c.handleRenderedManifest,
			subjects.DiffReportGenerated:           c.handleGeneratedReport,
		},
	})
	if err != nil {
		return fmt.Errorf("coordinator: consume: %w", err)
	}
	return nil
}

func (c *Coordinator) handleFileErrors(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleErrors",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	number := headers["pr.number"]
	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	errorMsg := headers["error.msg"]
	errorOrigin := headers["error.origin"]
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("error.origin", errorOrigin),
	)
	key := fmt.Sprintf("%s.%s.%s", owner, repo, number)
	c.mu.Lock()
	defer c.mu.Unlock()
	pr, err := nats.GetValue[models.PullRequest](ctx, c.store, key)
	if err != nil {
		c.log.Error("failed to unmarshal files", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	if f, ok := pr.Files[errorOrigin]; ok {
		f.Errors = append(f.Errors, errorMsg)
		pr.Files[errorOrigin] = f
	} else {
		pr.Files[errorOrigin] = models.FileResult{Errors: []string{errorMsg}}
	}
	if pr.Status == models.PipelineInProgress {
		pr.Status = models.PipelineFailed
		c.index.UpdateStatus(pr)
		c.store.SetValue(ctx, "index", c.index.GetElements())
		c.notifier.Notify("index", c.index.GetElements())
	}
	c.store.SetValue(ctx, key, pr)
	span.SetStatus(codes.Ok, "")
	ack()
}

func (c *Coordinator) handleAppErrors(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleErrors",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	number := headers["pr.number"]
	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	errorMsg := headers["error.msg"]
	errorOriginFile := headers["error.origin.file"]
	errorOriginApp := headers["error.origin.app"]
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("error.origin.file", errorOriginFile),
		attribute.String("error.origin.app", errorOriginApp),
	)
	key := fmt.Sprintf("%s.%s.%s", owner, repo, number)
	c.mu.Lock()
	defer c.mu.Unlock()
	pr, err := nats.GetValue[models.PullRequest](ctx, c.store, key)
	if err != nil {
		c.log.Error("failed to unmarshal files", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	if f, ok := pr.Files[errorOriginFile]; ok {
		if f.Apps == nil {
			f.Apps = make(map[string]models.App)
			f.Apps[errorOriginApp] = models.App{Errors: []string{errorMsg}}
		} else {
			if a, ok := f.Apps[errorOriginApp]; ok {
				a.Errors = append(a.Errors, errorMsg)
				f.Apps[errorOriginApp] = a
			} else {
				f.Apps[errorOriginApp] = models.App{Errors: []string{errorMsg}}
			}
		}
		pr.Files[errorOriginFile] = f
	} else {
		pr.Files[errorOriginFile] = models.FileResult{
			Apps: map[string]models.App{
				errorOriginApp: {Errors: []string{errorMsg}},
			},
		}
	}
	if pr.Status == models.PipelineInProgress {
		pr.Status = models.PipelineFailed
		c.index.UpdateStatus(pr)
		c.store.SetValue(ctx, "index", c.index.GetElements())
		c.notifier.Notify("index", c.index.GetElements())
	}
	c.store.SetValue(ctx, key, pr)
	span.SetStatus(codes.Ok, "")
	ack()
}

func (c *Coordinator) handlePREvent(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handlePREvent",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	pr, err := nats.Unmarshal[models.PullRequest](data)
	if err != nil {
		c.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	span.SetAttributes(
		attribute.String("pr.owner", pr.Owner),
		attribute.String("pr.repo", pr.Repo),
		attribute.String("pr.number", pr.Number),
	)
	c.log.Debug("coordinating webhook.pr.changed event",
		"prNum", pr.Number,
		"owner", pr.Owner,
		"repo", pr.Repo)

	key := fmt.Sprintf("%s.%s.%s", pr.Owner, pr.Repo, pr.Number)
	c.store.SetValue(ctx, key, pr)
	c.index.Update(pr)
	c.store.SetValue(ctx, "index", c.index.GetElements())
	c.notifier.Notify("index", c.index.GetElements())
	ack()
}

func (c *Coordinator) handleTotalAppUpdate(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleTotalAppUpdate",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	number := headers["pr.number"]
	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	baseSha := headers["pr.sha.base"]
	headSha := headers["pr.sha.head"]
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("app.total", headers["app.total"]),
	)
	total, err := strconv.Atoi(headers["app.total"])
	key := fmt.Sprintf("%s.%s.%s.%s.%s", owner, repo, number, baseSha, headSha)
	c.mu.Lock()
	defer c.mu.Unlock()
	progress, err := nats.GetValue[models.Progress](ctx, c.store, key)
	if err != nil {
		progress = models.Progress{}
	}
	progress.TotalApps += total
	c.store.SetValue(ctx, key, progress)
	ack()
}

func (c *Coordinator) handleRenderedManifest(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleRenderedManifest",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	number := headers["pr.number"]
	baseSha := headers["pr.sha.base"]
	headSha := headers["pr.sha.head"]
	sha := headers["sha.active"]
	appName := headers["app.name"]
	origin := headers["app.origin"]
	manifestLocation := headers.Get("manifest.location")
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("sha.active", sha),
		attribute.String("app.name", appName),
		attribute.String("app.origin", origin),
	)
	c.log.Debug("new helm.manifest.rendered event",
		"prNum", number,
		"appName", appName,
		"sha", sha,
	)
	baseKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, baseSha, origin, appName)
	headKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, headSha, origin, appName)
	headers.Set("Nats-Msg-Id", owner+repo+baseSha+headKey)
	if sha == headSha {
		c.store.SetValue(ctx, headKey, manifestLocation)
		if _, err := nats.GetValue[string](ctx, c.store, baseKey); err == nil {
			headers["app.from"] = baseKey
			headers["app.to"] = manifestLocation
			c.bus.Publish(ctx, subjects.CoordinatorAppReady, headers, nil)
		}
	} else {
		if _, err := nats.GetValue[string](ctx, c.store, headKey); err == nil {
			headers["app.from"] = manifestLocation
			headers["app.to"] = headKey
			c.bus.Publish(ctx, subjects.CoordinatorAppReady, headers, nil)
		}
		c.store.SetValue(ctx, baseKey, manifestLocation)
	}
	span.SetStatus(codes.Ok, "")
	ack()
}

func (c *Coordinator) handleGeneratedReport(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleGeneratedReport",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	number := headers["pr.number"]
	appName := headers["app.name"]
	origin := headers["app.origin"]
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("app.name", appName),
		attribute.String("app.origin", origin),
	)
	c.log.Debug("new diff.report.generated event",
		"owner", owner,
		"repo", repo,
		"pr", number,
		"appName", appName,
	)
	key := fmt.Sprintf("%s.%s.%s", owner, repo, number)
	pr, err := nats.GetValue[models.PullRequest](ctx, c.store, key)
	if err != nil {
		c.log.Error("failed to fetch pull request data", "error", err)
		nak()
		return
	}
	ds, err := nats.Unmarshal[models.DiffStats](data)
	if err != nil {
		c.log.Error("failed to unmarshal diffstats", "error", err)
		nak()
	}
	if f, ok := pr.Files[origin]; ok {
		if a, ok := f.Apps[appName]; ok {
			a.DiffStats = ds
		} else {
			f.Apps[appName] = models.App{DiffStats: ds}
		}
	} else {
		pr.Files[origin] = models.FileResult{
			Apps: map[string]models.App{
				appName: models.App{DiffStats: ds},
			},
		}
	}
	c.mu.Lock()
	progressId := fmt.Sprintf("%s.%s.%s.%s.%s", owner, repo, number, pr.BaseSHA, pr.HeadSHA)
	progress, err := nats.GetValue[models.Progress](ctx, c.store, progressId)
	if err != nil {
		c.log.Error("failed to unmarshal progress object", "error", err)
		nak()
	}
	progress.ProcessedApps += 1
	c.store.SetValue(ctx, progressId, progress)
	if pr.Status == models.PipelineInProgress && progress.TotalApps == progress.ProcessedApps {
		pr.Status = models.PipelineSucceeded
		c.index.UpdateStatus(pr)
		c.store.SetValue(ctx, "index", c.index.GetElements())
		c.notifier.Notify("index", c.index.GetElements())
	}
	c.mu.Unlock()
	c.notifier.Notify("summary:"+key, pr)
	c.store.SetValue(ctx, key, pr)
	span.SetStatus(codes.Ok, "")
	ack()
}

func (c *Coordinator) handleEmptyManifest(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
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
	if err := c.store.StoreObject(ctx, key, "---"); err != nil {
		c.log.Error("failed to store the manifest", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers["manifest.location"] = key
	c.bus.Publish(ctx, subjects.HelmManifestRendered, headers, nil)
	ack()
}
