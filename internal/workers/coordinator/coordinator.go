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
	"go.opentelemetry.io/otel/metric"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/keys"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/notifications"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/coordinator")
var prDuration, _ = otel.Meter("argocd-diff-reporter/internal/workers/coordinator").Float64Histogram(
	"pr.processing.duration",
	metric.WithDescription("Time from webhook receipt to PRProcessingCompleted"),
	metric.WithUnit("s"),
	metric.WithExplicitBucketBoundaries(0.5, 1, 2, 5, 10, 20, 30, 60),
)

type Coordinator struct {
	bus      *nats.Bus
	store    *nats.Store
	log      *slog.Logger
	index    *Index
	notifier *notifications.NotificationServer
	// mutex to serialize kv operaitons
	mu sync.Mutex
}

func New(cfg config.CoordinatorConfig, log *slog.Logger, b *nats.Bus, s *nats.Store, n *notifications.NotificationServer) *Coordinator {
	return &Coordinator{
		bus:      b,
		store:    s,
		notifier: n,
		index:    NewIndex(cfg.IndexCapacity),
		log:      log.With("component", "coordinator"),
	}
}

func (c *Coordinator) Run(ctx context.Context) error {
	c.log.InfoContext(ctx, "starting coordinator...")
	storedState, err := nats.GetValue[[]models.PullRequest](ctx, c.store, keys.Index)
	if err == nil {
		c.index.Load(storedState)
	} else {
		c.store.SetValue(ctx, keys.Index, c.index.GetElements())
	}
	err = c.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "coordinator",
		MaxDeliver: 3,
		AckWait:    10 * time.Second,
		Routes: []nats.Route{
			{Subjects: []string{subjects.GitFilesMatched}, Handler: c.indexInterestingPR},
			{Subjects: []string{subjects.ArgoSideParsed}, Handler: c.handleSideParsed},
			{Subjects: []string{subjects.ManifestRenderFinished}, Handler: c.handleRenderedManifest},
			{Subjects: []string{subjects.DiffReportGenerated}, Handler: c.handleGeneratedReport},
			{Subjects: []string{subjects.WebhookPRClosed}, Handler: c.handlePRClosed},
		},
	})
	if err != nil {
		return fmt.Errorf("coordinator: consume: %w", err)
	}
	return nil
}

// indexInterestingPR waits for non-empty PR events
func (c *Coordinator) indexInterestingPR(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"indexInterestingPR",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	pr, err := nats.Unmarshal[models.PullRequest](data)
	if err != nil {
		c.log.ErrorContext(ctx, "failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	span.SetAttributes(
		attribute.String("pr.owner", pr.Owner),
		attribute.String("pr.repo", pr.Repo),
		attribute.String("pr.number", pr.Number),
	)
	c.log.DebugContext(ctx, "coordinating webhook.pr.changed event",
		"prNum", pr.Number,
		"owner", pr.Owner,
		"repo", pr.Repo)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.index.Update(pr)
	elements := c.index.GetElements()
	c.store.SetValue(ctx, keys.Index, elements)
	c.store.SetValue(ctx, keys.PR(pr.Owner, pr.Repo, pr.Number), pr)
	c.notifier.Notify("index", elements)
	ack()
}

func (c *Coordinator) handleSideParsed(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleSideParsed",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers.Get("pr.owner")
	repo := headers.Get("pr.repo")
	number := headers.Get("pr.number")
	headSha := headers.Get("pr.sha.head")
	sha := headers.Get("sha.active")
	isHead := headSha == sha
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.Bool("side.base", !isHead),
		attribute.Bool("side.head", isHead),
	)

	side, err := nats.Unmarshal[[]models.FileParsingResult](data)
	if err != nil {
		c.log.ErrorContext(ctx, "failed to unmarshal files", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}

	var statusChanged bool
	prKey := keys.PR(owner, repo, number)
	woKey := keys.WorkOrder(owner, repo, number, headers.Get("RunId"))
	c.mu.Lock()
	defer c.mu.Unlock()
	pr, err := nats.GetValue[models.PullRequest](ctx, c.store, prKey)
	if err != nil {
		c.log.ErrorContext(ctx, "failed to fetch pull request data", "error", err)
		nak()
		return
	}

	wo, err := nats.GetValue[models.WorkOrder](ctx, c.store, woKey)
	if err != nil {
		wo = models.WorkOrder{
			BaseParsed: headers.Get("file.withBase") != "true",
			HeadParsed: headers.Get("file.withHead") != "true",
			Bom:        make(map[string]models.AppOrder),
			ToDo:       make(map[string]struct{}),
		}
	}
	if isHead {
		wo.HeadParsed = true
	} else {
		wo.BaseParsed = true
	}
	for _, af := range side {
		fRes, ok := pr.Files[af.File]
		if !ok {
			fRes = models.FileResult{
				Errors: make([]string, 0),
				Apps:   make(map[string]models.AppResult),
			}
		}
		if af.Error != "" {
			fRes.Errors = append(fRes.Errors, af.Error)
			pr.Files[af.File] = fRes
			pr.Status = models.PipelineFailed
			statusChanged = true
			continue
		}
		for _, a := range af.Apps {
			app, ok := fRes.Apps[a.Name]
			if !ok {
				app = models.AppResult{
					Errors: make([]string, 0),
				}
			}
			if a.Error != "" {
				app.Errors = append(app.Errors, a.Error)
			}
			fRes.Apps[a.Name] = app
			if len(app.Errors) != 0 {
				pr.Status = models.PipelineFailed
				statusChanged = true
				continue
			}
			ao := wo.Bom[a.Name]
			ao.Origin = af.File
			if isHead {
				ao.HasHead = true
			} else {
				ao.HasBase = true
			}
			wo.Bom[a.Name] = ao
			wo.ToDo[a.Name] = struct{}{}
		}
		pr.Files[af.File] = fRes
	}
	// Apps seen on this side still wait for their render; apps missing from it
	// (added/removed document) may already have their only half rendered.
	for name, ao := range wo.Bom {
		if ready(wo, ao) {
			c.publishAppReady(ctx, headers, name, ao.Origin, ao.BaseLoc, ao.HeadLoc)
		}
	}
	if statusChanged {
		data, err := nats.Marshal(pr)
		if err != nil {
			c.log.ErrorContext(ctx, "failed to marshal pr object", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		headers.Set(keys.MsgIDHeader, keys.MsgIDDone(owner, repo, number, headers.Get("RunId")))
		c.bus.Publish(ctx, subjects.PRProcessingCompleted, headers, data)
		c.index.UpdateStatus(pr)
		elements := c.index.GetElements()
		c.store.SetValue(ctx, keys.Index, elements)
		c.notifier.Notify("index", elements)
	}
	c.store.SetValue(ctx, prKey, pr)
	c.store.SetValue(ctx, woKey, wo)
	ack()
}

func (c *Coordinator) handleRenderedManifest(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleRenderedManifest",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers.Get("pr.owner")
	repo := headers.Get("pr.repo")
	number := headers.Get("pr.number")
	headSha := headers.Get("pr.sha.head")
	sha := headers.Get("sha.active")
	appName := headers.Get("app.name")
	origin := headers.Get("app.origin")
	appErr := headers.Get("error.msg")
	manifestLocation := headers.Get("manifest.location")
	runId := headers.Get("RunId")

	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("sha.active", sha),
		attribute.String("app.name", appName),
		attribute.String("app.origin", origin),
	)
	c.log.DebugContext(ctx, "new helm.manifest.rendered event",
		"prNum", number,
		"appName", appName,
		"sha", sha,
	)

	var statusChanged bool
	prKey := keys.PR(owner, repo, number)
	woKey := keys.WorkOrder(owner, repo, number, runId)
	c.mu.Lock()
	defer c.mu.Unlock()
	wo, err := nats.GetValue[models.WorkOrder](ctx, c.store, woKey)
	if err != nil {
		c.log.ErrorContext(ctx, "failed to find work order in storage", "key", woKey)
		nak()
		return
	}
	pr, err := nats.GetValue[models.PullRequest](ctx, c.store, prKey)
	if err != nil {
		c.log.ErrorContext(ctx, "failed to find work order in storage", "key", prKey)
		nak()
		return
	}
	if appErr != "" {
		f := pr.Files[origin]
		a := f.Apps[appName]
		a.Errors = append(a.Errors, appErr)
		f.Apps[appName] = a
		pr.Files[origin] = f
		pr.Status = models.PipelineFailed
		statusChanged = true
		delete(wo.ToDo, appName)
	} else {
		ao := wo.Bom[appName]
		if sha == headSha {
			ao.HeadLoc = manifestLocation
		} else {
			ao.BaseLoc = manifestLocation
		}
		wo.Bom[appName] = ao
		if ready(wo, ao) {
			c.publishAppReady(ctx, headers, appName, ao.Origin, ao.BaseLoc, ao.HeadLoc)
		}
	}

	if statusChanged {
		data, err := nats.Marshal(pr)
		if err != nil {
			c.log.ErrorContext(ctx, "failed to marshal pr object", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		headers.Set(keys.MsgIDHeader, keys.MsgIDDone(owner, repo, number, runId))
		c.bus.Publish(ctx, subjects.PRProcessingCompleted, headers, data)
		c.index.UpdateStatus(pr)
		elements := c.index.GetElements()
		c.store.SetValue(ctx, keys.Index, elements)
		c.notifier.Notify("index", elements)
	}
	c.store.SetValue(ctx, woKey, wo)
	c.store.SetValue(ctx, prKey, pr)
	span.SetStatus(codes.Ok, "")
	ack()
}

// ready reports whether an app can be diffed: both sides are known and every
// side the app was seen on has rendered. A side it was not seen on diffs
// against an empty manifest.
func ready(wo models.WorkOrder, ao models.AppOrder) bool {
	return wo.BaseParsed && wo.HeadParsed &&
		(!ao.HasBase || ao.BaseLoc != "") &&
		(!ao.HasHead || ao.HeadLoc != "")
}

func (c *Coordinator) publishAppReady(ctx context.Context, headers nats.Headers, appName, fileName, baseManifest, headManifest string) {
	owner := headers.Get("pr.owner")
	repo := headers.Get("pr.repo")
	number := headers.Get("pr.number")
	runId := headers.Get("RunId")
	headers.Set("app.name", appName)
	headers.Set("app.origin", fileName)
	headers.Set("manifest.base.location", baseManifest)
	headers.Set("manifest.head.location", headManifest)
	headers.Set(keys.MsgIDHeader, keys.MsgIDAppReady(owner, repo, number, runId, appName))
	c.bus.Publish(ctx, subjects.CoordinatorAppReady, headers, nil)
}

func (c *Coordinator) handleGeneratedReport(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleGeneratedReport",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers.Get("pr.owner")
	repo := headers.Get("pr.repo")
	number := headers.Get("pr.number")
	appName := headers.Get("app.name")
	origin := headers.Get("app.origin")
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("app.name", appName),
		attribute.String("app.origin", origin),
	)
	c.log.DebugContext(ctx, "new diff.report.generated event",
		"owner", owner,
		"repo", repo,
		"pr", number,
		"appName", appName,
	)
	prKey := keys.PR(owner, repo, number)
	woKey := keys.WorkOrder(owner, repo, number, headers.Get("RunId"))
	c.mu.Lock()
	defer c.mu.Unlock()
	pr, err := nats.GetValue[models.PullRequest](ctx, c.store, prKey)
	if err != nil {
		c.log.ErrorContext(ctx, "failed to fetch pull request data", "error", err)
		nak()
		return
	}
	ds, err := nats.Unmarshal[models.DiffStats](data)
	if err != nil {
		c.log.ErrorContext(ctx, "failed to unmarshal diffstats", "error", err)
		nak()
		return
	}
	wo, err := nats.GetValue[models.WorkOrder](ctx, c.store, woKey)
	if err != nil {
		c.log.ErrorContext(ctx, "failed to unmarshal diffstats", "error", err)
		nak()
		return
	}
	if f, ok := pr.Files[origin]; ok {
		if a, ok := f.Apps[appName]; ok {
			a.DiffStats = ds
			f.Apps[appName] = a
		} else {
			f.Apps[appName] = models.AppResult{DiffStats: ds}
		}
	} else {
		pr.Files[origin] = models.FileResult{
			Apps: map[string]models.AppResult{
				appName: {DiffStats: ds},
			},
		}
	}

	delete(wo.ToDo, appName)
	if len(wo.ToDo) == 0 && pr.Status != models.PipelineFailed {
		pr.Status = models.PipelineSucceeded
		data, err := nats.Marshal(pr)
		if err != nil {
			c.log.ErrorContext(ctx, "failed to marshal pr object", "error", err)
			span.SetStatus(codes.Error, err.Error())
			nak()
			return
		}
		headers.Set(keys.MsgIDHeader, keys.MsgIDDone(owner, repo, number, headers.Get("RunId")))
		c.bus.Publish(ctx, subjects.PRProcessingCompleted, headers, data)
	}
	if startMs, err := strconv.ParseInt(headers.Get("start.time"), 10, 64); err == nil {
		prDuration.Record(ctx, time.Since(time.UnixMilli(startMs)).Seconds(), metric.WithAttributes(
			attribute.String("pr.owner", pr.Owner),
			attribute.String("pr.repo", pr.Repo),
			attribute.String("pr.number", pr.Number),
			attribute.String("status", "succeeded"),
		))
	} else {
		c.log.ErrorContext(ctx, "failed to parse start.time header", "error", err)
	}
	c.index.UpdateStatus(pr)
	elements := c.index.GetElements()
	c.store.SetValue(ctx, keys.Index, elements)
	c.store.SetValue(ctx, prKey, pr)
	c.store.SetValue(ctx, woKey, wo)
	c.notifier.Notify("summary:"+prKey, pr)
	c.notifier.Notify("index", elements)
	span.SetStatus(codes.Ok, "")
	ack()
}

func (c *Coordinator) handlePRClosed(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handlePRClosed",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers.Get("pr.owner")
	repo := headers.Get("pr.repo")
	number := headers.Get("pr.number")
	appName := headers.Get("app.name")
	origin := headers.Get("app.origin")
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("app.name", appName),
		attribute.String("app.origin", origin),
	)
	c.log.DebugContext(ctx, "new diff.report.generated event",
		"owner", owner,
		"repo", repo,
		"pr", number,
		"appName", appName,
	)
	c.mu.Lock()
	defer c.mu.Unlock()
	pr, err := nats.Unmarshal[models.PullRequest](data)
	if err != nil {
		c.log.ErrorContext(ctx, "failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	c.index.Delete(pr)
	elements := c.index.GetElements()
	c.store.SetValue(ctx, keys.Index, elements)
	c.notifier.Notify("index", elements)
	ack()
}
