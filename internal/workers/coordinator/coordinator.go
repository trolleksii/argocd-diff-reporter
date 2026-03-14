package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/notifications"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/coordinator")

type Coordinator struct {
	bus      *nats.Bus
	store    *nats.Store
	log      *slog.Logger
	index    *Index
	notifier *notifications.NotificationServer
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
	storedIndex := c.store.GetIndex(ctx)
	c.index = NewIndex(10, storedIndex) // TODO: get the max capacity into the config
	err := c.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "coordinator",
		MaxDeliver: 3,
		AckWait:    10 * time.Second,
		Handlers: map[string]nats.Handler{
			"helm.manifest.rendered":      c.handleRenderedManifest,
			"diff.report.generated":       c.handleGeneratedReport,
			"webhook.pr.changed":          c.handlePREvent,
			"argo.file.parsing.failed":    c.handleFileErrors,
			"helm.manifest.render.failed": c.handleAppErrors,
			"git.chart.fetch.failed":      c.handleAppErrors,
			"helm.chart.fetch.failed":     c.handleAppErrors,
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
	baseSha := headers["pr.sha.base"]
	headSha := headers["pr.sha.head"]
	errorMsg := headers["error.msg"]
	errorOrigin := headers["error.origin"]
	key := fmt.Sprintf("%s.%s.%s.%s.%", owner, repo, number, baseSha, headSha)
	pr, err := nats.GetObject[models.PullRequest](ctx, c.store, key)
	if err != nil {
		c.log.Error("failed to unmarshal files", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	if f, ok := pr.Files[errorOrigin]; ok {
		f.Errors = append(f.Errors, errorMsg)
	} else {
		pr.Files[errorOrigin] = models.FileResult{Errors: []string{errorMsg}}
	}
	c.store.StoreObject(ctx, key, pr)
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
	baseSha := headers["pr.sha.base"]
	headSha := headers["pr.sha.head"]
	errorMsg := headers["error.msg"]
	errorOriginFile := headers["error.origini.file"]
	errorOriginApp := headers["error.origin.app"]
	key := fmt.Sprintf("%s.%s.%s.%s.%", owner, repo, number, baseSha, headSha)
	pr, err := nats.GetObject[models.PullRequest](ctx, c.store, key)
	if err != nil {
		c.log.Error("failed to unmarshal files", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	if f, ok := pr.Files[errorOriginFile]; ok {
		if a, ok := f.Apps[errorOriginApp]; ok {
			a.Errors = append(a.Errors, errorMsg)
		} else {
			f.Apps[errorOriginApp] = models.App{Errors: []string{errorMsg}}
		}
	} else {
		pr.Files[errorOriginFile] = models.FileResult{
			Apps: map[string]models.App{
				errorOriginApp: models.App{Errors: []string{errorMsg}},
			},
		}
	}
	c.store.StoreObject(ctx, key, pr)
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
	c.log.Debug("coordinating webhook.pr.changed event",
		"prNum", pr.Number,
		"owner", pr.Owner,
		"repo", pr.Repo)

	key := fmt.Sprintf("%s.%s.%s.%s.%", pr.Owner, pr.Repo, pr.Number, pr.BaseSHA, pr.HeadSHA)
	c.store.StoreObject(ctx, key, data)
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

	c.log.Debug("new helm.manifest.rendered event",
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
			c.bus.Publish(ctx, "coordinator.app.ready", headers, nil)
		}
	} else {
		if _, err := nats.GetValue[string](ctx, c.store, headKey); err == nil {
			headers["app.from"] = manifestLocation
			headers["app.to"] = headKey
			c.bus.Publish(ctx, "coordinator.app.ready", headers, nil)
		}
		c.store.SetValue(ctx, baseKey, manifestLocation)
	}
	span.SetStatus(codes.Ok, "")
	ack()
}

// Index should be handler in two scenarios:
//   1. New PR event
//   2. PR processing failed
//   3. PR processing succeded

func (c *Coordinator) handleGeneratedReport(ctx context.Context, headers nats.Headers, _ []byte, ack, nak func() error) {
	// update index
	//c.notifier.Notify("index", )
	// update summary
	//c.notifier.Notify("summary:"+prNum, pr)
	// notify customers
	//c.notifier.Notify(id, diffs)
}
