package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

// Coordinator is the reactive event loop that watches artifact completion and
// performs higher-order operations:
//
//   - When base + head manifests are both ready for an app →
//     publishes coordinator.app.ready so the diff worker can act.
//
//   - When all apps in a file are diffed → updates file status in the store.
//
//   - When all files in a PR are done → updates PR summary and publishes
//     coordinator.pr.evaluated for upstream consumers (GitHub Checks, etc.).
//
// NOTE: the argo worker must emit argo.file.apps.dispatched (with header
// "appCount") after processing each file so the coordinator knows when to
// consider a file fully dispatched.
type Coordinator struct {
	bus   *nats.Bus
	store *nats.Store
	log   *slog.Logger

	mu  sync.Mutex
	prs map[string]*prState // keyed by prNum
}

// prState is the in-flight tracking state for a single pull request.
type prState struct {
	// These fields are written once at creation and are safe to read after
	// releasing the mutex.
	owner   string
	repo    string
	title   string
	author  string
	baseSha string
	headSha string

	files map[string]*fileState // keyed by fileName
}

// fileState tracks per-file progress.
type fileState struct {
	apps   map[string]*appState // keyed by appName
	errors []string

	// appCount records how many apps the argo worker dispatched for each sha.
	// Receiving counts for both baseSha and headSha signals that argo has
	// finished processing this file for both commits.
	// Keyed by sha string (baseSha or headSha value).
	appCount map[string]int
}

// appState tracks manifest readiness and diff completion for one Application.
type appState struct {
	baseManifest  string // object-store key for the base manifest
	headManifest  string // object-store key for the head manifest
	diffTriggered bool
	diffComplete  bool
	diffError     string
}

func NewCoordinator(b *nats.Bus, s *nats.Store, log *slog.Logger) *Coordinator {
	return &Coordinator{
		bus:   b,
		store: s,
		log:   log.With("component", "coordinator"),
		prs:   make(map[string]*prState),
	}
}

func (c *Coordinator) Run(ctx context.Context) error {
	err := c.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "coordinator",
		MaxDeliver: 3,
		AckWait:    10 * time.Second,
		Handlers: map[string]nats.Handler{
			// Manifest rendering completion from all render workers.
			"helm.manifest.rendered":      c.handleManifestReady,
			"kustomize.manifest.rendered": c.handleManifestReady,
			"dir.manifest.rendered":       c.handleManifestReady,

			// Argo worker signals it finished dispatching all apps for a file.
			// Headers: prNum, ref, fileName, appCount.
			"argo.file.apps.dispatched": c.handleAppsDispatched,

			// File-level errors from the argo worker.
			"argo.parsing.failed":          c.handleArgoError,
			"applicationset.render.failed": c.handleArgoError,

			// Diff worker signals report creation (or failure via diffError header).
			"diff.report.created": c.handleDiffComplete,

			// Free in-memory state when a PR is closed.
			"webhook.pr.closed": c.handlePRClosed,
		},
	})
	if err != nil {
		return fmt.Errorf("coordinator: consume: %w", err)
	}
	return nil
}

// handleManifestReady is invoked when a render worker produces a manifest.
//
// Required headers: prNum, ref, baseSha, headSha, fileName, application,
// manifestLocation.
func (c *Coordinator) handleManifestReady(ctx context.Context, headers map[string]string, _ []byte, ack, nak func() error) {
	prNum := headers["prNum"]
	ref := headers["ref"]
	baseSha := headers["baseSha"]
	headSha := headers["headSha"]
	fileName := headers["fileName"]
	appName := headers["application"]
	manifestLoc := headers["manifestLocation"]

	if prNum == "" || ref == "" || fileName == "" || appName == "" || manifestLoc == "" {
		c.log.Warn("handleManifestReady: missing required headers", "headers", headers)
		try(c.log, "ack malformed message", ack)
		return
	}

	c.mu.Lock()
	pr := c.getOrCreatePR(prNum, headers)
	file := getOrCreate(pr.files, fileName, newFileState)
	app := getOrCreate(file.apps, appName, func() *appState { return &appState{} })

	switch ref {
	case baseSha:
		app.baseManifest = manifestLoc
	case headSha:
		app.headManifest = manifestLoc
	default:
		c.log.Warn("ref matches neither baseSha nor headSha",
			"ref", ref, "baseSha", baseSha, "headSha", headSha)
	}

	shouldTrigger := app.baseManifest != "" && app.headManifest != "" && !app.diffTriggered
	if shouldTrigger {
		app.diffTriggered = true
	}
	baseM, headM := app.baseManifest, app.headManifest
	c.mu.Unlock()

	if shouldTrigger {
		h := copyHeaders(headers)
		h["baseManifest"] = baseM
		h["headManifest"] = headM
		if err := c.bus.Publish(ctx, "coordinator.app.ready", h, nil); err != nil {
			c.log.Error("failed to publish coordinator.app.ready",
				"error", err, "prNum", prNum, "app", appName)
			try(c.log, "nak after publish failure", nak)
			return
		}
		c.log.Info("diff triggered", "prNum", prNum, "fileName", fileName, "app", appName)
	}

	try(c.log, "ack manifest ready", ack)
}

// handleAppsDispatched records the number of apps dispatched by the argo worker
// for a given (prNum, ref, fileName). Once counts are received for both shas,
// the coordinator can determine file completeness.
//
// Required headers: prNum, ref, fileName, appCount.
func (c *Coordinator) handleAppsDispatched(_ context.Context, headers map[string]string, _ []byte, ack, _ func() error) {
	prNum := headers["prNum"]
	ref := headers["ref"]
	fileName := headers["fileName"]
	count, _ := strconv.Atoi(headers["appCount"])

	c.mu.Lock()
	pr := c.getOrCreatePR(prNum, headers)
	file := getOrCreate(pr.files, fileName, newFileState)
	file.appCount[ref] = count
	c.mu.Unlock()

	try(c.log, "ack apps dispatched", ack)
}

// handleArgoError records a file-level parsing or ApplicationSet render error.
//
// Required headers: prNum, fileName, error.
func (c *Coordinator) handleArgoError(ctx context.Context, headers map[string]string, _ []byte, ack, _ func() error) {
	prNum := headers["prNum"]
	fileName := headers["fileName"]
	errMsg := headers["error"]

	if prNum == "" || fileName == "" {
		try(c.log, "ack malformed error event", ack)
		return
	}

	c.mu.Lock()
	pr := c.getOrCreatePR(prNum, headers)
	file := getOrCreate(pr.files, fileName, newFileState)
	file.errors = append(file.errors, errMsg)
	c.mu.Unlock()

	if err := c.syncStore(ctx, prNum); err != nil {
		c.log.Error("syncStore after argo error", "error", err)
	}
	try(c.log, "ack argo error", ack)
}

// handleDiffComplete records that the diff worker has finished (or failed) for
// one app.
//
// Required headers: prNum, fileName, application.
// Optional headers: diffError — non-empty if the diff worker encountered an error.
func (c *Coordinator) handleDiffComplete(ctx context.Context, headers map[string]string, _ []byte, ack, _ func() error) {
	prNum := headers["prNum"]
	fileName := headers["fileName"]
	appName := headers["application"]
	diffError := headers["diffError"]

	if prNum == "" || fileName == "" || appName == "" {
		try(c.log, "ack malformed diff.report.created event", ack)
		return
	}

	c.mu.Lock()
	pr := c.getOrCreatePR(prNum, headers)
	file := getOrCreate(pr.files, fileName, newFileState)
	app := getOrCreate(file.apps, appName, func() *appState { return &appState{} })
	app.diffComplete = true
	app.diffError = diffError
	c.mu.Unlock()

	if err := c.syncStore(ctx, prNum); err != nil {
		c.log.Error("syncStore after diff complete", "error", err)
	}
	try(c.log, "ack diff complete", ack)
}

// handlePRClosed frees in-memory state when a PR is closed.
func (c *Coordinator) handlePRClosed(_ context.Context, headers map[string]string, _ []byte, ack, _ func() error) {
	prNum := headers["prNum"]
	c.mu.Lock()
	delete(c.prs, prNum)
	c.mu.Unlock()
	try(c.log, "ack pr closed", ack)
}

// syncStore snapshots the current PR state, writes it to the store, and — if
// the PR is now fully evaluated — publishes coordinator.pr.evaluated.
//
// Called outside the mutex; the summary is built under the mutex and then the
// lock is released before any IO.
func (c *Coordinator) syncStore(ctx context.Context, prNum string) error {
	c.mu.Lock()
	pr, ok := c.prs[prNum]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	summary := buildPRSummary(prNum, pr)
	complete := prComplete(pr)
	// Copy immutable fields before releasing the lock.
	owner, repo := pr.owner, pr.repo
	c.mu.Unlock()

	if err := c.store.PutSummary(ctx, summary.Number, summary); err != nil {
		return fmt.Errorf("PutSummary: %w", err)
	}

	if err := c.syncIndex(ctx, summary, complete); err != nil {
		c.log.Warn("syncIndex failed", "error", err, "prNum", prNum)
	}

	if complete {
		h := map[string]string{
			"prNum":      prNum,
			"owner":      owner,
			"repository": repo,
			"success":    strconv.FormatBool(summary.Success),
		}
		if err := c.bus.Publish(ctx, "coordinator.pr.evaluated", h, nil); err != nil {
			return fmt.Errorf("publish coordinator.pr.evaluated: %w", err)
		}
		c.log.Info("PR evaluated", "prNum", prNum, "success", summary.Success)
	}
	return nil
}

// syncIndex updates the global PR index in the store.
//
// NOTE: this is a non-atomic read-modify-write; concurrent updates for
// different PRs may interleave. A future improvement would use NATS KV
// revision-based CAS or a dedicated index-writer goroutine.
func (c *Coordinator) syncIndex(ctx context.Context, summary models.PullRequest, complete bool) error {
	status := models.PipelineInProgress
	if complete {
		if summary.Success {
			status = models.PipelineSucceeded
		} else {
			status = models.PipelineFailed
		}
	}

	idx := c.store.GetIndex(ctx)
	for i, p := range idx {
		if p.Number == summary.Number {
			idx[i].Status = status
			return c.store.PutIndex(ctx, idx)
		}
	}
	idx = append(idx, models.ProcessedPR{
		Number: summary.Number,
		Title:  summary.Title,
		Status: status,
	})
	return c.store.PutIndex(ctx, idx)
}

// ── state construction ────────────────────────────────────────────────────────

func (c *Coordinator) getOrCreatePR(prNum string, headers map[string]string) *prState {
	pr, ok := c.prs[prNum]
	if !ok {
		pr = &prState{
			owner:   headers["owner"],
			repo:    headers["repository"],
			title:   headers["title"],
			author:  headers["author"],
			baseSha: headers["baseSha"],
			headSha: headers["headSha"],
			files:   make(map[string]*fileState),
		}
		c.prs[prNum] = pr
	}
	return pr
}

func newFileState() *fileState {
	return &fileState{
		apps:     make(map[string]*appState),
		appCount: make(map[string]int),
	}
}

func getOrCreate[K comparable, V any](m map[K]V, key K, create func() V) V {
	v, ok := m[key]
	if !ok {
		v = create()
		m[key] = v
	}
	return v
}

// ── completion logic ──────────────────────────────────────────────────────────

// fileComplete returns true when:
//  1. The argo worker has finished dispatching apps for both shas
//     (appCount has two entries), AND
//  2. Every app that reached the diff-trigger stage has completed.
//
// Known limitation: apps that exist only in base or only in head (added/removed
// between commits) never reach diffTriggered=true because the coordinator
// requires both manifests. Those apps currently stall file completion. A
// future improvement would track per-sha manifest arrival and synthesise an
// empty counterpart for one-sided apps.
func fileComplete(f *fileState) bool {
	if len(f.appCount) < 2 {
		return false
	}
	for _, a := range f.apps {
		if a.diffTriggered && !a.diffComplete {
			return false
		}
	}
	return true
}

// prComplete returns true once every file in the PR has reached a terminal
// state (either complete or failed with a recorded error).
func prComplete(pr *prState) bool {
	if len(pr.files) == 0 {
		return false
	}
	for _, f := range pr.files {
		if len(f.errors) > 0 {
			continue // terminal error state
		}
		if !fileComplete(f) {
			return false
		}
	}
	return true
}

// ── summary builders ──────────────────────────────────────────────────────────

func buildPRSummary(prNum string, pr *prState) models.PullRequest {
	files := make(map[string]models.FileResult, len(pr.files))
	success := true
	for name, f := range pr.files {
		fr := buildFileResult(f)
		files[name] = fr
		if fr.Status != "success" {
			success = false
		}
	}
	return models.PullRequest{
		Number:  prNum,
		Author:  pr.author,
		Owner:   pr.owner,
		Repo:    pr.repo,
		Title:   pr.title,
		BaseSHA: pr.baseSha,
		HeadSHA: pr.headSha,
		Files:   files,
		Success: success,
	}
}

func buildFileResult(f *fileState) models.FileResult {
	apps := make(map[string]models.App, len(f.apps))
	for name, a := range f.apps {
		var errs []string
		if a.diffError != "" {
			errs = append(errs, a.diffError)
		}
		apps[name] = models.App{Errors: errs}
	}
	return models.FileResult{
		Status: fileStatus(f),
		Errors: f.errors,
		Apps:   apps,
	}
}

func fileStatus(f *fileState) string {
	if len(f.errors) > 0 {
		return "failed"
	}
	if !fileComplete(f) {
		return "in_progress"
	}
	for _, a := range f.apps {
		if a.diffError != "" {
			return "failed"
		}
	}
	return "success"
}

// ── utilities ─────────────────────────────────────────────────────────────────

func copyHeaders(h map[string]string) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		out[k] = v
	}
	return out
}

func try(log *slog.Logger, msg string, fn func() error) {
	if err := fn(); err != nil {
		log.Error(msg, "error", err)
	}
}
