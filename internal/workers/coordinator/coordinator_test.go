package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/notifications"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

// coordinatorStreamName is used for all coordinator integration tests.
const coordinatorStreamName = "coordinator-test"

// allSubjects lists every NATS subject the coordinator publishes or consumes,
// so the test stream covers them all.
var allSubjects = []string{
	subjects.WebhookPRChanged,
	subjects.ArgoEmptyParsed,
	subjects.HelmManifestRendered,
	subjects.DiffReportGenerated,
	subjects.ArgoFileParseFailed,
	subjects.ArgoTotalUpdated,
	subjects.HelmManifestRenderFailed,
	subjects.GitChartFetchFailed,
	subjects.CoordinatorAppReady,
}

// newTestCoordinator creates a Coordinator with a real Bus/Store and an
// initialized Index. The caller is responsible for any cleanup registered by
// testutil.StartNATS.
func newTestCoordinator(t *testing.T) (*Coordinator, *internalnats.Bus, *internalnats.Store) {
	t.Helper()
	bus, store, _ := testutil.StartNATS(t)

	ctx := context.Background()
	err := bus.EnsureStream(ctx, coordinatorStreamName, allSubjects)
	require.NoError(t, err, "failed to create NATS stream")

	notifier := notifications.NewNotificationServer(testutil.NoopLogger())
	c := New(testutil.NoopLogger(), bus, store, notifier)
	c.index = NewIndex(10, nil) // initialise index as Run() would
	return c, bus, store
}

// ---------------------------------------------------------------------------
// handlePREvent
// ---------------------------------------------------------------------------

func TestHandlePREvent_StoresPRInKV(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()

	prModel := models.PullRequest{
		Number:  "42",
		Owner:   "org",
		Repo:    "repo",
		Title:   "My PR",
		BaseSHA: "base-sha",
		HeadSHA: "head-sha",
		Status:  models.PipelineInProgress,
		Files:   map[string]models.FileResult{},
	}
	data, err := internalnats.Marshal(prModel)
	require.NoError(t, err)

	headers := internalnats.Headers{}
	c.handlePREvent(ctx, headers, data, testutil.NoopAck, testutil.NoopNak)

	key := "org.repo.42"
	stored, err := internalnats.GetValue[models.PullRequest](ctx, store, key)
	require.NoError(t, err, "PR should be stored in KV under key %q", key)
	assert.Equal(t, prModel.Number, stored.Number)
	assert.Equal(t, prModel.Owner, stored.Owner)
	assert.Equal(t, prModel.Repo, stored.Repo)
	assert.Equal(t, prModel.Title, stored.Title)
	assert.Equal(t, prModel.BaseSHA, stored.BaseSHA)
	assert.Equal(t, prModel.HeadSHA, stored.HeadSHA)
}

func TestHandlePREvent_UpdatesIndex(t *testing.T) {
	c, _, _ := newTestCoordinator(t)
	ctx := context.Background()

	prModel := models.PullRequest{
		Number: "7",
		Owner:  "org",
		Repo:   "repo",
		Status: models.PipelineInProgress,
		Files:  map[string]models.FileResult{},
	}
	data, err := internalnats.Marshal(prModel)
	require.NoError(t, err)

	c.handlePREvent(ctx, internalnats.Headers{}, data, testutil.NoopAck, testutil.NoopNak)

	elems := c.index.GetElements()
	require.Len(t, elems, 1)
	assert.Equal(t, "7", elems[0].Number)
}

// ---------------------------------------------------------------------------
// handleRenderedManifest — head arrives first, then base
// ---------------------------------------------------------------------------

func TestHandleRenderedManifest_HeadFirst_PublishesAppReady(t *testing.T) {
	c, bus, _ := newTestCoordinator(t)
	ctx := context.Background()

	appReadyCh := testutil.SubscribeOnce(t, bus, subjects.CoordinatorAppReady)

	const (
		owner   = "org"
		repo    = "repo"
		number  = "1"
		baseSha = "base-abc"
		headSha = "head-def"
		origin  = "apps/myapp.yaml"
		appName = "myapp"
		baseLoc = "manifests/base/myapp"
		headLoc = "manifests/head/myapp"
	)

	// 1. Head manifest arrives first — no publish yet
	headHeaders := internalnats.Headers{
		"pr.owner":          owner,
		"pr.repo":           repo,
		"pr.number":         number,
		"pr.sha.base":       baseSha,
		"pr.sha.head":       headSha,
		"sha.active":        headSha,
		"app.name":          appName,
		"app.origin":        origin,
		"manifest.location": headLoc,
	}
	c.handleRenderedManifest(ctx, headHeaders, nil, testutil.NoopAck, testutil.NoopNak)

	select {
	case <-appReadyCh:
		t.Fatal("expected no coordinator.app.ready message when only head has arrived")
	case <-time.After(150 * time.Millisecond):
		// correct — no publish yet
	}

	// 2. Base manifest arrives — both sides present, should publish coordinator.app.ready
	baseHeaders := internalnats.Headers{
		"pr.owner":          owner,
		"pr.repo":           repo,
		"pr.number":         number,
		"pr.sha.base":       baseSha,
		"pr.sha.head":       headSha,
		"sha.active":        baseSha,
		"app.name":          appName,
		"app.origin":        origin,
		"manifest.location": baseLoc,
	}
	c.handleRenderedManifest(ctx, baseHeaders, nil, testutil.NoopAck, testutil.NoopNak)

	// When base arrives after head is already stored:
	//   app.from = the incoming base manifestLocation
	//   app.to   = headKey (the KV key under which head was previously stored)
	expectedHeadKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, headSha, origin, appName)
	select {
	case hdrs := <-appReadyCh:
		assert.Equal(t, baseLoc, hdrs["app.from"], "app.from should be the incoming base manifest location")
		assert.Equal(t, expectedHeadKey, hdrs["app.to"], "app.to should be the KV key of the stored head manifest")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for coordinator.app.ready message")
	}
}

// ---------------------------------------------------------------------------
// handleRenderedManifest — base arrives first, then head
// ---------------------------------------------------------------------------

func TestHandleRenderedManifest_BaseFirst_PublishesAppReady(t *testing.T) {
	c, bus, _ := newTestCoordinator(t)
	ctx := context.Background()

	appReadyCh := testutil.SubscribeOnce(t, bus, subjects.CoordinatorAppReady)

	const (
		owner   = "org"
		repo    = "repo"
		number  = "2"
		baseSha = "base-111"
		headSha = "head-222"
		origin  = "apps/chart.yaml"
		appName = "chart-app"
		baseLoc = "manifests/base/chart-app"
		headLoc = "manifests/head/chart-app"
	)

	// 1. Base manifest arrives first — no publish yet
	baseHeaders := internalnats.Headers{
		"pr.owner":          owner,
		"pr.repo":           repo,
		"pr.number":         number,
		"pr.sha.base":       baseSha,
		"pr.sha.head":       headSha,
		"sha.active":        baseSha,
		"app.name":          appName,
		"app.origin":        origin,
		"manifest.location": baseLoc,
	}
	c.handleRenderedManifest(ctx, baseHeaders, nil, testutil.NoopAck, testutil.NoopNak)

	select {
	case <-appReadyCh:
		t.Fatal("expected no coordinator.app.ready message when only base has arrived")
	case <-time.After(150 * time.Millisecond):
		// correct — no publish yet
	}

	// 2. Head manifest arrives — both sides present, should publish coordinator.app.ready
	headHeaders := internalnats.Headers{
		"pr.owner":          owner,
		"pr.repo":           repo,
		"pr.number":         number,
		"pr.sha.base":       baseSha,
		"pr.sha.head":       headSha,
		"sha.active":        headSha,
		"app.name":          appName,
		"app.origin":        origin,
		"manifest.location": headLoc,
	}
	c.handleRenderedManifest(ctx, headHeaders, nil, testutil.NoopAck, testutil.NoopNak)

	// When head arrives after base is already stored:
	//   app.from = baseKey (the KV key under which base was previously stored)
	//   app.to   = the incoming head manifestLocation
	expectedBaseKey2 := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, baseSha, origin, appName)
	select {
	case hdrs := <-appReadyCh:
		assert.Equal(t, expectedBaseKey2, hdrs["app.from"], "app.from should be the KV key of the stored base manifest")
		assert.Equal(t, headLoc, hdrs["app.to"], "app.to should be the incoming head manifest location")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for coordinator.app.ready message")
	}
}

// ---------------------------------------------------------------------------
// handleGeneratedReport — marks pipeline complete
// ---------------------------------------------------------------------------

func TestHandleGeneratedReport_MarkesPipelineComplete(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()

	const (
		owner   = "org"
		repo    = "repo"
		number  = "10"
		baseSha = "base-sha-x"
		headSha = "head-sha-x"
		origin  = "apps/app.yaml"
		appName = "myservice"
	)

	// Seed a PR in KV.
	prModel := models.PullRequest{
		Number:  number,
		Owner:   owner,
		Repo:    repo,
		BaseSHA: baseSha,
		HeadSHA: headSha,
		Status:  models.PipelineInProgress,
		Files:   map[string]models.FileResult{},
	}
	err := store.SetValue(ctx, fmt.Sprintf("%s.%s.%s", owner, repo, number), prModel)
	require.NoError(t, err)

	// Seed the progress: TotalApps=1, ProcessedApps=0.
	progressKey := fmt.Sprintf("%s.%s.%s.%s.%s", owner, repo, number, baseSha, headSha)
	err = store.SetValue(ctx, progressKey, models.Progress{TotalApps: 1, ProcessedApps: 0})
	require.NoError(t, err)

	// Build a DiffStats payload.
	ds := models.DiffStats{DiffCount: 3, Additions: 2, Removals: 1}
	data, err := internalnats.Marshal(ds)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":   owner,
		"pr.repo":    repo,
		"pr.number":  number,
		"app.name":   appName,
		"app.origin": origin,
	}
	c.handleGeneratedReport(ctx, headers, data, testutil.NoopAck, testutil.NoopNak)

	// PR should now have Status=PipelineSucceeded.
	prKey := fmt.Sprintf("%s.%s.%s", owner, repo, number)
	stored, err := internalnats.GetValue[models.PullRequest](ctx, store, prKey)
	require.NoError(t, err)
	assert.Equal(t, models.PipelineSucceeded, stored.Status, "pipeline should be marked succeeded when all apps are processed")
}

func TestHandleGeneratedReport_PartialProgress_StaysInProgress(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()

	const (
		owner   = "org"
		repo    = "repo"
		number  = "11"
		baseSha = "base-sha-y"
		headSha = "head-sha-y"
		origin  = "apps/multi.yaml"
		appName = "svc-a"
	)

	prModel := models.PullRequest{
		Number:  number,
		Owner:   owner,
		Repo:    repo,
		BaseSHA: baseSha,
		HeadSHA: headSha,
		Status:  models.PipelineInProgress,
		Files:   map[string]models.FileResult{},
	}
	err := store.SetValue(ctx, fmt.Sprintf("%s.%s.%s", owner, repo, number), prModel)
	require.NoError(t, err)

	// Two apps total; processing the first should leave status in progress.
	progressKey := fmt.Sprintf("%s.%s.%s.%s.%s", owner, repo, number, baseSha, headSha)
	err = store.SetValue(ctx, progressKey, models.Progress{TotalApps: 2, ProcessedApps: 0})
	require.NoError(t, err)

	ds := models.DiffStats{DiffCount: 1}
	data, err := internalnats.Marshal(ds)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":   owner,
		"pr.repo":    repo,
		"pr.number":  number,
		"app.name":   appName,
		"app.origin": origin,
	}
	c.handleGeneratedReport(ctx, headers, data, testutil.NoopAck, testutil.NoopNak)

	prKey := fmt.Sprintf("%s.%s.%s", owner, repo, number)
	stored, err := internalnats.GetValue[models.PullRequest](ctx, store, prKey)
	require.NoError(t, err)
	assert.Equal(t, models.PipelineInProgress, stored.Status, "pipeline should remain in progress when not all apps are processed")
}

// ---------------------------------------------------------------------------
// handleFileErrors — error aggregation
// ---------------------------------------------------------------------------

func TestHandleFileErrors_AggregatesErrorAndSetsFailed(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()

	const (
		owner    = "org"
		repo     = "repo"
		number   = "20"
		origin   = "apps/broken.yaml"
		errorMsg = "parse error: unexpected token"
	)

	// Seed a PR in KV.
	prModel := models.PullRequest{
		Number: number,
		Owner:  owner,
		Repo:   repo,
		Status: models.PipelineInProgress,
		Files:  map[string]models.FileResult{},
	}
	err := store.SetValue(ctx, fmt.Sprintf("%s.%s.%s", owner, repo, number), prModel)
	require.NoError(t, err)
	// Seed in index too so UpdateStatus can find the PR.
	c.index.Update(prModel)

	headers := internalnats.Headers{
		"pr.owner":     owner,
		"pr.repo":      repo,
		"pr.number":    number,
		"error.msg":    errorMsg,
		"error.origin": origin,
	}
	c.handleFileErrors(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	prKey := fmt.Sprintf("%s.%s.%s", owner, repo, number)
	stored, err := internalnats.GetValue[models.PullRequest](ctx, store, prKey)
	require.NoError(t, err)
	assert.Equal(t, models.PipelineFailed, stored.Status, "pipeline should be marked failed on file error")
	require.NotNil(t, stored.Files[origin])
	assert.Contains(t, stored.Files[origin].Errors, errorMsg)
}

// ---------------------------------------------------------------------------
// handleAppErrors — error aggregation at app level
// ---------------------------------------------------------------------------

func TestHandleAppErrors_AggregatesAppErrorAndSetsFailed(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()

	const (
		owner      = "org"
		repo       = "repo"
		number     = "30"
		originFile = "apps/chart.yaml"
		originApp  = "my-chart"
		errorMsg   = "helm template error: missing value"
	)

	prModel := models.PullRequest{
		Number: number,
		Owner:  owner,
		Repo:   repo,
		Status: models.PipelineInProgress,
		Files:  map[string]models.FileResult{},
	}
	err := store.SetValue(ctx, fmt.Sprintf("%s.%s.%s", owner, repo, number), prModel)
	require.NoError(t, err)
	c.index.Update(prModel)

	headers := internalnats.Headers{
		"pr.owner":          owner,
		"pr.repo":           repo,
		"pr.number":         number,
		"error.msg":         errorMsg,
		"error.origin.file": originFile,
		"error.origin.app":  originApp,
	}
	c.handleAppErrors(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	prKey := fmt.Sprintf("%s.%s.%s", owner, repo, number)
	stored, err := internalnats.GetValue[models.PullRequest](ctx, store, prKey)
	require.NoError(t, err)
	assert.Equal(t, models.PipelineFailed, stored.Status, "pipeline should be marked failed on app error")
	require.NotNil(t, stored.Files[originFile])
	require.NotNil(t, stored.Files[originFile].Apps)
	app, ok := stored.Files[originFile].Apps[originApp]
	require.True(t, ok, "app entry should exist in file result")
	assert.Contains(t, app.Errors, errorMsg)
}

// ---------------------------------------------------------------------------
// handleEmptyManifest — stores "---" and publishes HelmManifestRendered
// ---------------------------------------------------------------------------

func TestHandleEmptyManifest_StoresAndPublishes(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	ctx := context.Background()

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "42"
		sha     = "abc123"
		origin  = "apps/myapp.yaml"
		appName = "myapp"
	)

	headers := internalnats.Headers{
		"pr.owner":   owner,
		"pr.repo":    repo,
		"pr.number":  number,
		"sha.active": sha,
		"app.name":   appName,
		"app.origin": origin,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.HelmManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	c.handleEmptyManifest(ctx, headers, nil, ack, nak)

	assert.True(t, ackCalled, "ack should be called on success")
	assert.False(t, nakCalled, "nak should not be called on success")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "object should be stored at key %q", expectedKey)
	assert.Equal(t, "---", stored, "stored manifest should be ---")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for HelmManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleTotalAppUpdate
// ---------------------------------------------------------------------------

func TestHandleTotalAppUpdate_InitializesProgress(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()

	headers := internalnats.Headers{
		"pr.owner":    "org",
		"pr.repo":     "repo",
		"pr.number":   "42",
		"pr.sha.base": "base-abc",
		"pr.sha.head": "head-def",
		"app.total":   "3",
	}

	c.handleTotalAppUpdate(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	progress, err := internalnats.GetValue[models.Progress](ctx, store, "org.repo.42.base-abc.head-def")
	require.NoError(t, err)
	assert.Equal(t, 3, progress.TotalApps)
	assert.Equal(t, 0, progress.ProcessedApps)
}

func TestHandleTotalAppUpdate_AccumulatesTotal(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()

	key := "org.repo.42.base-abc.head-def"
	err := store.SetValue(ctx, key, models.Progress{TotalApps: 3, ProcessedApps: 0})
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":    "org",
		"pr.repo":     "repo",
		"pr.number":   "42",
		"pr.sha.base": "base-abc",
		"pr.sha.head": "head-def",
		"app.total":   "2",
	}

	c.handleTotalAppUpdate(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	progress, err := internalnats.GetValue[models.Progress](ctx, store, key)
	require.NoError(t, err)
	assert.Equal(t, 5, progress.TotalApps)
	assert.Equal(t, 0, progress.ProcessedApps)
}

// TestHandleEmptyManifest_StoreFailure_Naks verifies that when the object store
// returns an error, nak is called instead of ack.
// We trigger this by shutting down the NATS server before calling the handler.
func TestHandleEmptyManifest_StoreFailure_Naks(t *testing.T) {
	bus, store, cleanup := testutil.StartNATS(t)
	ctx := context.Background()
	err := bus.EnsureStream(ctx, "coordinator-test-fail", allSubjects)
	require.NoError(t, err)

	notifier := notifications.NewNotificationServer(testutil.NoopLogger())
	c := New(testutil.NoopLogger(), bus, store, notifier)
	c.index = NewIndex(10, nil)

	headers := internalnats.Headers{
		"pr.owner":   "org",
		"pr.repo":    "repo",
		"pr.number":  "1",
		"sha.active": "sha1",
		"app.name":   "app",
		"app.origin": "file.yaml",
	}

	cleanup()

	nakCalled := false
	ackCalled := false
	nak := func() error { nakCalled = true; return nil }
	ack := func() error { ackCalled = true; return nil }

	c.handleEmptyManifest(ctx, headers, nil, ack, nak)

	assert.True(t, nakCalled, "nak should be called when store fails")
	assert.False(t, ackCalled, "ack should not be called when store fails")
}
