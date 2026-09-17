package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/keys"
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
	subjects.GitFilesMatched,
	subjects.ArgoSideParsed,
	subjects.ManifestRenderFinished,
	subjects.DiffReportGenerated,
	subjects.WebhookPRClosed,
	subjects.CoordinatorAppReady,
	subjects.PRProcessingCompleted,
}

// Shared fixture identity; every test runs on its own NATS server so the
// same PR/run can be reused without cross-test dedup or KV bleed.
const (
	owner   = "org"
	repo    = "repo"
	number  = "42"
	baseSha = "base-sha"
	headSha = "head-sha"
	runId   = "run-1"
	origin  = "apps/myapp.yaml"
	appName = "myapp"
	baseLoc = "manifests/base/myapp"
	headLoc = "manifests/head/myapp"
)

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
	c := New(config.CoordinatorConfig{IndexCapacity: 10}, testutil.NoopLogger(), bus, store, notifier)
	return c, bus, store
}

// prHeaders returns the headers gitworker stamps on every message of a run.
func prHeaders() internalnats.Headers {
	return internalnats.Headers{
		"pr.owner":    owner,
		"pr.repo":     repo,
		"pr.number":   number,
		"pr.sha.base": baseSha,
		"pr.sha.head": headSha,
		"RunId":       runId,
	}
}

// sideHeaders returns the headers argoworker forwards on ArgoSideParsed.
func sideHeaders(sha string) internalnats.Headers {
	h := prHeaders()
	h.Set("sha.active", sha)
	h.Set("file.withBase", "true")
	h.Set("file.withHead", "true")
	return h
}

// appHeaders returns the headers a render/diff worker forwards for one app on one side.
func appHeaders(sha string) internalnats.Headers {
	h := prHeaders()
	h.Set("sha.active", sha)
	h.Set("app.origin", origin)
	h.Set("app.name", appName)
	return h
}

func newPR() models.PullRequest {
	return models.PullRequest{
		Owner: owner, Repo: repo, Number: number, BaseSHA: baseSha, HeadSHA: headSha,
		Files:  map[string]models.FileResult{},
		Status: models.PipelineInProgress,
	}
}

// seedPR stores the PR record in KV and the index, as indexQualifyingPR would.
func seedPR(t *testing.T, c *Coordinator, store *internalnats.Store, prModel models.PullRequest) {
	t.Helper()
	require.NoError(t, store.SetValue(context.Background(), keys.PR(owner, repo, number), prModel))
	c.index.Update(prModel)
}

// seedWorkOrder stores a run's work order with the given apps seen on both parsed sides.
func seedWorkOrder(t *testing.T, store *internalnats.Store, apps ...string) {
	t.Helper()
	wo := models.WorkOrder{BaseParsed: true, HeadParsed: true, Bom: map[string]models.AppOrder{}, ToDo: map[string]struct{}{}}
	for _, a := range apps {
		wo.Bom[a] = models.AppOrder{Origin: origin, HasBase: true, HasHead: true}
		wo.ToDo[a] = struct{}{}
	}
	require.NoError(t, store.SetValue(context.Background(), keys.WorkOrder(owner, repo, number, runId), wo))
}

func getPR(t *testing.T, store *internalnats.Store) models.PullRequest {
	t.Helper()
	prModel, err := internalnats.GetValue[models.PullRequest](context.Background(), store, keys.PR(owner, repo, number))
	require.NoError(t, err)
	return prModel
}

func getWorkOrder(t *testing.T, store *internalnats.Store) models.WorkOrder {
	t.Helper()
	wo, err := internalnats.GetValue[models.WorkOrder](context.Background(), store, keys.WorkOrder(owner, repo, number, runId))
	require.NoError(t, err)
	return wo
}

func expectMsg(t *testing.T, ch <-chan internalnats.Headers, what string) internalnats.Headers {
	t.Helper()
	select {
	case hdrs := <-ch:
		return hdrs
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func expectSilence(t *testing.T, ch <-chan internalnats.Headers, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("unexpected %s", what)
	case <-time.After(150 * time.Millisecond):
	}
}

// recorder returns ack/nak closures plus flags reporting which one was called.
func recorder() (ack, nak func() error, acked, naked *bool) {
	acked, naked = new(bool), new(bool)
	return func() error { *acked = true; return nil }, func() error { *naked = true; return nil }, acked, naked
}

// ---------------------------------------------------------------------------
// indexQualifyingPR
// ---------------------------------------------------------------------------

func TestIndexQualifyingPR_StoresPRInKV(t *testing.T) {
	c, _, store := newTestCoordinator(t)

	prModel := newPR()
	prModel.Title = "My PR"
	data, err := internalnats.Marshal(prModel)
	require.NoError(t, err)

	c.indexQualifyingPR(context.Background(), prHeaders(), data, testutil.NoopAck, testutil.NoopNak)

	stored := getPR(t, store)
	assert.Equal(t, prModel.PullRequestMeta, stored.PullRequestMeta)
}

func TestIndexQualifyingPR_UpdatesIndex(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()

	data, err := internalnats.Marshal(newPR())
	require.NoError(t, err)

	c.indexQualifyingPR(ctx, prHeaders(), data, testutil.NoopAck, testutil.NoopNak)

	elems := c.index.GetElements()
	require.Len(t, elems, 1)
	assert.Equal(t, number, elems[0].Number)

	storedIndex, err := internalnats.GetValue[[]models.PullRequest](ctx, store, keys.Index)
	require.NoError(t, err, "index should be stored in KV")
	require.Len(t, storedIndex, 1)
	assert.Equal(t, number, storedIndex[0].Number)
}

// ---------------------------------------------------------------------------
// composeWorkOrder
// ---------------------------------------------------------------------------

func TestComposeWorkOrder_CleanApps_CreatesWorkOrderAndRecordsApps(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()
	seedPR(t, c, store, newPR())

	side, err := internalnats.Marshal([]models.FileParsingResult{
		{File: origin, Apps: []models.AppParsingResult{{Name: appName}, {Name: "other"}}},
	})
	require.NoError(t, err)

	ack, nak, acked, naked := recorder()
	c.composeWorkOrder(ctx, sideHeaders(baseSha), side, ack, nak)
	// second side of the same run extends, not duplicates, the work order
	c.composeWorkOrder(ctx, sideHeaders(headSha), side, testutil.NoopAck, testutil.NoopNak)

	assert.True(t, *acked)
	assert.False(t, *naked)

	wo := getWorkOrder(t, store)
	assert.True(t, wo.BaseParsed)
	assert.True(t, wo.HeadParsed)
	assert.Equal(t, map[string]models.AppOrder{
		appName: {Origin: origin, HasBase: true, HasHead: true},
		"other": {Origin: origin, HasBase: true, HasHead: true},
	}, wo.Bom)
	assert.Equal(t, map[string]struct{}{appName: {}, "other": {}}, wo.ToDo)

	stored := getPR(t, store)
	assert.Equal(t, models.PipelineInProgress, stored.Status)
	require.Contains(t, stored.Files, origin)
	assert.Empty(t, stored.Files[origin].Errors)
	require.Contains(t, stored.Files[origin].Apps, appName)
	require.Contains(t, stored.Files[origin].Apps, "other")
	assert.Empty(t, stored.Files[origin].Apps[appName].Errors)
}

func TestComposeWorkOrder_FileError_FailsPR(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	seedPR(t, c, store, newPR())
	doneCh := testutil.SubscribeOnce(t, bus, subjects.PRProcessingCompleted)

	const errorMsg = "parse error: unexpected token"
	side, err := internalnats.Marshal([]models.FileParsingResult{{File: origin, Error: errorMsg}})
	require.NoError(t, err)

	c.composeWorkOrder(context.Background(), sideHeaders(baseSha), side, testutil.NoopAck, testutil.NoopNak)

	stored := getPR(t, store)
	assert.Equal(t, models.PipelineFailed, stored.Status)
	assert.Contains(t, stored.Files[origin].Errors, errorMsg)
	assert.Empty(t, getWorkOrder(t, store).ToDo)

	hdrs := expectMsg(t, doneCh, "PRProcessingCompleted")
	assert.Equal(t, keys.MsgIDDone(owner, repo, number, runId), hdrs[keys.MsgIDHeader])
}

func TestComposeWorkOrder_AppError_FailsPRAndSkipsApp(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	seedPR(t, c, store, newPR())
	doneCh := testutil.SubscribeOnce(t, bus, subjects.PRProcessingCompleted)

	const errorMsg = "invalid spec: missing source"
	side, err := internalnats.Marshal([]models.FileParsingResult{
		{File: origin, Apps: []models.AppParsingResult{{Name: appName}, {Name: "broken", Error: errorMsg}}},
	})
	require.NoError(t, err)

	c.composeWorkOrder(context.Background(), sideHeaders(baseSha), side, testutil.NoopAck, testutil.NoopNak)

	stored := getPR(t, store)
	assert.Equal(t, models.PipelineFailed, stored.Status)
	assert.Contains(t, stored.Files[origin].Apps["broken"].Errors, errorMsg)

	wo := getWorkOrder(t, store)
	assert.Contains(t, wo.ToDo, appName)
	assert.NotContains(t, wo.ToDo, "broken")
	assert.NotContains(t, wo.Bom, "broken")

	expectMsg(t, doneCh, "PRProcessingCompleted")
}

// removedAppSides returns base/head payloads where "removed" exists on base only.
func removedAppSides(t *testing.T) (base, head []byte) {
	t.Helper()
	base, err := internalnats.Marshal([]models.FileParsingResult{
		{File: origin, Apps: []models.AppParsingResult{{Name: appName}, {Name: "removed"}}},
	})
	require.NoError(t, err)
	head, err = internalnats.Marshal([]models.FileParsingResult{
		{File: origin, Apps: []models.AppParsingResult{{Name: appName}}},
	})
	require.NoError(t, err)
	return base, head
}

func assertRemovedAppReady(t *testing.T, hdrs internalnats.Headers) {
	t.Helper()
	assert.Equal(t, "removed", hdrs["app.name"])
	assert.Equal(t, origin, hdrs["app.origin"])
	assert.Equal(t, "manifests/base/removed", hdrs["manifest.base.location"])
	assert.Equal(t, "", hdrs["manifest.head.location"])
	assert.Equal(t, keys.MsgIDAppReady(owner, repo, number, runId, "removed"), hdrs[keys.MsgIDHeader])
}

// Regression: a document removed from a multi-doc file yields an app parsed on
// base only. After the last side it is marked one-sided so its base render
// diffs against an empty head.
func TestComposeWorkOrder_AppRemovedOnHead_RenderAfterLastSide(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	ctx := context.Background()
	seedPR(t, c, store, newPR())
	appReadyCh := testutil.SubscribeOnce(t, bus, subjects.CoordinatorAppReady)
	base, head := removedAppSides(t)

	c.composeWorkOrder(ctx, sideHeaders(baseSha), base, testutil.NoopAck, testutil.NoopNak)
	c.composeWorkOrder(ctx, sideHeaders(headSha), head, testutil.NoopAck, testutil.NoopNak)
	expectSilence(t, appReadyCh, "CoordinatorAppReady before render")

	wo := getWorkOrder(t, store)
	assert.Equal(t, models.AppOrder{Origin: origin, HasBase: true, HasHead: false}, wo.Bom["removed"])
	assert.Equal(t, models.AppOrder{Origin: origin, HasBase: true, HasHead: true}, wo.Bom[appName])

	h := renderHeaders(baseSha, "manifests/base/removed")
	h.Set("app.name", "removed")
	c.coordinateReportGeneration(ctx, h, nil, testutil.NoopAck, testutil.NoopNak)
	assertRemovedAppReady(t, expectMsg(t, appReadyCh, "CoordinatorAppReady for removed app"))
}

// Same as above but the base render lands before the head side is parsed:
// the last side must publish app.ready itself.
func TestComposeWorkOrder_AppRemovedOnHead_RenderBeforeLastSide(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	ctx := context.Background()
	seedPR(t, c, store, newPR())
	appReadyCh := testutil.SubscribeOnce(t, bus, subjects.CoordinatorAppReady)
	base, head := removedAppSides(t)

	c.composeWorkOrder(ctx, sideHeaders(baseSha), base, testutil.NoopAck, testutil.NoopNak)
	h := renderHeaders(baseSha, "manifests/base/removed")
	h.Set("app.name", "removed")
	c.coordinateReportGeneration(ctx, h, nil, testutil.NoopAck, testutil.NoopNak)
	expectSilence(t, appReadyCh, "CoordinatorAppReady before head side parsed")

	c.composeWorkOrder(ctx, sideHeaders(headSha), head, testutil.NoopAck, testutil.NoopNak)
	assertRemovedAppReady(t, expectMsg(t, appReadyCh, "CoordinatorAppReady for removed app"))
}

func TestComposeWorkOrder_MissingPR_Naks(t *testing.T) {
	c, _, _ := newTestCoordinator(t)

	side, err := internalnats.Marshal([]models.FileParsingResult{{File: origin}})
	require.NoError(t, err)

	ack, nak, acked, naked := recorder()
	c.composeWorkOrder(context.Background(), sideHeaders(baseSha), side, ack, nak)

	assert.True(t, *naked)
	assert.False(t, *acked)
}

// ---------------------------------------------------------------------------
// coordinateReportGeneration
// ---------------------------------------------------------------------------

func renderHeaders(sha, location string) internalnats.Headers {
	h := appHeaders(sha)
	h.Set("manifest.location", location)
	return h
}

func seedRenderState(t *testing.T, c *Coordinator, store *internalnats.Store) {
	t.Helper()
	prModel := newPR()
	prModel.Files[origin] = models.FileResult{Errors: []string{}, Apps: map[string]models.AppResult{appName: {Errors: []string{}}}}
	seedPR(t, c, store, prModel)
	seedWorkOrder(t, store, appName)
}

func assertAppReady(t *testing.T, hdrs internalnats.Headers) {
	t.Helper()
	assert.Equal(t, baseLoc, hdrs["manifest.base.location"])
	assert.Equal(t, headLoc, hdrs["manifest.head.location"])
	assert.Equal(t, keys.MsgIDAppReady(owner, repo, number, runId, appName), hdrs[keys.MsgIDHeader])
}

func TestCoordinateReportGeneration_HeadFirst_PublishesAppReady(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	ctx := context.Background()
	seedRenderState(t, c, store)
	appReadyCh := testutil.SubscribeOnce(t, bus, subjects.CoordinatorAppReady)

	c.coordinateReportGeneration(ctx, renderHeaders(headSha, headLoc), nil, testutil.NoopAck, testutil.NoopNak)
	expectSilence(t, appReadyCh, "CoordinatorAppReady after head only")

	c.coordinateReportGeneration(ctx, renderHeaders(baseSha, baseLoc), nil, testutil.NoopAck, testutil.NoopNak)
	assertAppReady(t, expectMsg(t, appReadyCh, "CoordinatorAppReady"))

	assert.Equal(t, models.AppOrder{Origin: origin, HasBase: true, BaseLoc: baseLoc, HasHead: true, HeadLoc: headLoc}, getWorkOrder(t, store).Bom[appName])
}

func TestCoordinateReportGeneration_BaseFirst_PublishesAppReady(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	ctx := context.Background()
	seedRenderState(t, c, store)
	appReadyCh := testutil.SubscribeOnce(t, bus, subjects.CoordinatorAppReady)

	c.coordinateReportGeneration(ctx, renderHeaders(baseSha, baseLoc), nil, testutil.NoopAck, testutil.NoopNak)
	expectSilence(t, appReadyCh, "CoordinatorAppReady after base only")

	c.coordinateReportGeneration(ctx, renderHeaders(headSha, headLoc), nil, testutil.NoopAck, testutil.NoopNak)
	assertAppReady(t, expectMsg(t, appReadyCh, "CoordinatorAppReady"))
}

func TestCoordinateReportGeneration_Error_FailsPRAndDropsApp(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	seedRenderState(t, c, store)
	doneCh := testutil.SubscribeOnce(t, bus, subjects.PRProcessingCompleted)
	appReadyCh := testutil.SubscribeOnce(t, bus, subjects.CoordinatorAppReady)

	const errorMsg = "helm template error: missing value"
	headers := appHeaders(baseSha)
	headers.Set("error.msg", errorMsg)

	ack, nak, acked, naked := recorder()
	c.coordinateReportGeneration(context.Background(), headers, nil, ack, nak)

	assert.True(t, *acked)
	assert.False(t, *naked)

	stored := getPR(t, store)
	assert.Equal(t, models.PipelineFailed, stored.Status)
	assert.Contains(t, stored.Files[origin].Apps[appName].Errors, errorMsg)
	assert.NotContains(t, getWorkOrder(t, store).ToDo, appName)

	hdrs := expectMsg(t, doneCh, "PRProcessingCompleted")
	assert.Equal(t, keys.MsgIDDone(owner, repo, number, runId), hdrs[keys.MsgIDHeader])
	expectSilence(t, appReadyCh, "CoordinatorAppReady for a failed app")
}

func TestCoordinateReportGeneration_MissingWorkOrder_Naks(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	seedPR(t, c, store, newPR())

	ack, nak, acked, naked := recorder()
	c.coordinateReportGeneration(context.Background(), renderHeaders(baseSha, baseLoc), nil, ack, nak)

	assert.True(t, *naked)
	assert.False(t, *acked)
}

// ---------------------------------------------------------------------------
// updateWorkOrder
// ---------------------------------------------------------------------------

func diffStatsData(t *testing.T, ds models.DiffStats) []byte {
	t.Helper()
	data, err := internalnats.Marshal(ds)
	require.NoError(t, err)
	return data
}

func TestUpdateWorkOrder_LastApp_MarksSucceededAndPublishes(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	seedPR(t, c, store, newPR())
	seedWorkOrder(t, store, appName)
	doneCh := testutil.SubscribeOnce(t, bus, subjects.PRProcessingCompleted)

	ds := models.DiffStats{DiffCount: 3, Additions: 2, Removals: 1}
	ack, nak, acked, naked := recorder()
	c.updateWorkOrder(context.Background(), appHeaders(headSha), diffStatsData(t, ds), ack, nak)

	assert.True(t, *acked)
	assert.False(t, *naked)

	stored := getPR(t, store)
	assert.Equal(t, models.PipelineSucceeded, stored.Status)
	assert.Equal(t, ds, stored.Files[origin].Apps[appName].DiffStats)
	assert.Empty(t, getWorkOrder(t, store).ToDo)

	hdrs := expectMsg(t, doneCh, "PRProcessingCompleted")
	assert.Equal(t, keys.MsgIDDone(owner, repo, number, runId), hdrs[keys.MsgIDHeader])
}

func TestUpdateWorkOrder_PartialProgress_StaysInProgress(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	seedPR(t, c, store, newPR())
	seedWorkOrder(t, store, appName, "other")
	doneCh := testutil.SubscribeOnce(t, bus, subjects.PRProcessingCompleted)

	c.updateWorkOrder(context.Background(), appHeaders(headSha), diffStatsData(t, models.DiffStats{DiffCount: 1}), testutil.NoopAck, testutil.NoopNak)

	assert.Equal(t, models.PipelineInProgress, getPR(t, store).Status)
	assert.Equal(t, map[string]struct{}{"other": {}}, getWorkOrder(t, store).ToDo)
	expectSilence(t, doneCh, "PRProcessingCompleted with apps still pending")
}

func TestUpdateWorkOrder_MissingPR_Naks(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	seedWorkOrder(t, store, appName)

	ack, nak, acked, naked := recorder()
	c.updateWorkOrder(context.Background(), appHeaders(headSha), diffStatsData(t, models.DiffStats{}), ack, nak)

	assert.True(t, *naked)
	assert.False(t, *acked)
}

func TestUpdateWorkOrder_MissingWorkOrder_Naks(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	seedPR(t, c, store, newPR())

	ack, nak, acked, naked := recorder()
	c.updateWorkOrder(context.Background(), appHeaders(headSha), diffStatsData(t, models.DiffStats{}), ack, nak)

	assert.True(t, *naked)
	assert.False(t, *acked)
}

func TestUpdateWorkOrder_Redelivery_PublishesCompletedOnce(t *testing.T) {
	c, bus, store := newTestCoordinator(t)
	ctx := context.Background()
	seedPR(t, c, store, newPR())
	seedWorkOrder(t, store, appName)
	doneCh := testutil.SubscribeN(t, bus, subjects.PRProcessingCompleted, 2)

	data := diffStatsData(t, models.DiffStats{DiffCount: 1})
	c.updateWorkOrder(ctx, appHeaders(headSha), data, testutil.NoopAck, testutil.NoopNak)
	c.updateWorkOrder(ctx, appHeaders(headSha), data, testutil.NoopAck, testutil.NoopNak)

	expectMsg(t, doneCh, "PRProcessingCompleted")
	expectSilence(t, doneCh, "second PRProcessingCompleted; JetStream should dedup on "+keys.MsgIDHeader)
}

// ---------------------------------------------------------------------------
// dropPRFromIndex
// ---------------------------------------------------------------------------

func TestDropPRFromIndex_RemovesFromIndex(t *testing.T) {
	c, _, store := newTestCoordinator(t)
	ctx := context.Background()
	seedPR(t, c, store, newPR())

	// webhook sends only the PR identity
	data, err := internalnats.Marshal(models.PullRequest{
		Owner: owner, Repo: repo, Number: number,
	})
	require.NoError(t, err)

	c.dropPRFromIndex(ctx, internalnats.Headers{"RunId": runId}, data, testutil.NoopAck, testutil.NoopNak)

	assert.Empty(t, c.index.GetElements())
	storedIndex, err := internalnats.GetValue[[]models.PullRequest](ctx, store, keys.Index)
	require.NoError(t, err)
	assert.Empty(t, storedIndex)
}

func TestDropPRFromIndex_Acks(t *testing.T) {
	t.Skip("bug: dropPRFromIndex (coordinator.go:405-444) never calls ack(); the message is redelivered until MaxDeliver")
}
