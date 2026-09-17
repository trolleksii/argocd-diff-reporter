package diffworker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/keys"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/notifications"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

// diffStreamSubjects lists every subject the DiffWorker publishes to or
// consumes so the test stream covers them all.
var diffStreamSubjects = []string{
	subjects.CoordinatorAppReady,
	subjects.DiffReportGenerated,
}

const diffTestStream = "diffworker-test"

// newTestDiffWorker creates a DiffWorker wired to a real embedded NATS server.
func newTestDiffWorker(t *testing.T) (*DiffWorker, *internalnats.Bus, *internalnats.Store) {
	t.Helper()
	bus, store, _ := testutil.StartNATS(t)

	ctx := context.Background()
	err := bus.EnsureStream(ctx, diffTestStream, diffStreamSubjects)
	require.NoError(t, err, "failed to create NATS test stream")

	log := testutil.NoopLogger()
	notifier := notifications.NewNotificationServer(log)
	w := New(log, bus, store, notifier)
	return w, bus, store
}

// readTestdata reads a file from the local testdata directory.
func readTestdata(t *testing.T, name string) string {
	t.Helper()
	path := fmt.Sprintf("testdata/%s", name)
	data, err := os.ReadFile(path)
	require.NoError(t, err, "read testdata file %s", path)
	return string(data)
}

// ---------------------------------------------------------------------------
// prepareDiffReport — DiffStats correctness
// ---------------------------------------------------------------------------

func TestPrepareDiffReport_DiffStatsCorrect(t *testing.T) {
	w, _, store := newTestDiffWorker(t)
	ctx := context.Background()

	const (
		owner   = "org"
		repo    = "repo"
		number  = "1"
		baseSha = "base-abc"
		headSha = "head-def"
		origin  = "apps/myapp.yaml"
		appName = "myapp"
		runId   = "run-stats"
	)

	baseLoc := keys.Manifest(owner, repo, number, baseSha, origin, appName)
	headLoc := keys.Manifest(owner, repo, number, headSha, origin, appName)

	// Seed manifests into the object store.
	require.NoError(t, store.StoreObject(ctx, baseLoc, readTestdata(t, "base.yaml")))
	require.NoError(t, store.StoreObject(ctx, headLoc, readTestdata(t, "head.yaml")))

	headers := internalnats.Headers{
		"pr.owner":               owner,
		"pr.repo":                repo,
		"pr.number":              number,
		"pr.sha.base":            baseSha,
		"pr.sha.head":            headSha,
		"app.name":               appName,
		"app.origin":             origin,
		"RunId":                  runId,
		"manifest.base.location": baseLoc,
		"manifest.head.location": headLoc,
	}

	w.prepareDiffReport(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	reportKey := keys.Report(owner, repo, number, baseSha, headSha, origin, appName)
	storedReport, err := internalnats.GetObject[models.Report](ctx, store, reportKey)
	require.NoError(t, err, "report should be stored at key %q", reportKey)

	// base.yaml has key="value-base", head.yaml has key="value-head" — one modification.
	assert.Greater(t, storedReport.DiffStats.DiffCount, 0, "DiffCount should be non-zero for different manifests")
	assert.Equal(t, owner, storedReport.Owner)
	assert.Equal(t, repo, storedReport.Repo)
	assert.Equal(t, number, storedReport.PRNumber)
	assert.Equal(t, baseSha, storedReport.BaseSHA)
	assert.Equal(t, headSha, storedReport.HeadSHA)
	assert.Equal(t, origin, storedReport.File)
	assert.Equal(t, appName, storedReport.AppName)
}

// ---------------------------------------------------------------------------
// prepareDiffReport — report stored at expected key
// ---------------------------------------------------------------------------

func TestPrepareDiffReport_ReportStoredAtExpectedKey(t *testing.T) {
	w, _, store := newTestDiffWorker(t)
	ctx := context.Background()

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "42"
		baseSha = "basesha42"
		headSha = "headsha42"
		origin  = "charts/service.yaml"
		appName = "my-service"
		runId   = "run-key"
	)

	baseLoc := keys.Manifest(owner, repo, number, baseSha, origin, appName)
	headLoc := keys.Manifest(owner, repo, number, headSha, origin, appName)

	require.NoError(t, store.StoreObject(ctx, baseLoc, readTestdata(t, "base.yaml")))
	require.NoError(t, store.StoreObject(ctx, headLoc, readTestdata(t, "head.yaml")))

	headers := internalnats.Headers{
		"pr.owner":               owner,
		"pr.repo":                repo,
		"pr.number":              number,
		"pr.sha.base":            baseSha,
		"pr.sha.head":            headSha,
		"app.name":               appName,
		"app.origin":             origin,
		"RunId":                  runId,
		"manifest.base.location": baseLoc,
		"manifest.head.location": headLoc,
	}

	w.prepareDiffReport(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	expectedKey := keys.Report(owner, repo, number, baseSha, headSha, origin, appName)
	_, err := internalnats.GetObject[models.Report](ctx, store, expectedKey)
	require.NoError(t, err, "report should be stored at key %q", expectedKey)
}

// ---------------------------------------------------------------------------
// prepareDiffReport — diff.report.generated published upon completion
// ---------------------------------------------------------------------------

func TestPrepareDiffReport_PublishesDiffReportGenerated(t *testing.T) {
	w, bus, store := newTestDiffWorker(t)
	ctx := context.Background()

	const (
		owner   = "pub-org"
		repo    = "pub-repo"
		number  = "7"
		baseSha = "base-pub"
		headSha = "head-pub"
		origin  = "apps/pub.yaml"
		appName = "pub-app"
		runId   = "run-pub"
	)

	baseLoc := keys.Manifest(owner, repo, number, baseSha, origin, appName)
	headLoc := keys.Manifest(owner, repo, number, headSha, origin, appName)

	require.NoError(t, store.StoreObject(ctx, baseLoc, readTestdata(t, "base.yaml")))
	require.NoError(t, store.StoreObject(ctx, headLoc, readTestdata(t, "head.yaml")))

	hdrCh, bodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.DiffReportGenerated)

	headers := internalnats.Headers{
		"pr.owner":               owner,
		"pr.repo":                repo,
		"pr.number":              number,
		"pr.sha.base":            baseSha,
		"pr.sha.head":            headSha,
		"app.name":               appName,
		"app.origin":             origin,
		"RunId":                  runId,
		"manifest.base.location": baseLoc,
		"manifest.head.location": headLoc,
	}

	w.prepareDiffReport(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-hdrCh:
		expectedKey := keys.Report(owner, repo, number, baseSha, headSha, origin, appName)
		assert.Equal(t, expectedKey, hdrs["report.id"], "report.id header should carry the report store key")
		assert.Equal(t, keys.MsgIDReport(owner, repo, number, runId, origin, appName), hdrs[keys.MsgIDHeader])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DiffReportGenerated message")
	}

	select {
	case body := <-bodyCh:
		stats, err := internalnats.Unmarshal[models.DiffStats](body)
		require.NoError(t, err, "DiffReportGenerated body should decode as DiffStats")
		assert.Greater(t, stats.DiffCount, 0)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DiffReportGenerated body")
	}
}

// ---------------------------------------------------------------------------
// prepareDiffReport — identical manifests produce zero diff
// ---------------------------------------------------------------------------

func TestPrepareDiffReport_IdenticalManifests_ZeroDiff(t *testing.T) {
	w, _, store := newTestDiffWorker(t)
	ctx := context.Background()

	const (
		owner   = "same-org"
		repo    = "same-repo"
		number  = "99"
		baseSha = "same-base"
		headSha = "same-head"
		origin  = "apps/same.yaml"
		appName = "same-app"
		runId   = "run-same"
	)

	baseLoc := keys.Manifest(owner, repo, number, baseSha, origin, appName)
	headLoc := keys.Manifest(owner, repo, number, headSha, origin, appName)

	// Both sides get the same manifest content.
	manifest := readTestdata(t, "base.yaml")
	require.NoError(t, store.StoreObject(ctx, baseLoc, manifest))
	require.NoError(t, store.StoreObject(ctx, headLoc, manifest))

	headers := internalnats.Headers{
		"pr.owner":               owner,
		"pr.repo":                repo,
		"pr.number":              number,
		"pr.sha.base":            baseSha,
		"pr.sha.head":            headSha,
		"app.name":               appName,
		"app.origin":             origin,
		"RunId":                  runId,
		"manifest.base.location": baseLoc,
		"manifest.head.location": headLoc,
	}

	w.prepareDiffReport(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	reportKey := keys.Report(owner, repo, number, baseSha, headSha, origin, appName)
	storedReport, err := internalnats.GetObject[models.Report](ctx, store, reportKey)
	require.NoError(t, err)
	assert.Equal(t, 0, storedReport.DiffStats.DiffCount, "identical manifests should produce zero DiffCount")
	assert.Equal(t, 0, storedReport.DiffStats.Modifications)
}

// ---------------------------------------------------------------------------
// prepareDiffReport — empty base location means "no manifest on this side"
// ---------------------------------------------------------------------------

func TestPrepareDiffReport_EmptyBaseLocation_ReportsAdditions(t *testing.T) {
	w, bus, store := newTestDiffWorker(t)
	ctx := context.Background()

	const (
		owner   = "add-org"
		repo    = "add-repo"
		number  = "5"
		baseSha = "base-add"
		headSha = "head-add"
		origin  = "apps/added.yaml"
		appName = "added-app"
		runId   = "run-add"
	)

	// Only the head side has a rendered manifest: the app was added in this PR.
	headLoc := keys.Manifest(owner, repo, number, headSha, origin, appName)
	require.NoError(t, store.StoreObject(ctx, headLoc, readTestdata(t, "head.yaml")))

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	reportGeneratedCh := testutil.SubscribeOnce(t, bus, subjects.DiffReportGenerated)

	headers := internalnats.Headers{
		"pr.owner":               owner,
		"pr.repo":                repo,
		"pr.number":              number,
		"pr.sha.base":            baseSha,
		"pr.sha.head":            headSha,
		"app.name":               appName,
		"app.origin":             origin,
		"RunId":                  runId,
		"manifest.base.location": "",
		"manifest.head.location": headLoc,
	}

	w.prepareDiffReport(ctx, headers, nil, ack, nak)

	assert.True(t, ackCalled, "ack should be called when the base side is empty")
	assert.False(t, nakCalled, "nak should not be called when the base side is empty")

	reportKey := keys.Report(owner, repo, number, baseSha, headSha, origin, appName)
	storedReport, err := internalnats.GetObject[models.Report](ctx, store, reportKey)
	require.NoError(t, err, "report should be stored at key %q", reportKey)
	assert.Greater(t, storedReport.DiffStats.Additions, 0, "an added app should be reported as additions")
	assert.Equal(t, 0, storedReport.DiffStats.Removals)

	select {
	case hdrs := <-reportGeneratedCh:
		assert.Equal(t, reportKey, hdrs["report.id"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DiffReportGenerated message")
	}
}

// ---------------------------------------------------------------------------
// prepareDiffReport — non-empty location that is missing from the store naks
// ---------------------------------------------------------------------------

func TestPrepareDiffReport_MissingBaseManifest_Naks(t *testing.T) {
	w, _, _ := newTestDiffWorker(t)
	ctx := context.Background()

	const (
		owner   = "org"
		repo    = "repo"
		number  = "1"
		baseSha = "base"
		headSha = "head"
		origin  = "file.yaml"
		appName = "app"
	)

	nakCalled := false
	nak := func() error { nakCalled = true; return nil }

	// Both locations are set but nothing was ever stored under them.
	headers := internalnats.Headers{
		"pr.owner":               owner,
		"pr.repo":                repo,
		"pr.number":              number,
		"pr.sha.base":            baseSha,
		"pr.sha.head":            headSha,
		"app.name":               appName,
		"app.origin":             origin,
		"RunId":                  "run-missing-base",
		"manifest.base.location": keys.Manifest(owner, repo, number, baseSha, origin, appName),
		"manifest.head.location": keys.Manifest(owner, repo, number, headSha, origin, appName),
	}

	w.prepareDiffReport(ctx, headers, nil, testutil.NoopAck, nak)
	assert.True(t, nakCalled, "nak should be called when base manifest is not found in the object store")
}

func TestPrepareDiffReport_MissingHeadManifest_Naks(t *testing.T) {
	w, _, store := newTestDiffWorker(t)
	ctx := context.Background()

	const (
		owner   = "org"
		repo    = "repo"
		number  = "2"
		baseSha = "base"
		headSha = "head"
		origin  = "file.yaml"
		appName = "app"
	)

	// Seed only the base manifest; leave the head manifest absent.
	baseLoc := keys.Manifest(owner, repo, number, baseSha, origin, appName)
	require.NoError(t, store.StoreObject(ctx, baseLoc, readTestdata(t, "base.yaml")))

	nakCalled := false
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"pr.owner":               owner,
		"pr.repo":                repo,
		"pr.number":              number,
		"pr.sha.base":            baseSha,
		"pr.sha.head":            headSha,
		"app.name":               appName,
		"app.origin":             origin,
		"RunId":                  "run-missing-head",
		"manifest.base.location": baseLoc,
		"manifest.head.location": keys.Manifest(owner, repo, number, headSha, origin, appName),
	}

	w.prepareDiffReport(ctx, headers, nil, testutil.NoopAck, nak)
	assert.True(t, nakCalled, "nak should be called when head manifest is not found in the object store")
}

// ---------------------------------------------------------------------------
// prepareDiffReport — invalid base manifest YAML naks
// ---------------------------------------------------------------------------

func TestPrepareDiffReport_InvalidBaseYAML_Naks(t *testing.T) {
	w, bus, store := newTestDiffWorker(t)
	ctx := context.Background()

	const (
		owner   = "org"
		repo    = "repo"
		number  = "10"
		baseSha = "base-inv-base"
		headSha = "head-inv-base"
		origin  = "file.yaml"
		appName = "app"
	)

	baseLoc := keys.Manifest(owner, repo, number, baseSha, origin, appName)
	headLoc := keys.Manifest(owner, repo, number, headSha, origin, appName)

	// Seed base manifest as invalid YAML, head manifest as valid.
	require.NoError(t, store.StoreObject(ctx, baseLoc, "{{{"))
	require.NoError(t, store.StoreObject(ctx, headLoc, readTestdata(t, "head.yaml")))

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"pr.owner":               owner,
		"pr.repo":                repo,
		"pr.number":              number,
		"pr.sha.base":            baseSha,
		"pr.sha.head":            headSha,
		"app.name":               appName,
		"app.origin":             origin,
		"RunId":                  "run-invalid-base",
		"manifest.base.location": baseLoc,
		"manifest.head.location": headLoc,
	}

	reportGeneratedCh := testutil.SubscribeOnce(t, bus, subjects.DiffReportGenerated)

	w.prepareDiffReport(ctx, headers, nil, ack, nak)

	assert.True(t, nakCalled, "nak should be called when base manifest contains invalid YAML")
	assert.False(t, ackCalled, "ack should not be called when base manifest contains invalid YAML")

	select {
	case <-reportGeneratedCh:
		t.Fatal("DiffReportGenerated should not be published when base manifest YAML is invalid")
	case <-time.After(200 * time.Millisecond):
		// Expected: no message published.
	}
}

// ---------------------------------------------------------------------------
// prepareDiffReport — invalid head manifest YAML naks
// ---------------------------------------------------------------------------

func TestPrepareDiffReport_InvalidHeadYAML_Naks(t *testing.T) {
	w, bus, store := newTestDiffWorker(t)
	ctx := context.Background()

	const (
		owner   = "org"
		repo    = "repo"
		number  = "11"
		baseSha = "base-inv-head"
		headSha = "head-inv-head"
		origin  = "file.yaml"
		appName = "app"
	)

	baseLoc := keys.Manifest(owner, repo, number, baseSha, origin, appName)
	headLoc := keys.Manifest(owner, repo, number, headSha, origin, appName)

	// Seed base manifest as valid, head manifest as invalid YAML.
	require.NoError(t, store.StoreObject(ctx, baseLoc, readTestdata(t, "base.yaml")))
	require.NoError(t, store.StoreObject(ctx, headLoc, "{{{"))

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"pr.owner":               owner,
		"pr.repo":                repo,
		"pr.number":              number,
		"pr.sha.base":            baseSha,
		"pr.sha.head":            headSha,
		"app.name":               appName,
		"app.origin":             origin,
		"RunId":                  "run-invalid-head",
		"manifest.base.location": baseLoc,
		"manifest.head.location": headLoc,
	}

	reportGeneratedCh := testutil.SubscribeOnce(t, bus, subjects.DiffReportGenerated)

	w.prepareDiffReport(ctx, headers, nil, ack, nak)

	assert.True(t, nakCalled, "nak should be called when head manifest contains invalid YAML")
	assert.False(t, ackCalled, "ack should not be called when head manifest contains invalid YAML")

	select {
	case <-reportGeneratedCh:
		t.Fatal("DiffReportGenerated should not be published when head manifest YAML is invalid")
	case <-time.After(200 * time.Millisecond):
		// Expected: no message published.
	}
}
