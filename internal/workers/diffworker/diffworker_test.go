package diffworker

import (
	"context"
	"fmt"
	"os"
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
// handleDiffReport — DiffStats correctness
// ---------------------------------------------------------------------------

func TestHandleDiffReport_DiffStatsCorrect(t *testing.T) {
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
	)

	fromKey := "from-key-1"
	toKey := "to-key-1"

	// Seed manifests into the object store.
	require.NoError(t, store.StoreObject(ctx, fromKey, readTestdata(t, "base.yaml")))
	require.NoError(t, store.StoreObject(ctx, toKey, readTestdata(t, "head.yaml")))

	headers := internalnats.Headers{
		"pr.owner":    owner,
		"pr.repo":     repo,
		"pr.number":   number,
		"pr.sha.base": baseSha,
		"pr.sha.head": headSha,
		"app.name":    appName,
		"app.origin":  origin,
		"app.from":    fromKey,
		"app.to":      toKey,
	}

	w.handleDiffReport(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	// The expected report key used by handleDiffReport.
	reportKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s.%s", owner, repo, number, baseSha, headSha, origin, appName)
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
// handleDiffReport — report stored at expected key
// ---------------------------------------------------------------------------

func TestHandleDiffReport_ReportStoredAtExpectedKey(t *testing.T) {
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
	)

	fromKey := "from-key-42"
	toKey := "to-key-42"

	require.NoError(t, store.StoreObject(ctx, fromKey, readTestdata(t, "base.yaml")))
	require.NoError(t, store.StoreObject(ctx, toKey, readTestdata(t, "head.yaml")))

	headers := internalnats.Headers{
		"pr.owner":    owner,
		"pr.repo":     repo,
		"pr.number":   number,
		"pr.sha.base": baseSha,
		"pr.sha.head": headSha,
		"app.name":    appName,
		"app.origin":  origin,
		"app.from":    fromKey,
		"app.to":      toKey,
	}

	w.handleDiffReport(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s.%s", owner, repo, number, baseSha, headSha, origin, appName)
	_, err := internalnats.GetObject[models.Report](ctx, store, expectedKey)
	require.NoError(t, err, "report should be stored at key %q", expectedKey)
}

// ---------------------------------------------------------------------------
// handleDiffReport — diff.report.generated published upon completion
// ---------------------------------------------------------------------------

func TestHandleDiffReport_PublishesDiffReportGenerated(t *testing.T) {
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
	)

	fromKey := "from-key-pub"
	toKey := "to-key-pub"

	require.NoError(t, store.StoreObject(ctx, fromKey, readTestdata(t, "base.yaml")))
	require.NoError(t, store.StoreObject(ctx, toKey, readTestdata(t, "head.yaml")))

	reportGeneratedCh := testutil.SubscribeOnce(t, bus, subjects.DiffReportGenerated)

	headers := internalnats.Headers{
		"pr.owner":    owner,
		"pr.repo":     repo,
		"pr.number":   number,
		"pr.sha.base": baseSha,
		"pr.sha.head": headSha,
		"app.name":    appName,
		"app.origin":  origin,
		"app.from":    fromKey,
		"app.to":      toKey,
	}

	w.handleDiffReport(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-reportGeneratedCh:
		expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s.%s", owner, repo, number, baseSha, headSha, origin, appName)
		assert.Equal(t, expectedKey, hdrs["report.id"], "report.id header should carry the report store key")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DiffReportGenerated message")
	}
}

// ---------------------------------------------------------------------------
// handleDiffReport — identical manifests produce zero diff
// ---------------------------------------------------------------------------

func TestHandleDiffReport_IdenticalManifests_ZeroDiff(t *testing.T) {
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
	)

	fromKey := "from-key-same"
	toKey := "to-key-same"

	// Both sides get the same manifest content.
	manifest := readTestdata(t, "base.yaml")
	require.NoError(t, store.StoreObject(ctx, fromKey, manifest))
	require.NoError(t, store.StoreObject(ctx, toKey, manifest))

	headers := internalnats.Headers{
		"pr.owner":    owner,
		"pr.repo":     repo,
		"pr.number":   number,
		"pr.sha.base": baseSha,
		"pr.sha.head": headSha,
		"app.name":    appName,
		"app.origin":  origin,
		"app.from":    fromKey,
		"app.to":      toKey,
	}

	w.handleDiffReport(ctx, headers, nil, testutil.NoopAck, testutil.NoopNak)

	reportKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s.%s", owner, repo, number, baseSha, headSha, origin, appName)
	storedReport, err := internalnats.GetObject[models.Report](ctx, store, reportKey)
	require.NoError(t, err)
	assert.Equal(t, 0, storedReport.DiffStats.DiffCount, "identical manifests should produce zero DiffCount")
	assert.Equal(t, 0, storedReport.DiffStats.Modifications)
}

// ---------------------------------------------------------------------------
// handleDiffReport — missing from-manifest naks
// ---------------------------------------------------------------------------

func TestHandleDiffReport_MissingFromManifest_Naks(t *testing.T) {
	w, _, _ := newTestDiffWorker(t)
	ctx := context.Background()

	nakCalled := false
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"pr.owner":    "org",
		"pr.repo":     "repo",
		"pr.number":   "1",
		"pr.sha.base": "base",
		"pr.sha.head": "head",
		"app.name":    "app",
		"app.origin":  "file.yaml",
		"app.from":    "nonexistent-from-key",
		"app.to":      "nonexistent-to-key",
	}

	w.handleDiffReport(ctx, headers, nil, testutil.NoopAck, nak)
	assert.True(t, nakCalled, "nak should be called when from-manifest is not found in the object store")
}

func TestHandleDiffReport_MissingToManifest_Naks(t *testing.T) {
	w, _, store := newTestDiffWorker(t)
	ctx := context.Background()

	// Seed only the from-manifest; leave to-manifest absent.
	require.NoError(t, store.StoreObject(ctx, "from-key-missing-to", readTestdata(t, "base.yaml")))

	nakCalled := false
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"pr.owner":    "org",
		"pr.repo":     "repo",
		"pr.number":   "2",
		"pr.sha.base": "base",
		"pr.sha.head": "head",
		"app.name":    "app",
		"app.origin":  "file.yaml",
		"app.from":    "from-key-missing-to",
		"app.to":      "nonexistent-to-key",
	}

	w.handleDiffReport(ctx, headers, nil, testutil.NoopAck, nak)
	assert.True(t, nakCalled, "nak should be called when to-manifest is not found in the object store")
}

// ---------------------------------------------------------------------------
// handleDiffReport — invalid from-manifest YAML naks
// ---------------------------------------------------------------------------

func TestHandleDiffReport_InvalidFromYAML_Naks(t *testing.T) {
	w, bus, store := newTestDiffWorker(t)
	ctx := context.Background()

	// Seed base manifest as invalid YAML, head manifest as valid.
	require.NoError(t, store.StoreObject(ctx, "invalid-from-key", "{{{"))
	require.NoError(t, store.StoreObject(ctx, "valid-to-key", readTestdata(t, "head.yaml")))

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"pr.owner":    "org",
		"pr.repo":     "repo",
		"pr.number":   "10",
		"pr.sha.base": "base-inv-from",
		"pr.sha.head": "head-inv-from",
		"app.name":    "app",
		"app.origin":  "file.yaml",
		"app.from":    "invalid-from-key",
		"app.to":      "valid-to-key",
	}

	reportGeneratedCh := testutil.SubscribeOnce(t, bus, subjects.DiffReportGenerated)

	w.handleDiffReport(ctx, headers, nil, ack, nak)

	assert.True(t, nakCalled, "nak should be called when from-manifest contains invalid YAML")
	assert.False(t, ackCalled, "ack should not be called when from-manifest contains invalid YAML")

	select {
	case <-reportGeneratedCh:
		t.Fatal("DiffReportGenerated should not be published when from-manifest YAML is invalid")
	case <-time.After(200 * time.Millisecond):
		// Expected: no message published.
	}
}

// ---------------------------------------------------------------------------
// handleDiffReport — invalid to-manifest YAML naks
// ---------------------------------------------------------------------------

func TestHandleDiffReport_InvalidToYAML_Naks(t *testing.T) {
	w, bus, store := newTestDiffWorker(t)
	ctx := context.Background()

	// Seed base manifest as valid, head manifest as invalid YAML.
	require.NoError(t, store.StoreObject(ctx, "valid-from-key", readTestdata(t, "base.yaml")))
	require.NoError(t, store.StoreObject(ctx, "invalid-to-key", "{{{"))

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"pr.owner":    "org",
		"pr.repo":     "repo",
		"pr.number":   "11",
		"pr.sha.base": "base-inv-to",
		"pr.sha.head": "head-inv-to",
		"app.name":    "app",
		"app.origin":  "file.yaml",
		"app.from":    "valid-from-key",
		"app.to":      "invalid-to-key",
	}

	reportGeneratedCh := testutil.SubscribeOnce(t, bus, subjects.DiffReportGenerated)

	w.handleDiffReport(ctx, headers, nil, ack, nak)

	assert.True(t, nakCalled, "nak should be called when to-manifest contains invalid YAML")
	assert.False(t, ackCalled, "ack should not be called when to-manifest contains invalid YAML")

	select {
	case <-reportGeneratedCh:
		t.Fatal("DiffReportGenerated should not be published when to-manifest YAML is invalid")
	case <-time.After(200 * time.Millisecond):
		// Expected: no message published.
	}
}
