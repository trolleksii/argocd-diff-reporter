package helmworker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/helm"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

// helmStreamSubjects lists every subject the HelmWorker publishes to or
// consumes so the test stream covers them all.
var helmStreamSubjects = []string{
	subjects.ArgoHelmOCIParsed,
	subjects.ArgoHelmHTTPParsed,
	subjects.ArgoEmptyParsed,
	subjects.HelmChartFetched,
	subjects.GitChartFetched,
	subjects.HelmChartFetchFailed,
	subjects.HelmManifestRendered,
	subjects.HelmManifestRenderFailed,
}

const helmTestStream = "helmworker-test"

// newTestHelmWorker creates a HelmWorker wired to a real embedded NATS server.
func newTestHelmWorker(t *testing.T) (*HelmWorker, *internalnats.Bus, *internalnats.Store) {
	t.Helper()
	bus, store, _ := testutil.StartNATS(t)

	ctx := context.Background()
	err := bus.EnsureStream(ctx, helmTestStream, helmStreamSubjects)
	require.NoError(t, err, "failed to create NATS test stream")

	log := testutil.NoopLogger()
	cc := helm.NewCredsCache()
	w := New(config.HelmWorkerConfig{}, log, bus, store, cc)
	return w, bus, store
}

// testdataChartPath returns the absolute path to internal/helm/testdata/mychart.
// go test sets cwd to the package directory, so we traverse up from there.
func testdataChartPath(t *testing.T) string {
	t.Helper()
	// This test file lives in internal/workers/helmworker; the chart is at
	// internal/helm/testdata/mychart. Resolve relative to the module root via
	// the known relative path from the package directory.
	wd, err := os.Getwd()
	require.NoError(t, err)
	// wd = .../internal/workers/helmworker when running under go test
	chartPath := filepath.Join(wd, "..", "..", "helm", "testdata", "mychart")
	absPath, err := filepath.Abs(chartPath)
	require.NoError(t, err)
	return absPath
}

// testdataBadChartPath returns the absolute path to internal/helm/testdata/badchart.
// go test sets cwd to the package directory, so we traverse up from there.
func testdataBadChartPath(t *testing.T) string {
	t.Helper()
	// This test file lives in internal/workers/helmworker; the chart is at
	// internal/helm/testdata/badchart. Resolve relative to the module root via
	// the known relative path from the package directory.
	wd, err := os.Getwd()
	require.NoError(t, err)
	// wd = .../internal/workers/helmworker when running under go test
	chartPath := filepath.Join(wd, "..", "..", "helm", "testdata", "badchart")
	absPath, err := filepath.Abs(chartPath)
	require.NoError(t, err)
	return absPath
}

// ---------------------------------------------------------------------------
// handleChartFetch — publishes HelmChartFetched on success
// ---------------------------------------------------------------------------

func TestHandleChartFetch_PublishesChartFetched(t *testing.T) {
	w, bus, _ := newTestHelmWorker(t)
	ctx := context.Background()

	fixedPath := t.TempDir()
	stubFetchFn := func(ref, revision string, creds helm.CredsProvider, cache *helm.HelmChartCache) (string, error) {
		return fixedPath, nil
	}

	const (
		owner   = "fetchorg"
		repo    = "fetchrepo"
		number  = "7"
		origin  = "apps/svc.yaml"
		appName = "svc"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			RepoURL:   "https://example.com/charts",
			ChartName: "mychart",
			Revision:  "1.0.0",
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":   owner,
		"pr.repo":    repo,
		"pr.number":  number,
		"sha.active": "sha-fetch",
		"app.origin": origin,
	}

	chartFetchedCh := testutil.SubscribeOnce(t, bus, subjects.HelmChartFetched)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleChartFetch(stubFetchFn)(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful fetch")
	assert.False(t, nakCalled, "nak should not be called on successful fetch")

	select {
	case hdrs := <-chartFetchedCh:
		assert.Equal(t, fixedPath, hdrs["chart.location"], "chart.location should be the path returned by fetchFn")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for HelmChartFetched message")
	}
}

// ---------------------------------------------------------------------------
// handleChartFetch — fetch error publishes HelmChartFetchFailed and acks
// ---------------------------------------------------------------------------

func TestHandleChartFetch_FetchError_PublishesFailure(t *testing.T) {
	w, bus, _ := newTestHelmWorker(t)
	ctx := context.Background()

	fetchErr := errors.New("fetch failed: network timeout")
	failingFetchFn := func(ref, revision string, creds helm.CredsProvider, cache *helm.HelmChartCache) (string, error) {
		return "", fetchErr
	}

	const appName = "broken-app"
	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			RepoURL:   "https://example.com/charts",
			ChartName: "broken-chart",
			Revision:  "0.1.0",
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":   "org",
		"pr.repo":    "repo",
		"pr.number":  "99",
		"sha.active": "sha-fail",
		"app.origin": "apps/broken.yaml",
	}

	chartFetchFailedCh := testutil.SubscribeOnce(t, bus, subjects.HelmChartFetchFailed)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleChartFetch(failingFetchFn)(ctx, headers, data, ack, nak)

	// The implementation calls ack() (not nak()) on fetch error — this is
	// intentional: the message has been processed and the error is published
	// as a separate failure event.
	assert.True(t, ackCalled, "ack should be called even when fetch fails (error is reported via event)")
	assert.False(t, nakCalled, "nak should not be called on fetch error")

	select {
	case hdrs := <-chartFetchFailedCh:
		assert.Equal(t, fetchErr.Error(), hdrs["error.msg"], "error.msg header should contain the fetch error")
		assert.Equal(t, appName, hdrs["error.origin.app"], "error.origin.app header should carry the app name")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for HelmChartFetchFailed message")
	}
}

// ---------------------------------------------------------------------------
// handleChartRender — renders real chart, stores result, publishes event
// ---------------------------------------------------------------------------

// TestHandleChartRender_RendersAndStores exercises the full render path using
// the fixture chart at internal/helm/testdata/mychart, verifying that the
// rendered manifest is stored and a HelmManifestRendered event is published.
func TestHandleChartRender_RendersAndStores(t *testing.T) {
	chartDir := testdataChartPath(t)

	// Verify the chart directory exists and is a valid chart before proceeding.
	if _, statErr := os.Stat(filepath.Join(chartDir, "Chart.yaml")); statErr != nil {
		t.Skipf("testdata chart not found at %s: %v", chartDir, statErr)
	}

	w, bus, store := newTestHelmWorker(t)
	ctx := context.Background()

	const (
		owner   = "renderorg"
		repo    = "renderrepo"
		number  = "5"
		sha     = "sha-render"
		origin  = "apps/render.yaml"
		appName = "render-app"
	)

	spec := models.AppSpec{
		AppName:   appName,
		Namespace: "default",
		Source: models.AppSource{
			RepoURL:   "https://example.com/charts",
			ChartName: "mychart",
			Revision:  "0.1.0",
		},
		Helm: models.HelmSpec{
			ReleaseName: "test-release",
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":       owner,
		"pr.repo":        repo,
		"pr.number":      number,
		"sha.active":     sha,
		"app.origin":     origin,
		"chart.location": chartDir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.HelmManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleChartRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful render")
	assert.False(t, nakCalled, "nak should not be called on successful render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for HelmManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleChartRender — render failure publishes HelmManifestRenderFailed and acks
// ---------------------------------------------------------------------------

// TestHandleChartRender_RenderFailurePublishesEvent drives handleChartRender
// into its render-failure branch by pointing chart.location at the malformed
// badchart fixture. It asserts that the handler acks (not naks), publishes a
// HelmManifestRenderFailed event with the expected error headers, and does
// not write any object to the store.
func TestHandleChartRender_RenderFailurePublishesEvent(t *testing.T) {
	chartDir := testdataBadChartPath(t)

	// Sanity-check the fixture is present before using it.
	if _, statErr := os.Stat(filepath.Join(chartDir, "Chart.yaml")); statErr != nil {
		t.Skipf("testdata badchart not found at %s: %v", chartDir, statErr)
	}

	w, bus, store := newTestHelmWorker(t)
	ctx := context.Background()

	const (
		owner   = "failorg"
		repo    = "failrepo"
		number  = "9"
		sha     = "sha-fail"
		origin  = "apps/fail.yaml"
		appName = "fail-app"
	)

	spec := models.AppSpec{
		AppName:   appName,
		Namespace: "default",
		Source: models.AppSource{
			RepoURL:   "https://example.com/charts",
			ChartName: "badchart",
			Revision:  "0.1.0",
		},
		Helm: models.HelmSpec{
			ReleaseName: "fail-release",
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":       owner,
		"pr.repo":        repo,
		"pr.number":      number,
		"sha.active":     sha,
		"app.origin":     origin,
		"chart.location": chartDir,
	}

	renderFailedCh := testutil.SubscribeOnce(t, bus, subjects.HelmManifestRenderFailed)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleChartRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called even when render fails (error is reported via event)")
	assert.False(t, nakCalled, "nak should not be called on render failure")

	select {
	case hdrs := <-renderFailedCh:
		assert.NotEmpty(t, hdrs["error.msg"], "error.msg header must carry the render error")
		assert.Equal(t, appName, hdrs["error.origin.app"], "error.origin.app header must carry the app name")
		assert.Equal(t, origin, hdrs["error.origin.file"], "error.origin.file header must carry the app origin")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for HelmManifestRenderFailed message")
	}

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	_, getErr := internalnats.GetObject[string](ctx, store, expectedKey)
	assert.Error(t, getErr, "no manifest should be stored when render fails")
}

func TestHandleChartRender_WithValues_OverridesReflected(t *testing.T) {
	chartDir := testdataChartPath(t)

	if _, statErr := os.Stat(filepath.Join(chartDir, "Chart.yaml")); statErr != nil {
		t.Skipf("testdata chart not found at %s: %v", chartDir, statErr)
	}

	w, bus, store := newTestHelmWorker(t)
	ctx := context.Background()

	const (
		owner   = "valuesorg"
		repo    = "valuesrepo"
		number  = "10"
		sha     = "sha-values"
		origin  = "apps/values.yaml"
		appName = "values-app"
	)

	spec := models.AppSpec{
		AppName:   appName,
		Namespace: "default",
		Source: models.AppSource{
			RepoURL:   "https://example.com/charts",
			ChartName: "mychart",
			Revision:  "0.1.0",
		},
		Helm: models.HelmSpec{
			ReleaseName: "values-test",
			Values:      map[string]any{"key": "overridden"},
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":       owner,
		"pr.repo":        repo,
		"pr.number":      number,
		"sha.active":     sha,
		"app.origin":     origin,
		"chart.location": chartDir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.HelmManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleChartRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful render")
	assert.False(t, nakCalled, "nak should not be called on successful render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.Contains(t, stored, "key: overridden", "rendered manifest should reflect the values override")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for HelmManifestRendered message")
	}
}
