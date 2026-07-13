//go:build integration

package helm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/repo"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

// credsProviderFunc adapts a plain func to CredsProvider for tests. A nil
// func yields anonymous (empty) credentials.
type credsProviderFunc func(ctx context.Context, repoURL string) (models.Creds, error)

func (f credsProviderFunc) Get(ctx context.Context, repoURL string) (models.Creds, error) {
	if f == nil {
		return models.Creds{}, nil
	}
	return f(ctx, repoURL)
}

func (f credsProviderFunc) Refresh(ctx context.Context, repoURL string) (models.Creds, error) {
	return f.Get(ctx, repoURL)
}

// cachingCredsProvider mimics the production read-through store: Get serves
// the cached value; Refresh reloads from the loader.
type cachingCredsProvider struct {
	load   func() models.Creds
	loads  int
	cached *models.Creds
}

func (p *cachingCredsProvider) Get(ctx context.Context, repoURL string) (models.Creds, error) {
	if p.cached == nil {
		return p.Refresh(ctx, repoURL)
	}
	return *p.cached, nil
}

func (p *cachingCredsProvider) Refresh(ctx context.Context, repoURL string) (models.Creds, error) {
	p.loads++
	c := p.load()
	p.cached = &c
	return c, nil
}

// renderChartLocal renders a Helm chart from a local directory without
// requiring a live Kubernetes cluster. It uses a hardcoded Kubernetes version
// so that detectKubernetesVersion is bypassed entirely.
func renderChartLocal(t *testing.T, chartPath string) string {
	t.Helper()

	settings := cli.New()
	settings.SetNamespace("default")

	actionConfig := new(action.Configuration)
	err := actionConfig.Init(settings.RESTClientGetter(), "default", "memory", func(format string, v ...interface{}) {})
	require.NoError(t, err)

	kubeVersion, err := chartutil.ParseKubeVersion("v1.30.0")
	require.NoError(t, err)

	installClient := action.NewInstall(actionConfig)
	installClient.DryRunOption = "true"
	installClient.ReleaseName = "integration-test"
	installClient.Namespace = "default"
	installClient.ClientOnly = true
	installClient.DryRun = true
	installClient.Replace = true
	installClient.KubeVersion = kubeVersion
	installClient.DisableHooks = true
	installClient.IncludeCRDs = true

	ch, err := loadAndValidateChart(chartPath)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	release, err := installClient.RunWithContext(ctx, ch, map[string]any{})
	require.NoError(t, err)
	require.NotNil(t, release)
	require.NotEmpty(t, release.Manifest)
	return release.Manifest
}

// buildTestHelmRepo creates a temporary directory containing a valid Helm
// repository (index.yaml + chart .tgz). It returns the directory path.
func buildTestHelmRepo(t *testing.T) string {
	t.Helper()

	repoDir := t.TempDir()

	// Build the chart .tgz
	tgzPath := buildTGZ(t)

	// Copy the .tgz into the repo dir as "mychart-0.1.0.tgz"
	dest := filepath.Join(repoDir, "mychart-0.1.0.tgz")
	data, err := os.ReadFile(tgzPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dest, data, 0644))

	return repoDir
}

// startHelmRepoServer starts an httptest.Server that serves a Helm chart
// repository. It returns the server and a request counter that increments for
// every HTTP request the server receives.
func startHelmRepoServer(t *testing.T, repoDir string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	return startHelmRepoServerWithAuth(t, repoDir, "", nil)
}

// startHelmRepoServerWithAuth is startHelmRepoServer with HTTP basic auth.
// An empty username disables the auth check. The accepted password is read
// through the pointer on every request, so tests can rotate it mid-flight.
func startHelmRepoServerWithAuth(t *testing.T, repoDir, username string, password *string) (*httptest.Server, *atomic.Int64) {
	t.Helper()

	var reqCount atomic.Int64

	// Generate the Helm index.yaml — the URL must match what the server will
	// serve so that LocateChart can resolve the download URL.
	// We use a placeholder URL first, generate the index, then start the server
	// with a handler that serves index.yaml + tgz files from repoDir.
	mux := http.NewServeMux()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		if username != "" {
			u, p, ok := r.BasicAuth()
			if !ok || u != username || p != *password {
				w.Header().Set("WWW-Authenticate", `Basic realm="helm"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	// Now that we know the server URL we can generate the index with correct URLs.
	indexFile, err := repo.IndexDirectory(repoDir, srv.URL)
	require.NoError(t, err)
	indexPath := filepath.Join(repoDir, "index.yaml")
	require.NoError(t, indexFile.WriteFile(indexPath, 0644))

	// Serve files from repoDir
	fileServer := http.FileServer(http.Dir(repoDir))
	mux.Handle("/", fileServer)

	return srv, &reqCount
}

// TestRenderChartWithSubChart verifies that renderChartLocal correctly renders
// both parent and sub-chart templates when a chart declares a dependency on a
// sub-chart bundled in its charts/ directory.
func TestRenderChartWithSubChart(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	chartPath := filepath.Join(wd, "testdata", "parentchart")
	manifest := renderChartLocal(t, chartPath)

	// The rendered output must contain manifests from the parent chart.
	assert.True(t, strings.Contains(manifest, "integration-test-parent-config"),
		"expected parent chart ConfigMap name in rendered output, got:\n%s", manifest)

	// The rendered output must also contain manifests from the child sub-chart.
	assert.True(t, strings.Contains(manifest, "integration-test-child-config"),
		"expected child chart ConfigMap name in rendered output, got:\n%s", manifest)
}

// TestHTTPChartFetchAndRender verifies that FetchChartHTTPS correctly downloads
// a chart from a local httptest-based Helm repository and that the resulting
// chart path contains a valid, renderable chart.
func TestHTTPChartFetchAndRender(t *testing.T) {
	repoDir := buildTestHelmRepo(t)
	srv, _ := startHelmRepoServer(t, repoDir)

	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)

	chartVersion := "0.1.0"

	chartPath, err := FetchChartHTTPS(context.Background(), srv.URL, "mychart", chartVersion, credsProviderFunc(nil), cache)
	require.NoError(t, err)
	assert.NotEmpty(t, chartPath)

	// The returned directory must contain a loadable Helm chart.
	ch, err := loadAndValidateChart(chartPath)
	require.NoError(t, err)
	require.NotNil(t, ch)
	assert.Equal(t, "mychart", ch.Metadata.Name)

	// Render using the client-only helper to confirm the chart is fully usable.
	manifest := renderChartLocal(t, chartPath)
	assert.Contains(t, manifest, "kind: ConfigMap")
	assert.Contains(t, manifest, "integration-test-config")
}

// TestHTTPChartCacheHit verifies that the second call to FetchChartHTTPS with
// identical arguments returns a cache hit without issuing any additional HTTP
// requests to the upstream repository server.
func TestHTTPChartCacheHit(t *testing.T) {
	repoDir := buildTestHelmRepo(t)
	srv, reqCount := startHelmRepoServer(t, repoDir)

	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)

	chartVersion := "0.1.0"

	// First fetch — should contact the server.
	cc := credsProviderFunc(nil)
	path1, err := FetchChartHTTPS(context.Background(), srv.URL, "mychart", chartVersion, cc, cache)
	require.NoError(t, err)
	require.NotEmpty(t, path1)

	countAfterFirst := reqCount.Load()
	assert.Positive(t, countAfterFirst, "expected HTTP requests during first fetch")

	// Second fetch — must come from cache, server must not receive new requests.
	path2, err := FetchChartHTTPS(context.Background(), srv.URL, "mychart", chartVersion, cc, cache)
	require.NoError(t, err)
	assert.Equal(t, path1, path2, "second fetch must return same cached path")

	countAfterSecond := reqCount.Load()
	assert.Equal(t, countAfterFirst, countAfterSecond,
		"no new HTTP requests expected on cache hit (before=%d, after=%d)",
		countAfterFirst, countAfterSecond)
}

// TestHTTPChartFetchWithAuth verifies that per-repo credentials are selected
// correctly when multiple private repositories are in play: two servers with
// different basic-auth creds, each fetch must authenticate with its own pair.
func TestHTTPChartFetchWithAuth(t *testing.T) {
	passA, passB := "pass-a", "pass-b"
	srvA, _ := startHelmRepoServerWithAuth(t, buildTestHelmRepo(t), "user-a", &passA)
	srvB, _ := startHelmRepoServerWithAuth(t, buildTestHelmRepo(t), "user-b", &passB)

	repoCreds := map[string]models.Creds{
		srvA.URL: {Username: "user-a", Password: passA},
		srvB.URL: {Username: "user-b", Password: passB},
	}
	cc := credsProviderFunc(func(ctx context.Context, repoURL string) (models.Creds, error) {
		return repoCreds[repoURL], nil
	})

	ctx := context.Background()
	for _, srvURL := range []string{srvA.URL, srvB.URL} {
		cache, err := NewChartDiskCache(t.TempDir())
		require.NoError(t, err)
		chartPath, err := FetchChartHTTPS(ctx, srvURL, "mychart", "0.1.0", cc, cache)
		require.NoError(t, err, "fetch from %s must succeed with its own creds", srvURL)
		require.NotEmpty(t, chartPath)
	}

	// A repo with no matching creds must fail with an auth error, not silently
	// succeed anonymously.
	passC := "pass-c"
	srvC, _ := startHelmRepoServerWithAuth(t, buildTestHelmRepo(t), "user-c", &passC)
	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)
	_, err = FetchChartHTTPS(ctx, srvC.URL, "mychart", "0.1.0", cc, cache)
	require.Error(t, err, "fetch without valid creds must fail")
	assert.Contains(t, err.Error(), "401", "failure must surface the auth error")
}

// TestHTTPChartFetchCredsRefresh verifies the stale-credentials path: the
// store holds expired creds, the pull fails with 401, the fetcher refreshes
// creds from the loader and retries successfully.
func TestHTTPChartFetchCredsRefresh(t *testing.T) {
	accepted := "initial"
	srv, _ := startHelmRepoServerWithAuth(t, buildTestHelmRepo(t), "user", &accepted)

	current := "initial"
	cc := &cachingCredsProvider{load: func() models.Creds {
		return models.Creds{Username: "user", Password: current}
	}}

	ctx := context.Background()

	// Prime the cache with the currently valid password.
	_, err := cc.Get(ctx, srv.URL)
	require.NoError(t, err)
	require.Equal(t, 1, cc.loads)

	// Rotate the credential: the server now only accepts the new password, and
	// the loader (source of truth) returns it — but the store still caches the
	// old one, like an expired TTL token.
	accepted = "rotated"
	current = "rotated"

	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)
	chartPath, err := FetchChartHTTPS(ctx, srv.URL, "mychart", "0.1.0", cc, cache)
	require.NoError(t, err, "fetch must recover from stale creds by refreshing")
	require.NotEmpty(t, chartPath)
	assert.Equal(t, 2, cc.loads, "stale creds must trigger exactly one refresh")
}

// TestGitChartFetchAndRender verifies that a chart stored inside a local Git
// repository can be cloned, loaded, cached, and successfully rendered.
//
// This test simulates what the git worker does: clone the repo and hand off the
// chart directory to the Helm render pipeline.
func TestGitChartFetchAndRender(t *testing.T) {
	// 1. Create a non-bare local git repository with the fixture chart committed.
	sourceRepoDir := t.TempDir()
	sourceRepo, err := git.PlainInit(sourceRepoDir, false)
	require.NoError(t, err)

	// Copy the testdata/mychart into the source repo worktree.
	wd, err := os.Getwd()
	require.NoError(t, err)
	srcChartDir := filepath.Join(wd, "testdata", "mychart")

	chartDestDir := filepath.Join(sourceRepoDir, "mychart")
	require.NoError(t, os.MkdirAll(chartDestDir, 0755))
	require.NoError(t, copyDir(srcChartDir, chartDestDir))

	wt, err := sourceRepo.Worktree()
	require.NoError(t, err)

	_, err = wt.Add(".")
	require.NoError(t, err)

	_, err = wt.Commit("add mychart", &git.CommitOptions{
		Author: &object.Signature{
			Name:  "test",
			Email: "test@example.com",
			When:  time.Now(),
		},
		AllowEmptyCommits: false,
	})
	require.NoError(t, err)

	// 2. Clone the source repo as the "git worker" would.
	cloneDir := t.TempDir()
	_, err = git.PlainClone(cloneDir, false, &git.CloneOptions{
		URL: sourceRepoDir,
	})
	require.NoError(t, err)

	clonedChartPath := filepath.Join(cloneDir, "mychart")

	// 3. Validate the chart is loadable from the cloned directory.
	ch, err := loadAndValidateChart(clonedChartPath)
	require.NoError(t, err)
	require.NotNil(t, ch)
	assert.Equal(t, "mychart", ch.Metadata.Name)

	// 4. Cache the chart and confirm cache hit works.
	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)

	cacheKey := GenerateCacheKey("git://local/mychart", "HEAD")
	require.NoError(t, cache.Set(cacheKey, clonedChartPath))

	cachedPath, found := cache.Get(cacheKey)
	require.True(t, found)
	require.NotEmpty(t, cachedPath)

	// 5. Render the chart from the cached directory.
	manifest := renderChartLocal(t, cachedPath)
	assert.Contains(t, manifest, "kind: ConfigMap")
	assert.Contains(t, manifest, "integration-test-config")

	// 6. Verify that a second GetOrFetch does not call the fetch function.
	fetchCalled := 0
	result, err := cache.GetOrFetch(cacheKey, func() (string, error) {
		fetchCalled++
		return "/should/not/be/called", nil
	})
	require.NoError(t, err)
	assert.Equal(t, 0, fetchCalled, "fetch must not be called on cache hit")
	assert.Equal(t, cachedPath, result)
}

// TestOCIChartFetch verifies that FetchChartOCI can pull a real chart from a
// public OCI registry, cache it on disk, and produce a directory that the Helm
// chart loader can successfully parse.
func TestOCIChartFetch(t *testing.T) {
	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)

	// Use the public ECR registry for the Karpenter Helm chart.
	// FetchChartOCI will prepend "oci://" automatically.
	chartVersion := "1.4.0"

	chartPath, err := FetchChartOCI(context.Background(), "public.ecr.aws/karpenter", "karpenter", chartVersion, credsProviderFunc(nil), cache)
	require.NoError(t, err, "FetchChartOCI must succeed for a public OCI chart")
	require.NotEmpty(t, chartPath, "returned chart path must not be empty")

	// The path must exist on disk.
	_, err = os.Stat(chartPath)
	require.NoError(t, err, "chart path must exist on disk")

	// The chart must be loadable by the Helm loader.
	ch, err := loader.Load(chartPath)
	require.NoError(t, err, "loader.Load must succeed on the fetched chart")
	require.NotNil(t, ch)
	assert.Equal(t, "karpenter", ch.Metadata.Name)
}

// TestHTTPSChartFetch verifies that FetchChartHTTPS can pull a real chart from
// a public HTTPS Helm repository (argoproj/argo-helm), cache it on disk, and
// produce a directory that the Helm chart loader can successfully parse.
func TestHTTPSChartFetch(t *testing.T) {
	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)

	chartVersion := "7.8.14"

	chartPath, err := FetchChartHTTPS(context.Background(), "https://argoproj.github.io/argo-helm", "argo-cd", chartVersion, credsProviderFunc(nil), cache)
	require.NoError(t, err, "FetchChartHTTPS must succeed for a public HTTPS chart")
	require.NotEmpty(t, chartPath, "returned chart path must not be empty")

	// The path must exist on disk.
	_, err = os.Stat(chartPath)
	require.NoError(t, err, "chart path must exist on disk")

	// The chart must be loadable by the Helm loader.
	ch, err := loader.Load(chartPath)
	require.NoError(t, err, "loader.Load must succeed on the fetched chart")
	require.NotNil(t, ch)
	assert.Equal(t, "argo-cd", ch.Metadata.Name)
}

// TestRenderChart_MissingChartYAML verifies that loading a chart directory
// without a Chart.yaml file returns an error from the loader.
func TestRenderChart_MissingChartYAML(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	chartPath := filepath.Join(wd, "testdata", "nochart")

	_, err = loader.Load(chartPath)
	assert.Error(t, err, "loading a chart directory without Chart.yaml must return an error")
}

// TestRenderChart_MalformedChartYAML verifies that loading a chart with
// malformed Chart.yaml content returns an error.
func TestRenderChart_MalformedChartYAML(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	chartPath := filepath.Join(wd, "testdata", "badchart")

	_, err = loadAndValidateChart(chartPath)
	assert.Error(t, err, "loading a chart with malformed Chart.yaml must return an error")
}

// TestRenderChart_TemplateFail verifies that a chart with valid Chart.yaml but
// a template that fails at render time produces an error during the install
// dry-run step.
func TestRenderChart_TemplateFail(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)

	chartPath := filepath.Join(wd, "testdata", "badtemplate")

	// The chart should load successfully since Chart.yaml is valid.
	ch, err := loadAndValidateChart(chartPath)
	require.NoError(t, err, "chart with valid Chart.yaml should load without error")
	require.NotNil(t, ch)

	// Set up the Helm install client for client-only dry-run rendering.
	settings := cli.New()
	settings.SetNamespace("default")

	actionConfig := new(action.Configuration)
	err = actionConfig.Init(settings.RESTClientGetter(), "default", "memory", func(format string, v ...interface{}) {})
	require.NoError(t, err)

	kubeVersion, err := chartutil.ParseKubeVersion("v1.30.0")
	require.NoError(t, err)

	installClient := action.NewInstall(actionConfig)
	installClient.DryRunOption = "true"
	installClient.ReleaseName = "integration-test"
	installClient.Namespace = "default"
	installClient.ClientOnly = true
	installClient.DryRun = true
	installClient.Replace = true
	installClient.KubeVersion = kubeVersion
	installClient.DisableHooks = true
	installClient.IncludeCRDs = true

	// Render with empty values — the template uses `required` so this must fail.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = installClient.RunWithContext(ctx, ch, map[string]any{})
	assert.Error(t, err, "rendering a chart with a failing template must return an error")
}
