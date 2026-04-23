package helm

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
)

// ---- helpers ----------------------------------------------------------------

// buildTGZ creates a minimal mychart-0.1.0.tgz in a temp file and returns its path.
func buildTGZ(t *testing.T) string {
	t.Helper()

	chartYAML := `apiVersion: v2
name: mychart
version: 0.1.0
description: A test chart
`
	configMapYAML := `apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}-config
data:
  key: value
`

	tmpFile, err := os.CreateTemp(t.TempDir(), "mychart-*.tgz")
	require.NoError(t, err)
	defer tmpFile.Close()

	gzw := gzip.NewWriter(tmpFile)
	tw := tar.NewWriter(gzw)

	files := []struct {
		name    string
		content string
	}{
		{"mychart/Chart.yaml", chartYAML},
		{"mychart/templates/configmap.yaml", configMapYAML},
	}

	for _, f := range files {
		hdr := &tar.Header{
			Name:     f.name,
			Typeflag: tar.TypeReg,
			Size:     int64(len(f.content)),
			Mode:     0644,
		}
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write([]byte(f.content))
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())
	require.NoError(t, gzw.Close())

	return tmpFile.Name()
}

// testdataChart returns the absolute path to testdata/mychart.
func testdataChart(t *testing.T) string {
	t.Helper()
	// __file__ equivalent: use the test source directory via os.Getwd()
	// go test sets cwd to the package directory, so testdata/ is reachable.
	wd, err := os.Getwd()
	require.NoError(t, err)
	return filepath.Join(wd, "testdata", "mychart")
}

// ---- GenerateCacheKey -------------------------------------------------------

func TestGenerateCacheKey_DifferentInputs(t *testing.T) {
	ref := "oci://registry.example.com/charts/mychart"
	key1 := GenerateCacheKey(ref, "1.0.0")
	key2 := GenerateCacheKey(ref, "2.0.0")
	assert.NotEqual(t, key1, key2, "different versions should produce different cache keys")

	key3 := GenerateCacheKey("oci://other.registry.io/charts/mychart", "1.0.0")
	assert.NotEqual(t, key1, key3, "different refs should produce different cache keys")
}

func TestGenerateCacheKey_IsHexString(t *testing.T) {
	key := GenerateCacheKey("some/chart", "0.1.0")
	assert.NotEmpty(t, key)
	for _, c := range key {
		assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
			"expected hex character, got %q", c)
	}
}

// ---- extractChartArchive ----------------------------------------------------

func TestExtractChartArchive_Valid(t *testing.T) {
	archivePath := buildTGZ(t)
	destDir := t.TempDir()

	err := extractChartArchive(archivePath, destDir)
	require.NoError(t, err)

	chartYAML := filepath.Join(destDir, "mychart", "Chart.yaml")
	configMap := filepath.Join(destDir, "mychart", "templates", "configmap.yaml")

	assert.FileExists(t, chartYAML)
	assert.FileExists(t, configMap)

	chartContent, err := os.ReadFile(chartYAML)
	require.NoError(t, err)
	assert.Contains(t, string(chartContent), "name: mychart")

	cmContent, err := os.ReadFile(configMap)
	require.NoError(t, err)
	assert.Contains(t, string(cmContent), "kind: ConfigMap")
}

func TestExtractChartArchive_InvalidPath(t *testing.T) {
	err := extractChartArchive("/nonexistent/path/chart.tgz", t.TempDir())
	require.Error(t, err)
}

func TestExtractChartArchive_NonGzip(t *testing.T) {
	// Write a plain text file with a .tgz extension — not valid gzip.
	notGzip, err := os.CreateTemp(t.TempDir(), "bad-*.tgz")
	require.NoError(t, err)
	_, err = notGzip.WriteString("this is not gzip content at all\n")
	require.NoError(t, err)
	notGzip.Close()

	err = extractChartArchive(notGzip.Name(), t.TempDir())
	require.Error(t, err)
}

// ---- loadAndValidateChart ---------------------------------------------------

func TestLoadAndValidateChart_Valid(t *testing.T) {
	chartPath := testdataChart(t)
	ch, err := loadAndValidateChart(chartPath)
	require.NoError(t, err)
	require.NotNil(t, ch)
	require.NotNil(t, ch.Metadata)
	assert.Equal(t, "mychart", ch.Metadata.Name)
}

func TestLoadAndValidateChart_NonExistentPath(t *testing.T) {
	_, err := loadAndValidateChart("/nonexistent/chart/path")
	require.Error(t, err)
}

func TestLoadAndValidateChart_MissingChartYAML(t *testing.T) {
	// A directory that exists but has no Chart.yaml
	emptyDir := t.TempDir()
	_, err := loadAndValidateChart(emptyDir)
	require.Error(t, err)
}

// ---- NewChartDiskCache + Get / Set / GetOrFetch ----------------------------

func TestNewChartDiskCache_CreatesSnapshotsDir(t *testing.T) {
	base := t.TempDir()
	cache, err := NewChartDiskCache(base)
	require.NoError(t, err)
	require.NotNil(t, cache)

	// The cache dir must be base/snapshots
	snapshotsDir := filepath.Join(base, "snapshots")
	info, err := os.Stat(snapshotsDir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestHelmChartCache_GetMissing(t *testing.T) {
	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)

	path, found := cache.Get("nonexistent-key")
	assert.False(t, found)
	assert.Empty(t, path)
}

func TestHelmChartCache_SetAndGet(t *testing.T) {
	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)

	chartPath := testdataChart(t)
	key := GenerateCacheKey(chartPath, "0.1.0")

	require.NoError(t, cache.Set(key, chartPath))

	cachedPath, found := cache.Get(key)
	assert.True(t, found)
	assert.NotEmpty(t, cachedPath)

	// Chart.yaml must be present inside the cached directory
	assert.FileExists(t, filepath.Join(cachedPath, "Chart.yaml"))
}

func TestHelmChartCache_GetOrFetch_CallsFetchOnMiss(t *testing.T) {
	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)

	fetchCalled := 0
	chartPath := testdataChart(t)
	key := "test-miss-key"

	result, err := cache.GetOrFetch(key, func() (string, error) {
		fetchCalled++
		return chartPath, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 1, fetchCalled, "fetch should be called exactly once on cache miss")
	assert.Equal(t, chartPath, result)
}

func TestHelmChartCache_GetOrFetch_SkipsFetchOnHit(t *testing.T) {
	cache, err := NewChartDiskCache(t.TempDir())
	require.NoError(t, err)

	chartPath := testdataChart(t)
	key := GenerateCacheKey(chartPath, "0.1.0")

	// Pre-populate the cache.
	require.NoError(t, cache.Set(key, chartPath))

	fetchCalled := 0
	result, err := cache.GetOrFetch(key, func() (string, error) {
		fetchCalled++
		return "/should/not/be/called", nil
	})

	require.NoError(t, err)
	assert.Equal(t, 0, fetchCalled, "fetch should NOT be called on cache hit")
	assert.NotEmpty(t, result)
}

func TestDetectKubernetesVersion_FallsBackWhenClusterUnreachable(t *testing.T) {
	// Force a completely bogus KUBECONFIG so ToRESTConfig / the discovery
	// client cannot reach a real cluster. t.Setenv is test-scoped, so no
	// cleanup is required.
	t.Setenv("KUBECONFIG", "/definitely/not/a/real/kubeconfig")
	t.Setenv("HOME", t.TempDir()) // prevent ~/.kube/config fallback

	settings := cli.New()

	got, err := detectKubernetesVersion(settings)
	require.NoError(t, err, "detectKubernetesVersion must not surface errors on unreachable cluster")
	require.NotNil(t, got, "fallback version must be non-nil")

	want, err := chartutil.ParseKubeVersion(fallbackKubeVersion)
	require.NoError(t, err)

	assert.Equal(t, want.Version, got.Version, "expected the pinned fallback version")
}
