package directoryworker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	sigYAML "sigs.k8s.io/yaml"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

// directoryStreamSubjects lists every subject the DirectoryWorker publishes to
// or consumes so the test stream covers them all.
var directoryStreamSubjects = []string{
	subjects.GitDirectoryFetched,
	subjects.DirectoryManifestRendered,
	subjects.DirectoryManifestRenderFailed,
}

const directoryTestStream = "directoryworker-test"

// newTestDirectoryWorker creates a DirectoryWorker wired to a real embedded NATS server.
func newTestDirectoryWorker(t *testing.T) (*DirectoryWorker, *internalnats.Bus, *internalnats.Store) {
	t.Helper()
	bus, store, _ := testutil.StartNATS(t)

	ctx := context.Background()
	err := bus.EnsureStream(ctx, directoryTestStream, directoryStreamSubjects)
	require.NoError(t, err, "failed to create NATS test stream")

	log := testutil.NoopLogger()
	w := New(log, bus, store)
	return w, bus, store
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — renders directory YAML files, stores, publishes
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_RendersAndStores(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	configMap := `apiVersion: v1
kind: ConfigMap
metadata:
  name: test-config
  namespace: default
data:
  key: value
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "configmap.yaml"), []byte(configMap), 0o644))

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-deploy
  namespace: default
spec:
  replicas: 1
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "42"
		sha     = "abc123"
		origin  = "apps/myapp.yaml"
		appName = "myapp"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Directory: models.DirectorySpec{
			Recurse: false,
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful render")
	assert.False(t, nakCalled, "nak should not be called on successful render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")
	assert.Contains(t, stored, "ConfigMap", "stored manifest should contain the ConfigMap resource")
	assert.Contains(t, stored, "Deployment", "stored manifest should contain the Deployment resource")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — recurse includes subdirectory files
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Recurse_IncludesSubdirFiles(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	rootYAML := `apiVersion: v1
kind: ConfigMap
metadata:
  name: root-config
  namespace: default
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "root.yaml"), []byte(rootYAML), 0o644))

	subdir := filepath.Join(dir, "subdir")
	require.NoError(t, os.MkdirAll(subdir, 0o755))

	subdirYAML := `apiVersion: v1
kind: ConfigMap
metadata:
  name: subdir-config
  namespace: default
`
	require.NoError(t, os.WriteFile(filepath.Join(subdir, "subconfig.yaml"), []byte(subdirYAML), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "7"
		sha     = "def456"
		origin  = "apps/recurse.yaml"
		appName = "recurse-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Directory: models.DirectorySpec{
			Recurse: true,
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ack := func() error { return nil }
	nak := func() error { return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.Contains(t, stored, "root-config", "stored manifest should contain the root-level resource")
	assert.Contains(t, stored, "subdir-config", "stored manifest should contain the subdirectory resource")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomization.yaml triggers krusty render
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_KustomizationYaml_UsesKrusty(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: kustomize-deploy
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kustomize-deploy
  template:
    metadata:
      labels:
        app: kustomize-deploy
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "55"
		sha     = "kustomize123"
		origin  = "apps/kustomize-app.yaml"
		appName = "kustomize-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")
	assert.Contains(t, stored, "Deployment", "stored manifest should contain the Deployment resource")
	assert.Contains(t, stored, "kustomize-deploy", "stored manifest should contain the deployment name")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies namePrefix
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_KustomizationYaml_WithOverlay(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "77"
		sha     = "overlay123"
		origin  = "apps/overlay-app.yaml"
		appName = "overlay-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			NamePrefix: "prod-",
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")
	assert.Contains(t, stored, "prod-my-app", "stored manifest should contain the prefixed deployment name")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — empty directory publishes DirectoryManifestRenderFailed
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_EmptyDirectory_PublishesFailure(t *testing.T) {
	w, bus, _ := newTestDirectoryWorker(t)
	ctx := context.Background()

	emptyDir := t.TempDir()

	const appName = "empty-app"
	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Directory: models.DirectorySpec{
			Recurse: false,
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":       "org",
		"pr.repo":        "repo",
		"pr.number":      "99",
		"sha.active":     "sha-empty",
		"app.origin":     "apps/empty.yaml",
		"chart.location": emptyDir,
	}

	renderFailedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRenderFailed)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called even when render fails (error reported via event)")
	assert.False(t, nakCalled, "nak should not be called on render error")

	select {
	case hdrs := <-renderFailedCh:
		assert.NotEmpty(t, hdrs["error.msg"], "error.msg header should be set on render failure")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRenderFailed message")
	}
}

// mustParseDeployment splits a multi-document YAML string on "---", finds the
// document with kind: Deployment, and unmarshals it into an appsv1.Deployment.
// It fatally fails the test if no Deployment document is found or unmarshalling fails.
func mustParseDeployment(t *testing.T, yamlStr string) appsv1.Deployment {
	t.Helper()
	docs := strings.Split(yamlStr, "---")
	for _, doc := range docs {
		trimmed := strings.TrimSpace(doc)
		if trimmed == "" {
			continue
		}
		if !strings.Contains(trimmed, "kind: Deployment") {
			continue
		}
		var deploy appsv1.Deployment
		if err := sigYAML.Unmarshal([]byte(trimmed), &deploy); err != nil {
			t.Fatalf("failed to unmarshal Deployment YAML: %v", err)
		}
		return deploy
	}
	t.Fatal("no Deployment document found in YAML output")
	return appsv1.Deployment{} // unreachable
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies namespace
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Overlay_Namespace(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "100"
		sha     = "ns-test-123"
		origin  = "apps/ns-app.yaml"
		appName = "ns-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			Namespace: "production",
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "production", deploy.Namespace, "deployment namespace should be set to 'production'")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies nameSuffix
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Overlay_NameSuffix(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "101"
		sha     = "suffix-test-456"
		origin  = "apps/suffix-app.yaml"
		appName = "suffix-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			NameSuffix: "-v2",
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "my-app-v2", deploy.Name, "deployment name should have suffix '-v2'")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies commonLabels
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Overlay_CommonLabels(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "102"
		sha     = "labels-test-789"
		origin  = "apps/labels-app.yaml"
		appName = "labels-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			CommonLabels: map[string]string{"team": "platform"},
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "platform", deploy.Labels["team"], "deployment metadata labels should contain team=platform")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies commonAnnotations
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Overlay_CommonAnnotations(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "103"
		sha     = "annotations-test-012"
		origin  = "apps/annotations-app.yaml"
		appName = "annotations-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			CommonAnnotations: map[string]string{"owner": "sre"},
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "sre", deploy.Annotations["owner"], "deployment metadata annotations should contain owner=sre")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies forceCommonLabels
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Overlay_ForceCommonLabels(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "104"
		sha     = "force-labels-test-345"
		origin  = "apps/force-labels-app.yaml"
		appName = "force-labels-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			ForceCommonLabels: true,
			CommonLabels:      map[string]string{"env": "prod"},
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "prod", deploy.Labels["env"], "deployment metadata labels should contain env=prod")
	assert.Equal(t, "prod", deploy.Spec.Selector.MatchLabels["env"], "selector matchLabels should contain env=prod")
	assert.Equal(t, "prod", deploy.Spec.Template.Labels["env"], "pod template labels should contain env=prod")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies images
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Overlay_Images(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "105"
		sha     = "images-test-567"
		origin  = "apps/images-app.yaml"
		appName = "images-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			Images: []string{"nginx=nginx:1.25"},
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "nginx:1.25", deploy.Spec.Template.Spec.Containers[0].Image, "container image should be overridden to nginx:1.25")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies replicas
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Overlay_Replicas(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "106"
		sha     = "replicas-test-678"
		origin  = "apps/replicas-app.yaml"
		appName = "replicas-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			Replicas: []models.KustomizeReplica{
				{Name: "my-app", Count: 5},
			},
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	require.NotNil(t, deploy.Spec.Replicas, "deployment spec.replicas should not be nil")
	assert.Equal(t, int32(5), *deploy.Spec.Replicas, "deployment spec.replicas should be 5")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies patches
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Overlay_Patches(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "107"
		sha     = "patches-test-890"
		origin  = "apps/patches-app.yaml"
		appName = "patches-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			Patches: []models.KustomizePatch{
				{
					Patch: `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  template:
    spec:
      containers:
      - name: app
        resources:
          limits:
            memory: "512Mi"`,
					Target: &models.KustomizePatchTarget{
						Kind: "Deployment",
						Name: "my-app",
					},
				},
			},
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	require.Len(t, deploy.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "512Mi", deploy.Spec.Template.Spec.Containers[0].Resources.Limits.Memory().String(),
		"container should have memory limit of 512Mi")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomize overlay applies namespace, commonLabels,
// images, and replicas simultaneously
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_Overlay_Combined(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "108"
		sha     = "combined-test-999"
		origin  = "apps/combined-app.yaml"
		appName = "combined-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Kustomize: models.KustomizeSpec{
			Namespace:    "staging",
			CommonLabels: map[string]string{"env": "staging"},
			Images:       []string{"nginx=nginx:1.25"},
			Replicas: []models.KustomizeReplica{
				{Name: "my-app", Count: 3},
			},
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty overlay render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty overlay render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "staging", deploy.Namespace, "deployment namespace should be set to 'staging'")
	assert.Equal(t, "staging", deploy.Labels["env"], "deployment metadata labels should contain env=staging")
	assert.Equal(t, "nginx:1.25", deploy.Spec.Template.Spec.Containers[0].Image, "container image should be overridden to nginx:1.25")
	require.NotNil(t, deploy.Spec.Replicas, "deployment spec.replicas should not be nil")
	assert.Equal(t, int32(3), *deploy.Spec.Replicas, "deployment spec.replicas should be 3")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomization.yml auto-detection
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_KustomizationYml_UsesKrusty(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: yml-detect-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: yml-detect-app
  template:
    metadata:
      labels:
        app: yml-detect-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "200"
		sha     = "yml-detect-123"
		origin  = "apps/yml-detect.yaml"
		appName = "yml-detect-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "yml-detect-app", deploy.Name, "deployment name should be yml-detect-app")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — Kustomization (capitalized, no extension) auto-detection
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_KustomizationCapitalized_UsesKrusty(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: cap-detect-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cap-detect-app
  template:
    metadata:
      labels:
        app: cap-detect-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yaml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yaml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "Kustomization"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "201"
		sha     = "cap-detect-456"
		origin  = "apps/cap-detect.yaml"
		appName = "cap-detect-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "cap-detect-app", deploy.Name, "deployment name should be cap-detect-app")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — mixed .yaml and .yml extensions in plain directory
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_MixedExtensions_IncludesBoth(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	configMap := `apiVersion: v1
kind: ConfigMap
metadata:
  name: test-config
  namespace: default
data:
  key: value
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "configmap.yaml"), []byte(configMap), 0o644))

	service := `apiVersion: v1
kind: Service
metadata:
  name: test-service
  namespace: default
spec:
  selector:
    app: test
  ports:
  - port: 80
    targetPort: 8080
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "service.yml"), []byte(service), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "202"
		sha     = "mixed-ext-789"
		origin  = "apps/mixed-ext.yaml"
		appName = "mixed-ext-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
		},
		Directory: models.DirectorySpec{
			Recurse: false,
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful render")
	assert.False(t, nakCalled, "nak should not be called on successful render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.Contains(t, stored, "ConfigMap")
	assert.Contains(t, stored, "test-config")
	assert.Contains(t, stored, "Service")
	assert.Contains(t, stored, "test-service")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}

// ---------------------------------------------------------------------------
// handleDirectoryRender — kustomization.yml with .yml resources
// ---------------------------------------------------------------------------

func TestHandleDirectoryRender_KustomizationYml_YmlResources(t *testing.T) {
	w, bus, store := newTestDirectoryWorker(t)
	ctx := context.Background()

	dir := t.TempDir()

	deployment := `apiVersion: apps/v1
kind: Deployment
metadata:
  name: yml-resource-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: yml-resource-app
  template:
    metadata:
      labels:
        app: yml-resource-app
    spec:
      containers:
      - name: app
        image: nginx:latest
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "deployment.yml"), []byte(deployment), 0o644))

	kustomization := `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
- deployment.yml
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kustomization.yml"), []byte(kustomization), 0o644))

	const (
		owner   = "myorg"
		repo    = "myrepo"
		number  = "203"
		sha     = "yml-res-012"
		origin  = "apps/yml-res.yaml"
		appName = "yml-resource-app"
	)

	spec := models.AppSpec{
		AppName: appName,
		Source: models.AppSource{
			Path:     ".",
			Revision: "main",
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
		"chart.location": dir,
	}

	manifestRenderedCh := testutil.SubscribeOnce(t, bus, subjects.DirectoryManifestRendered)

	ackCalled := false
	nakCalled := false
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	w.handleDirectoryRender(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on successful krusty render")
	assert.False(t, nakCalled, "nak should not be called on successful krusty render")

	expectedKey := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, appName)
	stored, err := internalnats.GetObject[string](ctx, store, expectedKey)
	require.NoError(t, err, "manifest should be stored at key %q", expectedKey)
	assert.NotEmpty(t, stored, "stored manifest should not be empty")

	deploy := mustParseDeployment(t, stored)
	assert.Equal(t, "yml-resource-app", deploy.Name, "deployment name should be yml-resource-app")

	select {
	case hdrs := <-manifestRenderedCh:
		assert.Equal(t, expectedKey, hdrs["manifest.location"], "manifest.location header should carry the store key")
		assert.Equal(t, appName, hdrs["app.name"], "app.name header should be set")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DirectoryManifestRendered message")
	}
}
