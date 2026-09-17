package argoworker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	appv1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/trolleksii/argocd-diff-reporter/internal/argo"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

func TestParseFileResources_SingleApplication(t *testing.T) {
	appSets, apps, err := parseFileResources(filepath.Join("testdata", "application.yaml"))
	require.NoError(t, err)
	assert.Len(t, appSets, 0)
	require.Len(t, apps, 1)
	assert.Equal(t, "my-app", apps[0].Name)
}

func TestParseFileResources_SingleApplicationSet(t *testing.T) {
	appSets, apps, err := parseFileResources(filepath.Join("testdata", "applicationset.yaml"))
	require.NoError(t, err)
	require.Len(t, appSets, 1)
	assert.Len(t, apps, 0)
	assert.Equal(t, "my-appset", appSets[0].Name)
}

func TestParseFileResources_MultiDocument(t *testing.T) {
	appSets, apps, err := parseFileResources(filepath.Join("testdata", "multi.yaml"))
	require.NoError(t, err)
	assert.Len(t, appSets, 1)
	assert.Len(t, apps, 1)
}

func TestParseFileResources_UnknownKind(t *testing.T) {
	appSets, apps, err := parseFileResources(filepath.Join("testdata", "unknown_kind.yaml"))
	require.NoError(t, err)
	assert.Len(t, appSets, 0)
	assert.Len(t, apps, 0)
}

func TestParseFileResources_Mixed(t *testing.T) {
	appSets, apps, err := parseFileResources(filepath.Join("testdata", "mixed.yaml"))
	require.NoError(t, err)
	assert.Len(t, appSets, 1)
	assert.Len(t, apps, 1)
}

func TestParseFileResources_InvalidYAML(t *testing.T) {
	_, _, err := parseFileResources(filepath.Join("testdata", "invalid.yaml"))
	require.Error(t, err)
}

func TestParseFileResources_NonExistentFile(t *testing.T) {
	_, _, err := parseFileResources(filepath.Join("testdata", "does_not_exist.yaml"))
	require.Error(t, err)
}

func TestParseFileResources_EmptyFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "empty-*.yaml")
	require.NoError(t, err)
	f.Close()

	appSets, apps, err := parseFileResources(f.Name())
	require.NoError(t, err)
	assert.Len(t, appSets, 0)
	assert.Len(t, apps, 0)
}

// ---------------------------------------------------------------------------
// parseForArgoResources integration tests
// ---------------------------------------------------------------------------

// argoStreamSubjects lists all subjects the ArgoWorker publishes so the test
// stream covers them all.
var argoStreamSubjects = []string{
	subjects.GitFilesSnapshotted,
	subjects.ArgoHelmOCIParsed,
	subjects.ArgoHelmHTTPParsed,
	subjects.ArgoHelmGitParsed,
	subjects.ArgoSideParsed,
	subjects.ArgoDirectoryGitParsed,
}

const argoTestStream = "argo-worker-test"

// newTestArgoWorker creates an ArgoWorker wired to a real embedded NATS server.
// customRendererFunc is injected so no live K8s connection is needed.
func newTestArgoWorker(t *testing.T, fn argo.AppSetRenderer) (*ArgoWorker, *internalnats.Bus) {
	t.Helper()
	bus, _, _ := testutil.StartNATS(t)

	ctx := context.Background()
	err := bus.EnsureStream(ctx, argoTestStream, argoStreamSubjects)
	require.NoError(t, err, "failed to create NATS test stream")

	w := New(testutil.NoopLogger(), bus, fn)
	return w, bus
}

// testHeaders builds the headers gitworker sets on GitFilesSnapshotted. Each
// test passes its own runId so JetStream Msg-Id dedup never collides.
func testHeaders(runId, snapshotDir string) internalnats.Headers {
	return internalnats.Headers{
		"pr.number":         "1",
		"pr.owner":          "org",
		"pr.repo":           "repo",
		"sha.active":        "headsha",
		"RunId":             runId,
		"pr.files.snapshot": snapshotDir,
	}
}

// helmApp builds a minimal appv1alpha1.Application with a Helm source.
// repoURL and path control the subject-routing logic in buildAppSpec.
func helmApp(name, repoURL, path, chart string) appv1alpha1.Application {
	valuesJSON, _ := json.Marshal(map[string]any{})
	return appv1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appv1alpha1.ApplicationSpec{
			Source: &appv1alpha1.ApplicationSource{
				RepoURL:        repoURL,
				TargetRevision: "1.0.0",
				Path:           path,
				Chart:          chart,
				Helm: &appv1alpha1.ApplicationSourceHelm{
					ReleaseName:  name + "-release",
					ValuesObject: &runtime.RawExtension{Raw: valuesJSON},
				},
			},
			Destination: appv1alpha1.ApplicationDestination{
				Namespace: "default",
			},
		},
	}
}

// helmAppWithParams builds a Helm Application with ValueFiles, Parameters, and Values.
func helmAppWithParams(name, repoURL, chart string) appv1alpha1.Application {
	valuesJSON, _ := json.Marshal(map[string]any{"image": map[string]any{"tag": "v1.0.0"}})
	return appv1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appv1alpha1.ApplicationSpec{
			Source: &appv1alpha1.ApplicationSource{
				RepoURL:        repoURL,
				TargetRevision: "1.0.0",
				Chart:          chart,
				Helm: &appv1alpha1.ApplicationSourceHelm{
					ReleaseName:  name + "-release",
					ValuesObject: &runtime.RawExtension{Raw: valuesJSON},
					ValueFiles:   []string{"values-prod.yaml", "values-secrets.yaml"},
					Parameters: []appv1alpha1.HelmParameter{
						{Name: "replicas", Value: "3"},
						{Name: "debug", Value: "true", ForceString: true},
					},
				},
			},
			Destination: appv1alpha1.ApplicationDestination{
				Namespace: "production",
			},
		},
	}
}

// helmAppNoValues builds a Helm Application with no values, valueFiles, or parameters.
func helmAppNoValues(name, repoURL, chart string) appv1alpha1.Application {
	return appv1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appv1alpha1.ApplicationSpec{
			Source: &appv1alpha1.ApplicationSource{
				RepoURL:        repoURL,
				TargetRevision: "1.0.0",
				Chart:          chart,
				Helm: &appv1alpha1.ApplicationSourceHelm{
					ReleaseName: name + "-release",
				},
			},
			Destination: appv1alpha1.ApplicationDestination{
				Namespace: "default",
			},
		},
	}
}

// directoryApp builds a minimal appv1alpha1.Application with a plain directory source.
// When recurse is true, Directory.Recurse is set on the source.
func directoryApp(name, repoURL, path string, recurse bool) appv1alpha1.Application {
	src := &appv1alpha1.ApplicationSource{
		RepoURL:        repoURL,
		TargetRevision: "HEAD",
		Path:           path,
	}
	if recurse {
		src.Directory = &appv1alpha1.ApplicationSourceDirectory{
			Recurse: true,
		}
	}
	return appv1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appv1alpha1.ApplicationSpec{
			Source: src,
			Destination: appv1alpha1.ApplicationDestination{
				Namespace: "default",
			},
		},
	}
}

// kustomizeApp builds a minimal appv1alpha1.Application with an explicit
// Kustomize source type. The provided ApplicationSourceKustomize is set on
// Source.Kustomize so that ExplicitType() returns ApplicationSourceTypeKustomize.
func kustomizeApp(name, repoURL, path string, k *appv1alpha1.ApplicationSourceKustomize) appv1alpha1.Application {
	return appv1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appv1alpha1.ApplicationSpec{
			Source: &appv1alpha1.ApplicationSource{
				RepoURL:        repoURL,
				TargetRevision: "HEAD",
				Path:           path,
				Kustomize:      k,
			},
			Destination: appv1alpha1.ApplicationDestination{
				Namespace: "default",
			},
		},
	}
}

// autoDetectApp builds a minimal appv1alpha1.Application with NO explicit source
// type fields (no Helm, Kustomize, Directory, or Plugin). This means
// ExplicitType() returns nil and the app is routed to ArgoDirectoryGitParsed
// for deferred auto-detection.
func autoDetectApp(name, repoURL, path string) appv1alpha1.Application {
	return appv1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appv1alpha1.ApplicationSpec{
			Source: &appv1alpha1.ApplicationSource{
				RepoURL:        repoURL,
				TargetRevision: "HEAD",
				Path:           path,
			},
			Destination: appv1alpha1.ApplicationDestination{
				Namespace: "default",
			},
		},
	}
}

// pluginApp builds a minimal appv1alpha1.Application with a Plugin source
// type. ExplicitType() returns ApplicationSourceTypePlugin, which is not
// routed by the ArgoWorker and should trigger the unsupported-source-type
// error path in buildAppSpec.
func pluginApp(name string) appv1alpha1.Application {
	return appv1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appv1alpha1.ApplicationSpec{
			Source: &appv1alpha1.ApplicationSource{
				RepoURL: "https://example.com/repo.git",
				Plugin:  &appv1alpha1.ApplicationSourcePlugin{Name: "my-plugin"},
			},
		},
	}
}

// multiSourceTypeApp builds a minimal appv1alpha1.Application with both Helm
// and Kustomize source types set, which causes ExplicitType() to return an error.
func multiSourceTypeApp(name string) appv1alpha1.Application {
	valuesJSON, _ := json.Marshal(map[string]any{})
	return appv1alpha1.Application{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appv1alpha1.ApplicationSpec{
			Source: &appv1alpha1.ApplicationSource{
				RepoURL:        "https://github.com/example/repo",
				TargetRevision: "HEAD",
				Path:           "manifests/app",
				Helm: &appv1alpha1.ApplicationSourceHelm{
					ReleaseName:  name + "-release",
					ValuesObject: &runtime.RawExtension{Raw: valuesJSON},
				},
				Kustomize: &appv1alpha1.ApplicationSourceKustomize{},
			},
			Destination: appv1alpha1.ApplicationDestination{
				Namespace: "default",
			},
		},
	}
}

// makeSnapshotDir creates a temp snapshot directory and copies the given
// testdata files into it by name. Returns the snapshot dir path.
func makeSnapshotDir(t *testing.T, fileNames ...string) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range fileNames {
		src, err := os.ReadFile(filepath.Join("testdata", name))
		require.NoError(t, err, "read testdata file %s", name)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), src, 0o644))
	}
	return dir
}

// filesPayload serialises the []string file list parseForArgoResources expects.
func filesPayload(t *testing.T, files ...string) []byte {
	t.Helper()
	data, err := internalnats.Marshal(files)
	require.NoError(t, err)
	return data
}

// awaitSide waits for the ArgoSideParsed payload and decodes it.
func awaitSide(t *testing.T, ch <-chan []byte) []models.FileParsingResult {
	t.Helper()
	select {
	case body := <-ch:
		side, err := internalnats.Unmarshal[[]models.FileParsingResult](body)
		require.NoError(t, err, "failed to unmarshal ArgoSideParsed body")
		return side
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoSideParsed message")
		return nil
	}
}

// ---------------------------------------------------------------------------
// OCI subject routing
// ---------------------------------------------------------------------------

func TestParseForArgoResources_OCIAppRoutedToOCISubject(t *testing.T) {
	// The Application YAML on disk has no Helm — use the rendererFunc path via
	// an ApplicationSet file so we fully exercise rendererFunc injection.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			helmApp("oci-app", "oci://registry.example.com/charts", "", "my-chart"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	ociCh := testutil.SubscribeOnce(t, bus, subjects.ArgoHelmOCIParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	headers := testHeaders("run-1", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-ociCh:
		assert.Equal(t, "applicationset.yaml", hdrs["app.origin"])
		assert.Equal(t, "oci-app", hdrs["app.name"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmOCIParsed message")
	}
}

// ---------------------------------------------------------------------------
// Git subject routing
// ---------------------------------------------------------------------------

func TestParseForArgoResources_GitAppRoutedToGitSubject(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			helmApp("git-app", "git@github.com:example/charts.git", "charts/my-app", ""),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	gitCh := testutil.SubscribeOnce(t, bus, subjects.ArgoHelmGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	headers := testHeaders("run-2", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-gitCh:
		assert.Equal(t, "applicationset.yaml", hdrs["app.origin"])
		assert.Equal(t, "git-app", hdrs["app.name"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmGitParsed message")
	}
}

// ---------------------------------------------------------------------------
// HTTP subject routing
// ---------------------------------------------------------------------------

func TestParseForArgoResources_HTTPAppRoutedToHTTPSubject(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			helmApp("http-app", "https://charts.example.com", "", "my-chart"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	httpCh := testutil.SubscribeOnce(t, bus, subjects.ArgoHelmHTTPParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	headers := testHeaders("run-3", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-httpCh:
		assert.Equal(t, "applicationset.yaml", hdrs["app.origin"])
		assert.Equal(t, "http-app", hdrs["app.name"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmHTTPParsed message")
	}
}

// ---------------------------------------------------------------------------
// ArgoSideParsed lists every app rendered from a file
// ---------------------------------------------------------------------------

func TestParseForArgoResources_SideParsed_ListsAllApps(t *testing.T) {
	// Renderer returns two apps for one ApplicationSet.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			helmApp("app-one", "oci://registry.example.com/charts", "", "chart-one"),
			helmApp("app-two", "oci://registry.example.com/charts", "", "chart-two"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	sideCh := testutil.SubscribeOnceBody(t, bus, subjects.ArgoSideParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	headers := testHeaders("run-4", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), testutil.NoopAck, testutil.NoopNak)

	side := awaitSide(t, sideCh)
	require.Len(t, side, 1)
	assert.Equal(t, "applicationset.yaml", side[0].File)
	assert.Empty(t, side[0].Error)
	assert.Equal(t, []models.AppParsingResult{{Name: "app-one"}, {Name: "app-two"}}, side[0].Apps)
}

// ---------------------------------------------------------------------------
// Multi-ApplicationSet file — appsets render in parallel
// ---------------------------------------------------------------------------

func TestParseForArgoResources_MultiAppSetFile_RendersInParallel(t *testing.T) {
	// Each render blocks until all three ApplicationSets are in-flight at
	// once. A sequential implementation times out inside the renderer,
	// returns errors, and the side result below carries a file error.
	var entered sync.WaitGroup
	entered.Add(3)
	allIn := make(chan struct{})
	go func() {
		entered.Wait()
		close(allIn)
	}()
	rendererFunc := func(appSet appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		entered.Done()
		select {
		case <-allIn:
		case <-time.After(2 * time.Second):
			return nil, fmt.Errorf("render of %s never became concurrent", appSet.Name)
		}
		return []appv1alpha1.Application{
			helmApp(appSet.Name+"-app", "oci://registry.example.com/charts", "", "my-chart"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	sideCh := testutil.SubscribeOnceBody(t, bus, subjects.ArgoSideParsed)

	snapshotDir := makeSnapshotDir(t, "multi_appsets.yaml")
	headers := testHeaders("run-5", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "multi_appsets.yaml"), testutil.NoopAck, testutil.NoopNak)

	side := awaitSide(t, sideCh)
	require.Len(t, side, 1)
	assert.Empty(t, side[0].Error, "all three ApplicationSets should render successfully in parallel")
	assert.Len(t, side[0].Apps, 3)
}

// ---------------------------------------------------------------------------
// Error handling — invalid file lands in ArgoSideParsed, not a panic
// ---------------------------------------------------------------------------

func TestParseForArgoResources_InvalidFile_ReportsFileErrorNotPanic(t *testing.T) {
	// rendererFunc should never be called for unparseable files.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		t.Fatal("rendererFunc should not be called for invalid YAML files")
		return nil, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	sideCh := testutil.SubscribeOnceBody(t, bus, subjects.ArgoSideParsed)

	snapshotDir := makeSnapshotDir(t, "invalid.yaml")
	headers := testHeaders("run-6", snapshotDir)

	// Must not panic.
	require.NotPanics(t, func() {
		w.parseForArgoResources(context.Background(), headers, filesPayload(t, "invalid.yaml"), testutil.NoopAck, testutil.NoopNak)
	})

	side := awaitSide(t, sideCh)
	require.Len(t, side, 1)
	assert.Equal(t, "invalid.yaml", side[0].File)
	assert.NotEmpty(t, side[0].Error, "file-level Error should be set for invalid YAML")
	assert.Empty(t, side[0].Apps)
}

// ---------------------------------------------------------------------------
// Renderer function error lands in ArgoSideParsed as a file error
// ---------------------------------------------------------------------------

func TestParseForArgoResources_RendererError_ReportsFileError(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return nil, fmt.Errorf("renderer exploded")
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	sideCh := testutil.SubscribeOnceBody(t, bus, subjects.ArgoSideParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	headers := testHeaders("run-7", snapshotDir)

	require.NotPanics(t, func() {
		w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), testutil.NoopAck, testutil.NoopNak)
	})

	side := awaitSide(t, sideCh)
	require.Len(t, side, 1)
	assert.Equal(t, "applicationset.yaml", side[0].File)
	assert.Equal(t, "renderer exploded", side[0].Error)
}

// ---------------------------------------------------------------------------
// Non-existent file lands in ArgoSideParsed as a file error
// ---------------------------------------------------------------------------

func TestParseForArgoResources_NonExistentFile_ReportsFileError(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		t.Fatal("rendererFunc should not be called for missing files")
		return nil, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	sideCh := testutil.SubscribeOnceBody(t, bus, subjects.ArgoSideParsed)

	snapshotDir := t.TempDir() // empty — file won't exist
	headers := testHeaders("run-8", snapshotDir)

	require.NotPanics(t, func() {
		w.parseForArgoResources(context.Background(), headers, filesPayload(t, "does-not-exist.yaml"), testutil.NoopAck, testutil.NoopNak)
	})

	side := awaitSide(t, sideCh)
	require.Len(t, side, 1)
	assert.Equal(t, "does-not-exist.yaml", side[0].File)
	assert.NotEmpty(t, side[0].Error)
}

// ---------------------------------------------------------------------------
// Malformed payload naks and publishes nothing
// ---------------------------------------------------------------------------

func TestParseForArgoResources_MalformedPayload_NaksAndPublishesNothing(t *testing.T) {
	// rendererFunc must never be reached — unmarshal must fail first.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		panic("rendererFunc should not be called for malformed payload")
	}
	w, bus := newTestArgoWorker(t, rendererFunc)

	sideCh := testutil.SubscribeOnce(t, bus, subjects.ArgoSideParsed)

	var ackCount, nakCount int
	ack := func() error { ackCount++; return nil }
	nak := func() error { nakCount++; return nil }

	headers := testHeaders("run-9", t.TempDir())

	// Raw bytes that are guaranteed to fail msgpack decoding as []string.
	malformed := []byte{0xff, 0xff, 0xff, 0xff, 0xff}

	require.NotPanics(t, func() {
		w.parseForArgoResources(context.Background(), headers, malformed, ack, nak)
	})

	assert.Equal(t, 1, nakCount, "nak should be called exactly once for malformed payload")
	assert.Equal(t, 0, ackCount, "ack should not be called for malformed payload")

	// ArgoSideParsed must never be published on the malformed-payload path —
	// the handler must bail out before the publish/route loop runs.
	select {
	case hdrs := <-sideCh:
		t.Fatalf("unexpected ArgoSideParsed delivered for malformed payload: %+v", hdrs)
	case <-time.After(150 * time.Millisecond):
	}
}

// ---------------------------------------------------------------------------
// Plain Application file routes without invoking the renderer
// ---------------------------------------------------------------------------

func TestParseForArgoResources_PlainApplication_RoutedWithoutRenderer(t *testing.T) {
	// testdata/application.yaml holds a plain Application (not an
	// ApplicationSet), so parseFileResources puts it directly into the apps
	// slice and the rendererFunc is never invoked. Panic if it is.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		panic("rendererFunc should not be called when file contains a plain Application")
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirCh := testutil.SubscribeOnce(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "application.yaml")

	var ackCount, nakCount int
	ack := func() error { ackCount++; return nil }
	nak := func() error { nakCount++; return nil }

	headers := testHeaders("run-10", snapshotDir)

	require.NotPanics(t, func() {
		w.parseForArgoResources(context.Background(), headers, filesPayload(t, "application.yaml"), ack, nak)
	})

	select {
	case hdrs := <-dirCh:
		assert.Equal(t, "application.yaml", hdrs["app.origin"])
		assert.Equal(t, "my-app", hdrs["app.name"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message")
	}

	assert.Equal(t, 1, ackCount, "ack should be called exactly once for a successful route")
	assert.Equal(t, 0, nakCount, "nak should not be called on the happy path")
}

// ---------------------------------------------------------------------------
// Directory subject routing
// ---------------------------------------------------------------------------

func TestParseForArgoResources_DirectoryAppRoutedToDirectoryGitSubject(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			directoryApp("directory-app", "https://github.com/example/repo", "manifests/staging", false),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirCh := testutil.SubscribeOnce(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_directory.yaml")
	headers := testHeaders("run-30", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset_directory.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-dirCh:
		assert.Equal(t, "applicationset_directory.yaml", hdrs["app.origin"])
		assert.Equal(t, "directory-app", hdrs["app.name"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message")
	}
}

func TestParseForArgoResources_DirectoryApp_RecurseTrue_PropagatesInBody(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			directoryApp("directory-recurse-app", "https://github.com/example/repo", "manifests/staging", true),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirCh, bodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_directory.yaml")
	headers := testHeaders("run-31", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset_directory.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirCh:
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](<-bodyCh)
		require.NoError(t, err)
		assert.True(t, decoded.Directory.Recurse, "Directory.Recurse should be true when set on the application source")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message")
	}
}

func TestBuildAppSpec_ExplicitDirectory_SourceTypeAndRecurse(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			directoryApp("explicit-dir-app", "https://github.com/example/repo", "manifests/staging", true),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirHdrCh, dirBodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_directory.yaml")
	headers := testHeaders("run-35", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset_directory.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](body)
		require.NoError(t, err, "failed to unmarshal ArgoAppSpec from message body")
		assert.Equal(t, models.SourceType(models.SourceTypeDirectory), decoded.SourceType,
			"SourceType should be SourceTypeDirectory for explicit Directory source type")
		assert.True(t, decoded.Directory.Recurse,
			"Directory.Recurse should be true when set on the application source")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message for explicit directory app")
	}
}

// ---------------------------------------------------------------------------
// Kustomize app (no helm) routes to ArgoDirectoryGitParsed
// ---------------------------------------------------------------------------

// TestParseForArgoResources_KustomizeAppRoutedToDirectoryGitSubject verifies
// that an Application whose source has spec.source.kustomize != nil but no
// Helm spec is treated as a plain-directory app and published to
// ArgoDirectoryGitParsed (not skipped). This is the unified routing contract
// introduced when the Kustomize-specific pipeline was removed.
func TestParseForArgoResources_KustomizeAppRoutedToDirectoryGitSubject(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		// Build an application that has kustomize set but no helm.
		app := directoryApp("kustomize-app", "https://github.com/example/repo", "overlays/staging", false)
		app.Spec.Source.Kustomize = &appv1alpha1.ApplicationSourceKustomize{}
		return []appv1alpha1.Application{app}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirCh := testutil.SubscribeOnce(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_kustomize.yaml")
	headers := testHeaders("run-40", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset_kustomize.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-dirCh:
		assert.Equal(t, "applicationset_kustomize.yaml", hdrs["app.origin"],
			"kustomize app with no helm should route to ArgoDirectoryGitParsed")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message for kustomize app")
	}
}

// ---------------------------------------------------------------------------
// Multiple explicit source types lands in ArgoSideParsed as an app error
// ---------------------------------------------------------------------------

func TestBuildAppSpec_MultipleExplicitTypes_ReportsAppError(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			multiSourceTypeApp("conflict-app"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	sideCh := testutil.SubscribeOnceBody(t, bus, subjects.ArgoSideParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	headers := testHeaders("run-50", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), testutil.NoopAck, testutil.NoopNak)

	side := awaitSide(t, sideCh)
	require.Len(t, side, 1)
	assert.Empty(t, side[0].Error, "a per-app error must not be reported as a file error")
	require.Len(t, side[0].Apps, 1)
	assert.Equal(t, "conflict-app", side[0].Apps[0].Name)
	assert.Contains(t, side[0].Apps[0].Error, "multiple explicit source types",
		"app Error should mention multiple explicit source types")
}

// ---------------------------------------------------------------------------
// Unsupported source type (Plugin) returns an error from buildAppSpec
// ---------------------------------------------------------------------------

func TestBuildAppSpec_UnsupportedSourceType_Plugin_ReturnsError(t *testing.T) {
	w := New(testutil.NoopLogger(), nil, nil)

	_, _, err := w.buildAppSpec(context.Background(), pluginApp("plugin-app"))
	require.Error(t, err, "buildAppSpec should fail for unsupported source types")
	assert.Contains(t, err.Error(), "unsupported source type",
		"error should mention unsupported source type")
	assert.Contains(t, err.Error(), "Plugin",
		"error should include the Plugin source type name")
}

func TestParseForArgoResources_DirectoryHelmTogether_NoRegression(t *testing.T) {
	callCount := 0
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		callCount++
		switch callCount {
		case 1:
			return []appv1alpha1.Application{
				helmApp("helm-app", "oci://registry.example.com/charts", "", "my-chart"),
			}, nil
		default:
			return []appv1alpha1.Application{
				directoryApp("directory-app", "https://github.com/example/repo", "manifests/staging", false),
			}, nil
		}
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	ociCh := testutil.SubscribeOnce(t, bus, subjects.ArgoHelmOCIParsed)
	dirCh := testutil.SubscribeOnce(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml", "applicationset_directory.yaml")
	headers := testHeaders("run-33", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml", "applicationset_directory.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-ociCh:
		assert.Equal(t, "applicationset.yaml", hdrs["app.origin"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmOCIParsed message")
	}

	select {
	case hdrs := <-dirCh:
		assert.Equal(t, "applicationset_directory.yaml", hdrs["app.origin"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message")
	}
}

// ---------------------------------------------------------------------------
// Mixed source types in a single file (one renderer call) route correctly
// ---------------------------------------------------------------------------

// TestParseForArgoResources_MixedSourceTypes_InSingleFile_RoutedCorrectly
// drives the path where a single file holds one ApplicationSet whose single
// renderer invocation returns a heterogeneous []Application (a Helm app and a
// Directory app). Each app must be routed to its own downstream subject and
// ArgoSideParsed must list both. The renderer's call count is asserted to be
// exactly 1 to prove both apps came from a single call — distinguishing this
// test from TestParseForArgoResources_DirectoryHelmTogether_NoRegression
// which uses two separate files.
func TestParseForArgoResources_MixedSourceTypes_InSingleFile_RoutedCorrectly(t *testing.T) {
	var rendererCalls int
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		rendererCalls++
		// helmApp("helm-a", ...) with an OCI RepoURL routes to ArgoHelmOCIParsed.
		// directoryApp("dir-a", ...) routes to ArgoDirectoryGitParsed.
		return []appv1alpha1.Application{
			helmApp("helm-a", "oci://registry.example.com/charts", "", "my-chart"),
			directoryApp("dir-a", "https://github.com/example/repo", "manifests/staging", false),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)

	helmHdrCh, helmBodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoHelmOCIParsed)
	dirHdrCh, dirBodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)
	sideCh := testutil.SubscribeOnceBody(t, bus, subjects.ArgoSideParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")

	var ackCount, nakCount int
	ack := func() error { ackCount++; return nil }
	nak := func() error { nakCount++; return nil }

	headers := testHeaders("run-34", snapshotDir)

	require.NotPanics(t, func() {
		w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), ack, nak)
	})

	// Helm app must be routed to its OCI subject and carry helm-a in the body.
	select {
	case hdrs := <-helmHdrCh:
		assert.Equal(t, "applicationset.yaml", hdrs["app.origin"])
		assert.Equal(t, "helm-a", hdrs["app.name"])
		body := <-helmBodyCh
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](body)
		require.NoError(t, err, "failed to unmarshal helm ArgoAppSpec body")
		assert.Equal(t, models.SourceType(models.SourceTypeHelm), decoded.SourceType,
			"helm routing event must carry SourceTypeHelm")
		assert.Equal(t, "helm-a-release", decoded.Helm.ReleaseName,
			"helm routing event body must carry the helm-a release name from the mixed renderer call")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmOCIParsed message (helm-a)")
	}

	// Directory app must be routed to the directory subject and carry dir-a.
	select {
	case hdrs := <-dirHdrCh:
		assert.Equal(t, "applicationset.yaml", hdrs["app.origin"])
		assert.Equal(t, "dir-a", hdrs["app.name"])
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](body)
		require.NoError(t, err, "failed to unmarshal directory ArgoAppSpec body")
		assert.Equal(t, "dir-a", decoded.AppName,
			"directory routing event body must carry the directory app name from the mixed renderer call")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message (dir-a)")
	}

	// ArgoSideParsed must list both apps from the single file.
	side := awaitSide(t, sideCh)
	require.Len(t, side, 1)
	assert.Equal(t, []models.AppParsingResult{{Name: "helm-a"}, {Name: "dir-a"}}, side[0].Apps)

	// The renderer must have been invoked exactly once — both apps came from
	// a single call, which is the whole point of this edge case.
	assert.Equal(t, 1, rendererCalls,
		"rendererFunc must be invoked exactly once for a single ApplicationSet file")
	assert.Equal(t, 1, ackCount, "ack should be called exactly once for a successful mixed-types route")
	assert.Equal(t, 0, nakCount, "nak must not be called on the happy path")
}

// ---------------------------------------------------------------------------
// Nil source type (auto-detect) routes to ArgoDirectoryGitParsed
// ---------------------------------------------------------------------------

func TestBuildAppSpec_NilSourceType_RoutesToDirectoryForAutoDetect(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			autoDetectApp("auto-app", "https://github.com/example/repo", "manifests/staging"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirHdrCh, dirBodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	headers := testHeaders("run-60", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](body)
		require.NoError(t, err, "failed to unmarshal ArgoAppSpec from message body")
		assert.Equal(t, models.SourceType(models.SourceTypeUndefined), decoded.SourceType,
			"SourceType should be SourceTypeUndefined (zero value) when ExplicitType() returns nil — auto-detection is deferred")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message for auto-detect app")
	}
}

// ---------------------------------------------------------------------------
// ArgoCD project propagation for helm apps
// ---------------------------------------------------------------------------

func TestBuildAppSpec_HelmApp_PropagatesProject(t *testing.T) {
	w := New(testutil.NoopLogger(), nil, nil)

	app := helmApp("proj-app", "oci://registry.example.com/charts", "", "my-chart")
	app.Spec.Project = "team-a"

	subject, spec, err := w.buildAppSpec(context.Background(), app)
	require.NoError(t, err, "buildAppSpec should succeed for a helm OCI app")
	assert.Equal(t, subjects.ArgoHelmOCIParsed, subject)
	assert.Equal(t, "team-a", spec.Project,
		"ArgoAppSpec must carry the ArgoCD project so creds can be resolved per project at fetch time")
}

// ---------------------------------------------------------------------------
// Kustomize simple fields propagation
// ---------------------------------------------------------------------------

func TestBuildAppSpec_Kustomize_SimpleFields(t *testing.T) {
	kustSpec := &appv1alpha1.ApplicationSourceKustomize{
		NamePrefix:             "pre-",
		NameSuffix:             "-suf",
		Namespace:              "custom-ns",
		CommonLabels:           map[string]string{"env": "staging", "team": "platform"},
		CommonAnnotations:      map[string]string{"owner": "platform-team"},
		ForceCommonLabels:      true,
		ForceCommonAnnotations: true,
		Components:             []string{"components/monitoring", "components/logging"},
	}

	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			kustomizeApp("kust-simple", "https://github.com/example/repo", "overlays/staging", kustSpec),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirHdrCh, dirBodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_kustomize.yaml")
	headers := testHeaders("run-41", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset_kustomize.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](body)
		require.NoError(t, err, "failed to unmarshal ArgoAppSpec from message body")

		assert.Equal(t, models.SourceType(models.SourceTypeKustomize), decoded.SourceType,
			"SourceType should be SourceTypeKustomize")
		assert.Equal(t, "pre-", decoded.Kustomize.NamePrefix)
		assert.Equal(t, "-suf", decoded.Kustomize.NameSuffix)
		assert.Equal(t, "custom-ns", decoded.Kustomize.Namespace)
		assert.Equal(t, map[string]string{"env": "staging", "team": "platform"}, decoded.Kustomize.CommonLabels)
		assert.Equal(t, map[string]string{"owner": "platform-team"}, decoded.Kustomize.CommonAnnotations)
		assert.True(t, decoded.Kustomize.ForceCommonLabels, "ForceCommonLabels should be true")
		assert.True(t, decoded.Kustomize.ForceCommonAnnotations, "ForceCommonAnnotations should be true")
		assert.Equal(t, []string{"components/monitoring", "components/logging"}, decoded.Kustomize.Components)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message for kustomize simple fields")
	}
}

// ---------------------------------------------------------------------------
// Kustomize images and replicas propagation
// ---------------------------------------------------------------------------

func TestBuildAppSpec_Kustomize_ImagesAndReplicas(t *testing.T) {
	kustSpec := &appv1alpha1.ApplicationSourceKustomize{
		Images: appv1alpha1.KustomizeImages{
			appv1alpha1.KustomizeImage("nginx=nginx:1.25"),
			appv1alpha1.KustomizeImage("redis=redis:7"),
		},
		Replicas: []appv1alpha1.KustomizeReplica{
			{Name: "web", Count: intstr.FromInt(3)},
			{Name: "worker", Count: intstr.FromInt(5)},
		},
	}

	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			kustomizeApp("kust-images", "https://github.com/example/repo", "overlays/prod", kustSpec),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirHdrCh, dirBodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_kustomize.yaml")
	headers := testHeaders("run-42", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset_kustomize.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](body)
		require.NoError(t, err, "failed to unmarshal ArgoAppSpec from message body")

		assert.Equal(t, []string{"nginx=nginx:1.25", "redis=redis:7"}, decoded.Kustomize.Images,
			"Kustomize images should be converted to plain strings")

		require.Len(t, decoded.Kustomize.Replicas, 2, "expected 2 kustomize replicas")
		assert.Equal(t, "web", decoded.Kustomize.Replicas[0].Name)
		assert.Equal(t, int64(3), decoded.Kustomize.Replicas[0].Count)
		assert.Equal(t, "worker", decoded.Kustomize.Replicas[1].Name)
		assert.Equal(t, int64(5), decoded.Kustomize.Replicas[1].Count)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message for kustomize images and replicas")
	}
}

// ---------------------------------------------------------------------------
// Kustomize patches propagation
// ---------------------------------------------------------------------------

func TestBuildAppSpec_Kustomize_Patches(t *testing.T) {
	kustSpec := &appv1alpha1.ApplicationSourceKustomize{
		Patches: []appv1alpha1.KustomizePatch{
			{
				Patch: "- op: replace\n  path: /spec/replicas\n  value: 3",
				Target: &appv1alpha1.KustomizeSelector{
					KustomizeResId: appv1alpha1.KustomizeResId{
						KustomizeGvk: appv1alpha1.KustomizeGvk{
							Group:   "apps",
							Version: "v1",
							Kind:    "Deployment",
						},
						Name:      "web",
						Namespace: "production",
					},
					LabelSelector:      "app=web",
					AnnotationSelector: "team=platform",
				},
			},
			{
				Path: "patches/increase-memory.yaml",
			},
		},
	}

	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			kustomizeApp("kust-patches", "https://github.com/example/repo", "overlays/prod", kustSpec),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirHdrCh, dirBodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_kustomize.yaml")
	headers := testHeaders("run-43", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset_kustomize.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](body)
		require.NoError(t, err, "failed to unmarshal ArgoAppSpec from message body")

		require.Len(t, decoded.Kustomize.Patches, 2, "expected 2 kustomize patches")

		// Patch 0: inline patch with target selector
		assert.Equal(t, "- op: replace\n  path: /spec/replicas\n  value: 3", decoded.Kustomize.Patches[0].Patch)
		require.NotNil(t, decoded.Kustomize.Patches[0].Target, "patch 0 target should not be nil")
		assert.Equal(t, "apps", decoded.Kustomize.Patches[0].Target.Group)
		assert.Equal(t, "v1", decoded.Kustomize.Patches[0].Target.Version)
		assert.Equal(t, "Deployment", decoded.Kustomize.Patches[0].Target.Kind)
		assert.Equal(t, "web", decoded.Kustomize.Patches[0].Target.Name)
		assert.Equal(t, "production", decoded.Kustomize.Patches[0].Target.Namespace)
		assert.Equal(t, "app=web", decoded.Kustomize.Patches[0].Target.LabelSelector)
		assert.Equal(t, "team=platform", decoded.Kustomize.Patches[0].Target.AnnotationSelector)

		// Patch 1: file-based patch without target
		assert.Equal(t, "patches/increase-memory.yaml", decoded.Kustomize.Patches[1].Path)
		assert.Nil(t, decoded.Kustomize.Patches[1].Target, "patch 1 target should be nil")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message for kustomize patches")
	}
}

// ---------------------------------------------------------------------------
// Helm ValueFiles + Parameters propagation
// ---------------------------------------------------------------------------

func TestBuildAppSpec_Helm_ParametersAndValueFiles(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			helmAppWithParams("helm-params", "oci://registry.example.com/charts", "my-chart"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	hdrCh, bodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoHelmOCIParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	headers := testHeaders("run-80", snapshotDir)

	w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-hdrCh:
		body := <-bodyCh
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](body)
		require.NoError(t, err)

		assert.Equal(t, models.SourceType(models.SourceTypeHelm), decoded.SourceType)
		assert.Equal(t, "helm-params-release", decoded.Helm.ReleaseName)
		assert.Equal(t, map[string]any{"image": map[string]any{"tag": "v1.0.0"}}, decoded.Helm.Values)
		assert.Equal(t, []string{"values-prod.yaml", "values-secrets.yaml"}, decoded.Helm.ValueFiles)

		require.Len(t, decoded.Helm.Parameters, 2)
		assert.Equal(t, "replicas", decoded.Helm.Parameters[0].Name)
		assert.Equal(t, "3", decoded.Helm.Parameters[0].Value)
		assert.False(t, decoded.Helm.Parameters[0].ForceString)
		assert.Equal(t, "debug", decoded.Helm.Parameters[1].Name)
		assert.Equal(t, "true", decoded.Helm.Parameters[1].Value)
		assert.True(t, decoded.Helm.Parameters[1].ForceString)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmOCIParsed message")
	}
}

func TestBuildAppSpec_Helm_NoValues_NoPanic(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			helmAppNoValues("helm-empty", "oci://registry.example.com/charts", "my-chart"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	hdrCh, bodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoHelmOCIParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	headers := testHeaders("run-81", snapshotDir)

	require.NotPanics(t, func() {
		w.parseForArgoResources(context.Background(), headers, filesPayload(t, "applicationset.yaml"), testutil.NoopAck, testutil.NoopNak)
	})

	select {
	case <-hdrCh:
		body := <-bodyCh
		decoded, err := internalnats.Unmarshal[models.ArgoAppSpec](body)
		require.NoError(t, err)

		assert.Equal(t, "helm-empty-release", decoded.Helm.ReleaseName)
		assert.Nil(t, decoded.Helm.Values)
		assert.Nil(t, decoded.Helm.ValueFiles)
		assert.Nil(t, decoded.Helm.Parameters)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmOCIParsed message")
	}
}
