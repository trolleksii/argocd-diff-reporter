package argoworker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	appv1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
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
// handleSnapshottedFiles integration tests
// ---------------------------------------------------------------------------

// argoStreamSubjects lists all subjects the ArgoWorker publishes so the test
// stream covers them all.
var argoStreamSubjects = []string{
	subjects.GitFilesSnapshotted,
	subjects.ArgoHelmOCIParsed,
	subjects.ArgoHelmHTTPParsed,
	subjects.ArgoHelmGitParsed,
	subjects.ArgoEmptyParsed,
	subjects.ArgoAppGenerationFailed,
	subjects.ArgoTotalUpdated,
	subjects.ArgoDirectoryGitParsed,
}

const argoTestStream = "argo-worker-test"

// newTestArgoWorker creates an ArgoWorker wired to a real embedded NATS server.
// customRendererFunc is injected so no live K8s connection is needed.
func newTestArgoWorker(t *testing.T, fn AppSetRendererFunc) (*ArgoWorker, *internalnats.Bus) {
	t.Helper()
	bus, _, _ := testutil.StartNATS(t)

	ctx := context.Background()
	err := bus.EnsureStream(ctx, argoTestStream, argoStreamSubjects)
	require.NoError(t, err, "failed to create NATS test stream")

	w := New(config.ArgoCDConfig{}, testutil.NoopLogger(), bus, fn)
	return w, bus
}

func testHeaders(prNumber, snapshotDir string) internalnats.Headers {
	return internalnats.Headers{
		"pr.number":         prNumber,
		"pr.owner":          "org",
		"pr.repo":           "repo",
		"sha.active":        "headsha",
		"sha.complementary": "basesha",
		"pr.files.snapshot": snapshotDir,
	}
}

// helmApp builds a minimal appv1alpha1.Application with a Helm source.
// repoURL and path control the subject-routing logic in handleSnapshottedFiles.
func helmApp(name, repoURL, path, chart string) appv1alpha1.Application {
	valuesJSON, _ := json.Marshal(map[string]any{})
	return appv1alpha1.Application{
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

// makeSpecsPayload serialises a []models.FileProcessingSpec via msgpack.
func makeSpecsPayload(t *testing.T, specs []models.FileProcessingSpec) []byte {
	t.Helper()
	data, err := internalnats.Marshal(specs)
	require.NoError(t, err)
	return data
}

// ---------------------------------------------------------------------------
// OCI subject routing
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_OCIAppRoutedToOCISubject(t *testing.T) {
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
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset.yaml", ArtifactName: "apps/applicationset.yaml"},
	}
	headers := testHeaders("1", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-ociCh:
		assert.Equal(t, "apps/applicationset.yaml", hdrs["app.origin"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmOCIParsed message")
	}
}

// ---------------------------------------------------------------------------
// Git subject routing
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_GitAppRoutedToGitSubject(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			helmApp("git-app", "git@github.com:example/charts.git", "charts/my-app", ""),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	gitCh := testutil.SubscribeOnce(t, bus, subjects.ArgoHelmGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset.yaml", ArtifactName: "apps/applicationset.yaml"},
	}
	headers := testHeaders("2", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-gitCh:
		assert.Equal(t, "apps/applicationset.yaml", hdrs["app.origin"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmGitParsed message")
	}
}

// ---------------------------------------------------------------------------
// HTTP subject routing
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_HTTPAppRoutedToHTTPSubject(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			helmApp("http-app", "https://charts.example.com", "", "my-chart"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	httpCh := testutil.SubscribeOnce(t, bus, subjects.ArgoHelmHTTPParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset.yaml", ArtifactName: "apps/applicationset.yaml"},
	}
	headers := testHeaders("3", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-httpCh:
		assert.Equal(t, "apps/applicationset.yaml", hdrs["app.origin"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmHTTPParsed message")
	}
}

// ---------------------------------------------------------------------------
// ArgoTotalUpdated count
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_ArgoTotalUpdated_CorrectCount(t *testing.T) {
	// Renderer returns two apps for one ApplicationSet.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			helmApp("app-one", "oci://registry.example.com/charts", "", "chart-one"),
			helmApp("app-two", "oci://registry.example.com/charts", "", "chart-two"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	totalCh := testutil.SubscribeOnce(t, bus, subjects.ArgoTotalUpdated)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset.yaml", ArtifactName: "apps/applicationset.yaml"},
	}
	headers := testHeaders("4", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-totalCh:
		assert.Equal(t, "2", hdrs["app.total"], "ArgoTotalUpdated header should carry the correct app count")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoTotalUpdated message")
	}
}

// ---------------------------------------------------------------------------
// Error handling — invalid file produces error message, not a panic
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_InvalidFile_PublishesErrorNotPanic(t *testing.T) {
	// rendererFunc should never be called for unparseable files.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		t.Fatal("rendererFunc should not be called for invalid YAML files")
		return nil, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	errCh := testutil.SubscribeOnce(t, bus, subjects.ArgoAppGenerationFailed)

	snapshotDir := makeSnapshotDir(t, "invalid.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "invalid.yaml", ArtifactName: "apps/invalid.yaml"},
	}
	headers := testHeaders("5", snapshotDir)

	// Must not panic.
	require.NotPanics(t, func() {
		w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)
	})

	select {
	case hdrs := <-errCh:
		assert.NotEmpty(t, hdrs["error.msg"], "error.msg should be set in the error message")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoAppGenerationFailed message")
	}
}

// ---------------------------------------------------------------------------
// Renderer function error publishes ArgoAppGenerationFailed
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_RendererError_PublishesError(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return nil, fmt.Errorf("renderer exploded")
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	errCh := testutil.SubscribeOnce(t, bus, subjects.ArgoAppGenerationFailed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset.yaml", ArtifactName: "apps/applicationset.yaml"},
	}
	headers := testHeaders("10", snapshotDir)

	require.NotPanics(t, func() {
		w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)
	})

	select {
	case hdrs := <-errCh:
		assert.Equal(t, "renderer exploded", hdrs["error.msg"])
		assert.Equal(t, "apps/applicationset.yaml", hdrs["error.origin"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoAppGenerationFailed message from renderer error")
	}
}

// ---------------------------------------------------------------------------
// Non-existent file produces error message
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_NonExistentFile_PublishesError(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		t.Fatal("rendererFunc should not be called for missing files")
		return nil, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	errCh := testutil.SubscribeOnce(t, bus, subjects.ArgoAppGenerationFailed)

	snapshotDir := t.TempDir() // empty — file won't exist
	specs := []models.FileProcessingSpec{
		{FileName: "does-not-exist.yaml", ArtifactName: "apps/does-not-exist.yaml"},
	}
	headers := testHeaders("6", snapshotDir)

	require.NotPanics(t, func() {
		w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)
	})

	select {
	case hdrs := <-errCh:
		assert.NotEmpty(t, hdrs["error.msg"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoAppGenerationFailed message")
	}
}

// ---------------------------------------------------------------------------
// Empty specs payload publishes ArgoTotalUpdated with app.total == "0"
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_EmptySpecs_PublishesZeroTotal(t *testing.T) {
	// rendererFunc must never be called when there are no specs to process.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		panic("rendererFunc should not be called for empty specs payload")
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	totalCh := testutil.SubscribeOnce(t, bus, subjects.ArgoTotalUpdated)

	var ackCount, nakCount int
	ack := func() error { ackCount++; return nil }
	nak := func() error { nakCount++; return nil }

	headers := testHeaders("7", t.TempDir())

	data := makeSpecsPayload(t, nil)

	require.NotPanics(t, func() {
		w.handleSnapshottedFiles(context.Background(), headers, data, ack, nak)
	})

	select {
	case hdrs := <-totalCh:
		assert.Equal(t, "0", hdrs["app.total"],
			"ArgoTotalUpdated header should carry app.total=0 for empty specs payload")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoTotalUpdated message")
	}

	assert.Equal(t, 1, ackCount, "ack should be called exactly once for empty specs")
	assert.Equal(t, 0, nakCount, "nak should not be called for empty specs")
}

// ---------------------------------------------------------------------------
// Malformed payload naks and skips publishing any routing/total events
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_MalformedPayload_NaksAndSkipsTotal(t *testing.T) {
	// rendererFunc must never be reached — unmarshal must fail first.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		panic("rendererFunc should not be called for malformed payload")
	}
	w, bus := newTestArgoWorker(t, rendererFunc)

	totalCh := testutil.SubscribeOnce(t, bus, subjects.ArgoTotalUpdated)
	failedCh := testutil.SubscribeOnce(t, bus, subjects.ArgoAppGenerationFailed)

	var ackCount, nakCount int
	ack := func() error { ackCount++; return nil }
	nak := func() error { nakCount++; return nil }

	headers := testHeaders("8", t.TempDir())

	// Raw bytes that are guaranteed to fail msgpack decoding as
	// []models.FileProcessingSpec.
	malformed := []byte{0xff, 0xff, 0xff, 0xff, 0xff}

	require.NotPanics(t, func() {
		w.handleSnapshottedFiles(context.Background(), headers, malformed, ack, nak)
	})

	assert.Equal(t, 1, nakCount, "nak should be called exactly once for malformed payload")
	assert.Equal(t, 0, ackCount, "ack should not be called for malformed payload")

	// Neither ArgoTotalUpdated nor ArgoAppGenerationFailed should ever be
	// published on the malformed-payload path — the handler must bail out
	// before the publish/route loop runs.
	select {
	case hdrs := <-totalCh:
		t.Fatalf("unexpected ArgoTotalUpdated delivered for malformed payload: %+v", hdrs)
	case <-time.After(150 * time.Millisecond):
	}

	select {
	case hdrs := <-failedCh:
		t.Fatalf("unexpected ArgoAppGenerationFailed delivered for malformed payload: %+v", hdrs)
	case <-time.After(150 * time.Millisecond):
	}
}

// ---------------------------------------------------------------------------
// Empty ArtifactName falls back to FileName for app.origin header
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_EmptyArtifactName_UsesFileName(t *testing.T) {
	// The snapshot file contains a plain Application (not an ApplicationSet),
	// so parseFileResources puts it directly into the apps slice and the
	// rendererFunc is never invoked. Panic if it is, to prove the path.
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		panic("rendererFunc should not be called when file contains a plain Application")
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirCh := testutil.SubscribeOnce(t, bus, subjects.ArgoDirectoryGitParsed)

	// Write a minimal Directory-source Application under a deliberately
	// misleading filename ("applicationset.yaml") so the assertion cannot
	// accidentally pass by matching an ArtifactName substring.
	snapshotDir := t.TempDir()
	const fileName = "applicationset.yaml"
	appYAML := []byte(`apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: empty-artifact-app
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/example/repo
    targetRevision: HEAD
    path: manifests/staging
  destination:
    server: https://kubernetes.default.svc
    namespace: default
`)
	require.NoError(t, os.WriteFile(filepath.Join(snapshotDir, fileName), appYAML, 0o644))

	specs := []models.FileProcessingSpec{
		{FileName: fileName, ArtifactName: ""},
	}

	var ackCount, nakCount int
	ack := func() error { ackCount++; return nil }
	nak := func() error { nakCount++; return nil }

	headers := testHeaders("9", snapshotDir)

	require.NotPanics(t, func() {
		w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), ack, nak)
	})

	select {
	case hdrs := <-dirCh:
		assert.Equal(t, fileName, hdrs["app.origin"],
			"app.origin header must fall back to FileName when ArtifactName is empty")
		assert.NotEmpty(t, hdrs["app.origin"],
			"app.origin must not be empty — empty-string fallback would be a regression")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message")
	}

	assert.Equal(t, 1, ackCount, "ack should be called exactly once for a successful route")
	assert.Equal(t, 0, nakCount, "nak should not be called on the happy path")
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

// ---------------------------------------------------------------------------
// Directory subject routing
// ---------------------------------------------------------------------------

func TestHandleSnapshottedFiles_DirectoryAppRoutedToDirectoryGitSubject(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			directoryApp("directory-app", "https://github.com/example/repo", "manifests/staging", false),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirCh := testutil.SubscribeOnce(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_directory.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset_directory.yaml", ArtifactName: "apps/applicationset_directory.yaml"},
	}
	headers := testHeaders("30", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-dirCh:
		assert.Equal(t, "apps/applicationset_directory.yaml", hdrs["app.origin"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message")
	}
}

func TestHandleSnapshottedFiles_DirectoryApp_RecurseTrue_PropagatesInBody(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			directoryApp("directory-recurse-app", "https://github.com/example/repo", "manifests/staging", true),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirCh, bodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_directory.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset_directory.yaml", ArtifactName: "apps/applicationset_directory.yaml"},
	}
	headers := testHeaders("31", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirCh:
		decoded, err := internalnats.Unmarshal[models.AppSpec](<-bodyCh)
		require.NoError(t, err)
		assert.True(t, decoded.Directory.Recurse, "Directory.Recurse should be true when set on the application source")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message")
	}
}

func TestRouteApp_ExplicitDirectory_SourceTypeAndRecurse(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			directoryApp("explicit-dir-app", "https://github.com/example/repo", "manifests/staging", true),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirHdrCh, dirBodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_directory.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset_directory.yaml", ArtifactName: "apps/applicationset_directory.yaml"},
	}
	headers := testHeaders("35", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.AppSpec](body)
		require.NoError(t, err, "failed to unmarshal AppSpec from message body")
		assert.Equal(t, models.SourceType(models.SourceTypeDirectory), decoded.SourceType,
			"SourceType should be SourceTypeDirectory for explicit Directory source type")
		assert.True(t, decoded.Directory.Recurse,
			"Directory.Recurse should be true when set on the application source")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message for explicit directory app")
	}
}

func TestHandleSnapshottedFiles_DirectoryDeletedApp_PublishesEmptyParsed(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			directoryApp("directory-app", "https://github.com/example/repo", "manifests/staging", false),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	emptyCh := testutil.SubscribeOnce(t, bus, subjects.ArgoEmptyParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_directory.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset_directory.yaml", ArtifactName: "apps/applicationset_directory.yaml", HasNoCounterpart: true},
	}
	headers := testHeaders("32", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-emptyCh:
		assert.Equal(t, "directory-app", hdrs["app.name"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoEmptyParsed message")
	}
}

// ---------------------------------------------------------------------------
// Kustomize app (no helm) routes to ArgoDirectoryGitParsed
// ---------------------------------------------------------------------------

// TestHandleSnapshottedFiles_KustomizeAppRoutedToDirectoryGitSubject verifies
// that an Application whose source has spec.source.kustomize != nil but no
// Helm spec is treated as a plain-directory app and published to
// ArgoDirectoryGitParsed (not skipped). This is the unified routing contract
// introduced when the Kustomize-specific pipeline was removed.
func TestHandleSnapshottedFiles_KustomizeAppRoutedToDirectoryGitSubject(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		// Build an application that has kustomize set but no helm.
		app := directoryApp("kustomize-app", "https://github.com/example/repo", "overlays/staging", false)
		app.Spec.Source.Kustomize = &appv1alpha1.ApplicationSourceKustomize{}
		return []appv1alpha1.Application{app}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirCh := testutil.SubscribeOnce(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset_kustomize.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset_kustomize.yaml", ArtifactName: "apps/applicationset_kustomize.yaml"},
	}
	headers := testHeaders("40", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-dirCh:
		assert.Equal(t, "apps/applicationset_kustomize.yaml", hdrs["app.origin"],
			"kustomize app with no helm should route to ArgoDirectoryGitParsed")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message for kustomize app")
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
// error path in routeApp.
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

// ---------------------------------------------------------------------------
// Multiple explicit source types publishes ArgoAppGenerationFailed
// ---------------------------------------------------------------------------

func TestRouteApp_MultipleExplicitTypes_PublishesError(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			multiSourceTypeApp("conflict-app"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	errCh := testutil.SubscribeOnce(t, bus, subjects.ArgoAppGenerationFailed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset.yaml", ArtifactName: "apps/applicationset.yaml"},
	}
	headers := testHeaders("50", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-errCh:
		assert.Contains(t, hdrs["error.msg"], "multiple explicit source types",
			"error.msg should mention multiple explicit source types")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoAppGenerationFailed message for multiple source types")
	}
}

// ---------------------------------------------------------------------------
// Unsupported source type (Plugin) publishes ArgoAppGenerationFailed
// ---------------------------------------------------------------------------

func TestRouteApp_UnsupportedSourceType_Plugin_PublishesError(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return nil, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	errCh := testutil.SubscribeOnce(t, bus, subjects.ArgoAppGenerationFailed)

	headers := internalnats.Headers{
		"pr.number":         "70",
		"pr.owner":          "org",
		"pr.repo":           "repo",
		"sha.active":        "headsha",
		"sha.complementary": "basesha",
		"app.origin":        "apps/plugin-app.yaml",
	}

	err := w.routeApp(context.Background(), pluginApp("plugin-app"), headers, false)
	require.NoError(t, err, "routeApp should return nil for unsupported source types")

	select {
	case hdrs := <-errCh:
		assert.Equal(t, "plugin-app", hdrs["error.origin"],
			"error.origin should be the unsupported app's name")
		assert.Contains(t, hdrs["error.msg"], "unsupported source type",
			"error.msg should mention unsupported source type")
		assert.Contains(t, hdrs["error.msg"], "Plugin",
			"error.msg should include the Plugin source type name")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoAppGenerationFailed message for plugin source type")
	}
}

func TestHandleSnapshottedFiles_DirectoryHelmTogether_NoRegression(t *testing.T) {
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
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset.yaml", ArtifactName: "apps/applicationset.yaml"},
		{FileName: "applicationset_directory.yaml", ArtifactName: "apps/applicationset_directory.yaml"},
	}
	headers := testHeaders("33", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-ociCh:
		assert.Equal(t, "apps/applicationset.yaml", hdrs["app.origin"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoHelmOCIParsed message")
	}

	select {
	case hdrs := <-dirCh:
		assert.Equal(t, "apps/applicationset_directory.yaml", hdrs["app.origin"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message")
	}
}

// ---------------------------------------------------------------------------
// Mixed source types in a single file (one renderer call) route correctly
// ---------------------------------------------------------------------------

// TestHandleSnapshottedFiles_MixedSourceTypes_InSingleFile_RoutedCorrectly
// drives the path where a single FileProcessingSpec points at one
// ApplicationSet file whose single renderer invocation returns a heterogeneous
// []Application (a Helm app and a Directory app). Each app must be routed to
// its own downstream subject and ArgoTotalUpdated must report the aggregated
// count. The renderer's call count is asserted to be exactly 1 to prove both
// apps came from a single call — distinguishing this test from
// TestHandleSnapshottedFiles_DirectoryHelmTogether_NoRegression which uses
// two separate files.
func TestHandleSnapshottedFiles_MixedSourceTypes_InSingleFile_RoutedCorrectly(t *testing.T) {
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
	totalCh := testutil.SubscribeOnce(t, bus, subjects.ArgoTotalUpdated)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset.yaml", ArtifactName: "apps/applicationset.yaml"},
	}

	var ackCount, nakCount int
	ack := func() error { ackCount++; return nil }
	nak := func() error { nakCount++; return nil }

	headers := testHeaders("34", snapshotDir)

	require.NotPanics(t, func() {
		w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), ack, nak)
	})

	// Helm app must be routed to its OCI subject and carry helm-a in the body.
	// The helmApp test helper does not set ObjectMeta.Name (only the Helm
	// ReleaseName embeds the name), so we match on ReleaseName and SourceType
	// to prove this was the helm-a app from the mixed renderer call.
	select {
	case hdrs := <-helmHdrCh:
		assert.Equal(t, "apps/applicationset.yaml", hdrs["app.origin"])
		body := <-helmBodyCh
		decoded, err := internalnats.Unmarshal[models.AppSpec](body)
		require.NoError(t, err, "failed to unmarshal helm AppSpec body")
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
		assert.Equal(t, "apps/applicationset.yaml", hdrs["app.origin"])
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.AppSpec](body)
		require.NoError(t, err, "failed to unmarshal directory AppSpec body")
		assert.Equal(t, "dir-a", decoded.AppName,
			"directory routing event body must carry the directory app name from the mixed renderer call")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message (dir-a)")
	}

	// ArgoTotalUpdated must aggregate both apps from the single file.
	select {
	case hdrs := <-totalCh:
		assert.Equal(t, "2", hdrs["app.total"],
			"ArgoTotalUpdated must report app.total=2 for a single file yielding two apps of mixed source types")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoTotalUpdated message")
	}

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

func TestRouteApp_NilSourceType_RoutesToDirectoryForAutoDetect(t *testing.T) {
	rendererFunc := func(_ appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		return []appv1alpha1.Application{
			autoDetectApp("auto-app", "https://github.com/example/repo", "manifests/staging"),
		}, nil
	}
	w, bus := newTestArgoWorker(t, rendererFunc)
	dirHdrCh, dirBodyCh := testutil.SubscribeOnceWithBody(t, bus, subjects.ArgoDirectoryGitParsed)

	snapshotDir := makeSnapshotDir(t, "applicationset.yaml")
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset.yaml", ArtifactName: "apps/applicationset.yaml"},
	}
	headers := testHeaders("60", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.AppSpec](body)
		require.NoError(t, err, "failed to unmarshal AppSpec from message body")
		assert.Equal(t, models.SourceType(models.SourceTypeUndefined), decoded.SourceType,
			"SourceType should be SourceTypeUndefined (zero value) when ExplicitType() returns nil — auto-detection is deferred")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ArgoDirectoryGitParsed message for auto-detect app")
	}
}

// ---------------------------------------------------------------------------
// Kustomize simple fields propagation
// ---------------------------------------------------------------------------

func TestRouteApp_Kustomize_SimpleFields(t *testing.T) {
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
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset_kustomize.yaml", ArtifactName: "apps/applicationset_kustomize.yaml"},
	}
	headers := testHeaders("41", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.AppSpec](body)
		require.NoError(t, err, "failed to unmarshal AppSpec from message body")

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

func TestRouteApp_Kustomize_ImagesAndReplicas(t *testing.T) {
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
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset_kustomize.yaml", ArtifactName: "apps/applicationset_kustomize.yaml"},
	}
	headers := testHeaders("42", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.AppSpec](body)
		require.NoError(t, err, "failed to unmarshal AppSpec from message body")

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

func TestRouteApp_Kustomize_Patches(t *testing.T) {
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
	specs := []models.FileProcessingSpec{
		{FileName: "applicationset_kustomize.yaml", ArtifactName: "apps/applicationset_kustomize.yaml"},
	}
	headers := testHeaders("43", snapshotDir)

	w.handleSnapshottedFiles(context.Background(), headers, makeSpecsPayload(t, specs), testutil.NoopAck, testutil.NoopNak)

	select {
	case <-dirHdrCh:
		body := <-dirBodyCh
		decoded, err := internalnats.Unmarshal[models.AppSpec](body)
		require.NoError(t, err, "failed to unmarshal AppSpec from message body")

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
