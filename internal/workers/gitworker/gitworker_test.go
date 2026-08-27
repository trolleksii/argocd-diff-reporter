package gitworker

// Integration tests for GitWorker — same package so unexported fields are accessible.
//
// Part 1: Pure function tests (no NATS, no git I/O).
//   - TestGlobMatches
//   - TestFilterAndSplitChanges
//
// Part 2: Handler integration tests.
//   Because getOrCreateRepo requires a live *githubauth.GithubCredManager, the
//   handler tests bypass it entirely by pre-populating w.repos with an
//   in-memory repository built via repository.NewTestRepository (or a stub
//   RepositoryProvider). This avoids any network calls and keeps the tests
//   fast and hermetic.
//
//   Tests in this section:
//   - TestHandlePRChanged_PublishesMatchedAndResolved
//   - TestHandleFilesResolved_PublishesFilesSnapshotted
//   - TestHandleHelmGitParsed_PublishesChartFetched (via fetchSource)
//   - TestHandleDirectoryGitParsed_PublishesDirectoryFetched (via fetchSource)
//   - TestFetchSource_SnapshotError_PublishesRenderFinished

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/keys"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/repository"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

const testStreamName = "gitworker-test"

// allTestSubjects covers every subject the git worker publishes or consumes,
// so a single stream backs all handler integration tests.
var allTestSubjects = []string{
	subjects.WebhookPRChanged,
	subjects.GitFilesMatched,
	subjects.GitFilesResolved,
	subjects.GitFilesSnapshotted,
	subjects.GitChartFetched,
	subjects.GitDirectoryFetched,
	subjects.ArgoHelmGitParsed,
	subjects.ArgoDirectoryGitParsed,
	subjects.ManifestRenderFinished,
}

// newTestWorker creates a GitWorker wired to an in-process NATS bus. The
// caller is responsible for populating w.repos before invoking any handler.
func newTestWorker(t *testing.T) (*GitWorker, *internalnats.Bus) {
	t.Helper()
	bus, _, _ := testutil.StartNATS(t)
	ctx := context.Background()
	err := bus.EnsureStream(ctx, testStreamName, allTestSubjects)
	require.NoError(t, err)

	w := &GitWorker{
		cfg: config.GitWorkerConfig{
			FileGlobs: []string{"*.yaml", "**/*.yaml"},
		},
		log:   testutil.NoopLogger().With("worker", "git"),
		bus:   bus,
		repos: make(map[string]RepositoryProvider),
	}
	return w, bus
}

// ---------------------------------------------------------------------------
// Part 1: Pure function tests
// ---------------------------------------------------------------------------

// TestGlobMatches verifies that globMatches correctly matches files against a
// list of glob patterns using filepath.Match semantics.
//
// filepath.Match treats "**" as two literal characters, NOT as a recursive
// wildcard. As a consequence:
//   - "**/*.yaml" matches exactly one directory level deep: "dir/file.yaml"
//   - "**/*.yaml" does NOT match two levels deep: "a/b/file.yaml"
//   - "**/*.yaml" does NOT match a root-level file: "file.yaml"
//
// This is the same behaviour exposed through globMatches, and tests below
// document it explicitly.
func TestGlobMatches(t *testing.T) {
	tests := []struct {
		name  string
		file  string
		globs []string
		want  bool
	}{
		{
			name:  "single star matches root yaml",
			file:  "app.yaml",
			globs: []string{"*.yaml"},
			want:  true,
		},
		{
			name:  "single star does not match nested yaml",
			file:  "charts/app.yaml",
			globs: []string{"*.yaml"},
			want:  false,
		},
		{
			// filepath.Match: ** matches any single path segment, so **/*.yaml
			// matches exactly one directory deep.
			name:  "double star matches one level deep yaml",
			file:  "charts/app.yaml",
			globs: []string{"**/*.yaml"},
			want:  true,
		},
		{
			// Two directory levels exceed what **/*.yaml can match.
			name:  "double star does not match two levels deep",
			file:  "a/b/app.yaml",
			globs: []string{"**/*.yaml"},
			want:  false,
		},
		{
			// No directory component — **/*.yaml requires exactly one segment.
			name:  "double star does not match root yaml",
			file:  "app.yaml",
			globs: []string{"**/*.yaml"},
			want:  false,
		},
		{
			name:  "empty file never matches",
			file:  "",
			globs: []string{"*.yaml"},
			want:  false,
		},
		{
			name:  "no globs never matches",
			file:  "app.yaml",
			globs: []string{},
			want:  false,
		},
		{
			name:  "exact match",
			file:  "apps/my-app.yaml",
			globs: []string{"apps/my-app.yaml"},
			want:  true,
		},
		{
			name:  "wildcard matches any extension",
			file:  "app.json",
			globs: []string{"*"},
			want:  true,
		},
		{
			name:  "first glob matches — second is irrelevant",
			file:  "app.yaml",
			globs: []string{"*.yaml", "*.json"},
			want:  true,
		},
		{
			name:  "second glob matches when first does not",
			file:  "app.json",
			globs: []string{"*.yaml", "*.json"},
			want:  true,
		},
		{
			name:  "non-matching pattern returns false",
			file:  "app.yaml",
			globs: []string{"*.json", "*.toml"},
			want:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := globMatches(tc.file, tc.globs)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestFilterAndSplitChanges verifies that filterAndSplitChanges populates the
// from (base) and to (head) slices independently: a change contributes its
// From name only to from, and its To name only to to. A base-only change must
// never leak into to, a head-only change must never leak into from, and a
// rename must keep the old name on the base side and the new name on the head
// side.
func TestFilterAndSplitChanges(t *testing.T) {
	globs := []string{"*.yaml"}

	tests := []struct {
		name     string
		changes  []repository.Change
		globs    []string
		wantFrom []string
		wantTo   []string
	}{
		{
			name:     "modified file — same name on both sides",
			changes:  []repository.Change{{From: "app.yaml", To: "app.yaml"}},
			wantFrom: []string{"app.yaml"},
			wantTo:   []string{"app.yaml"},
		},
		{
			name:     "deleted file — base side only",
			changes:  []repository.Change{{From: "deleted.yaml", To: ""}},
			wantFrom: []string{"deleted.yaml"},
			wantTo:   nil,
		},
		{
			name:     "added file — head side only",
			changes:  []repository.Change{{From: "", To: "added.yaml"}},
			wantFrom: nil,
			wantTo:   []string{"added.yaml"},
		},
		{
			name:     "renamed file — old name on base, new name on head",
			changes:  []repository.Change{{From: "old.yaml", To: "new.yaml"}},
			wantFrom: []string{"old.yaml"},
			wantTo:   []string{"new.yaml"},
		},
		{
			name:     "only from matches glob — to dropped",
			changes:  []repository.Change{{From: "app.yaml", To: "app.json"}},
			wantFrom: []string{"app.yaml"},
			wantTo:   nil,
		},
		{
			name:     "only to matches glob — from dropped",
			changes:  []repository.Change{{From: "app.json", To: "app.yaml"}},
			wantFrom: nil,
			wantTo:   []string{"app.yaml"},
		},
		{
			name:     "non-matching file excluded from both sides",
			changes:  []repository.Change{{From: "script.sh", To: "script.sh"}},
			wantFrom: nil,
			wantTo:   nil,
		},
		{
			name: "mixed changes — each side built independently",
			changes: []repository.Change{
				{From: "app.yaml", To: "app.yaml"},      // both
				{From: "script.sh", To: "script.sh"},    // neither
				{From: "", To: "new.yaml"},              // head only
				{From: "gone.yaml", To: ""},             // base only
				{From: "before.yaml", To: "after.yaml"}, // rename
			},
			wantFrom: []string{"app.yaml", "gone.yaml", "before.yaml"},
			wantTo:   []string{"app.yaml", "new.yaml", "after.yaml"},
		},
		{
			name:     "empty input returns nil slices",
			changes:  nil,
			wantFrom: nil,
			wantTo:   nil,
		},
		{
			name:     "empty globs excludes everything",
			changes:  []repository.Change{{From: "app.yaml", To: "app.yaml"}},
			globs:    []string{},
			wantFrom: nil,
			wantTo:   nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := tc.globs
			if g == nil {
				g = globs
			}
			from, to := filterAndSplitChanges(tc.changes, g)
			assert.Equal(t, tc.wantFrom, from, "from (base side)")
			assert.Equal(t, tc.wantTo, to, "to (head side)")
		})
	}
}

// ---------------------------------------------------------------------------
// Part 2: Handler integration tests
// ---------------------------------------------------------------------------

// repoKey returns the canonical map key used by the git worker for a GitHub repo.
func repoKey(owner, repo string) string {
	return fmt.Sprintf("https://github.com/%s/%s", owner, repo)
}

// stubRepo is an in-memory RepositoryProvider for handler integration tests.
// It returns a configurable list of changes from ListChangedFiles and a fixed
// snapshot path from GetOrCreateSnapshot.
type stubRepo struct {
	changes []repository.Change
}

func (s *stubRepo) ListChangedFiles(_, _ string) ([]repository.Change, error) {
	return s.changes, nil
}

func (s *stubRepo) GetOrCreateSnapshot(_, _ string, _ []string) (string, error) {
	return "snapshot-path", nil
}

// stubRepoError is a RepositoryProvider whose methods always return an error.
type stubRepoError struct{ err error }

func (s *stubRepoError) ListChangedFiles(_, _ string) ([]repository.Change, error) {
	return nil, s.err
}

func (s *stubRepoError) GetOrCreateSnapshot(_, _ string, _ []string) (string, error) {
	return "", s.err
}

// stubAuth is an AuthProvider whose GetBasicHTTPAuth always returns an error.
type stubAuth struct{ err error }

func (s *stubAuth) GetBasicHTTPAuth() (*githttp.BasicAuth, error) {
	return nil, s.err
}

// TestHandlePRChanged_PublishesMatchedAndResolved verifies that handlePRChanged,
// given a stubbed repository returning one changed file present on both sides,
// publishes subjects.GitFilesMatched (Msg-Id keys.MsgIDSides) followed by two
// subjects.GitFilesResolved messages — one per side — carrying the pr.* headers,
// file.withBase/file.withHead, and sha.active set to that side's SHA.
func TestHandlePRChanged_PublishesMatchedAndResolved(t *testing.T) {
	w, bus := newTestWorker(t)

	stub := &stubRepo{
		changes: []repository.Change{
			{From: "app.yaml", To: "app.yaml"},
		},
	}
	w.repos[repoKey("myorg", "myrepo")] = stub

	matchedCh := testutil.SubscribeOnce(t, bus, subjects.GitFilesMatched)
	resolvedCh := testutil.SubscribeN(t, bus, subjects.GitFilesResolved, 2)

	pr := models.PullRequest{PullRequestMeta: models.PullRequestMeta{
		Owner:   "myorg",
		Repo:    "myrepo",
		Number:  "1",
		BaseSHA: "base-sha",
		HeadSHA: "head-sha",
	}}
	data, err := internalnats.Marshal(pr)
	require.NoError(t, err)

	const runId = "run-pr-changed"
	var ackCalled bool
	ack := func() error { ackCalled = true; return nil }

	ctx := context.Background()
	w.handlePRChanged(ctx, internalnats.Headers{"RunId": runId}, data, ack, testutil.NoopNak)
	assert.True(t, ackCalled, "ack should be called after publishing")

	select {
	case hdrs := <-matchedCh:
		assert.Equal(t, "myorg", hdrs["pr.owner"])
		assert.Equal(t, "myrepo", hdrs["pr.repo"])
		assert.Equal(t, "1", hdrs["pr.number"])
		assert.Equal(t, "base-sha", hdrs["pr.sha.base"])
		assert.Equal(t, "head-sha", hdrs["pr.sha.head"])
		assert.Equal(t, keys.MsgIDSides(runId), hdrs[keys.MsgIDHeader])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for git.files.matched message")
	}

	var received []internalnats.Headers
	deadline := time.After(3 * time.Second)
	for len(received) < 2 {
		select {
		case hdrs := <-resolvedCh:
			received = append(received, hdrs)
		case <-deadline:
			t.Fatalf("timed out waiting for git.files.resolved messages, got %d of 2", len(received))
		}
	}

	var shaActiveValues []string
	for _, hdrs := range received {
		assert.Equal(t, "myorg", hdrs["pr.owner"])
		assert.Equal(t, "myrepo", hdrs["pr.repo"])
		assert.Equal(t, "true", hdrs["file.withBase"])
		assert.Equal(t, "true", hdrs["file.withHead"])
		assert.Equal(t, keys.MsgIDFiles("myorg", "myrepo", "1", runId, hdrs["sha.active"]), hdrs[keys.MsgIDHeader])
		shaActiveValues = append(shaActiveValues, hdrs["sha.active"])
	}
	assert.ElementsMatch(t, []string{"base-sha", "head-sha"}, shaActiveValues)
}

// TestHandlePRChanged_RepositoryError_Naks verifies that when the repository's
// ListChangedFiles returns an error, handlePRChanged calls nak and does not
// publish any message to subjects.GitFilesResolved.
func TestHandlePRChanged_RepositoryError_Naks(t *testing.T) {
	w, bus := newTestWorker(t)

	w.repos[repoKey("org", "repo")] = &stubRepoError{err: errors.New("list failed")}

	// Register a consumer so any accidental publish would be captured.
	resolvedCh := testutil.SubscribeN(t, bus, subjects.GitFilesResolved, 1)

	pr := models.PullRequest{PullRequestMeta: models.PullRequestMeta{
		Owner:   "org",
		Repo:    "repo",
		Number:  "1",
		BaseSHA: "b",
		HeadSHA: "h",
	}}
	data, err := internalnats.Marshal(pr)
	require.NoError(t, err)

	var nakCalled, ackCalled bool
	nak := func() error { nakCalled = true; return nil }
	ack := func() error { ackCalled = true; return nil }

	ctx := context.Background()
	w.handlePRChanged(ctx, internalnats.Headers{"RunId": "run-repo-error"}, data, ack, nak)

	assert.True(t, nakCalled, "nak should be called on repository error")
	assert.False(t, ackCalled, "ack should not be called on repository error")

	select {
	case <-resolvedCh:
		t.Fatal("unexpected message published to git.files.resolved")
	case <-time.After(200 * time.Millisecond):
		// silence confirmed
	}
}

// TestHandlePRChanged_MissingRepo_Naks verifies that when w.repos does not
// contain the requested URL, getOrCreateRepo falls through to
// repository.NewRepository which calls w.auth.GetBasicHTTPAuth(). When auth
// returns an error, handlePRChanged calls nak and publishes nothing.
func TestHandlePRChanged_MissingRepo_Naks(t *testing.T) {
	w, bus := newTestWorker(t)

	// Leave w.repos empty (cache miss) and inject a failing auth.
	w.auth = &stubAuth{err: errors.New("auth failed")}

	resolvedCh := testutil.SubscribeN(t, bus, subjects.GitFilesResolved, 1)

	pr := models.PullRequest{PullRequestMeta: models.PullRequestMeta{
		Owner:   "org2",
		Repo:    "repo2",
		Number:  "2",
		BaseSHA: "b2",
		HeadSHA: "h2",
	}}
	data, err := internalnats.Marshal(pr)
	require.NoError(t, err)

	var nakCalled, ackCalled bool
	nak := func() error { nakCalled = true; return nil }
	ack := func() error { ackCalled = true; return nil }

	ctx := context.Background()
	w.handlePRChanged(ctx, internalnats.Headers{"RunId": "run-missing-repo"}, data, ack, nak)

	assert.True(t, nakCalled, "nak should be called when auth fails during repo creation")
	assert.False(t, ackCalled, "ack should not be called when auth fails during repo creation")

	select {
	case <-resolvedCh:
		t.Fatal("unexpected message published to git.files.resolved")
	case <-time.After(200 * time.Millisecond):
		// silence confirmed
	}
}

// TestHandleFilesResolved_PublishesFilesSnapshotted verifies that
// handleFilesResolved, given a valid file list and a pre-populated repository,
// publishes subjects.GitFilesSnapshotted with pr.files.snapshot set.
func TestHandleFilesResolved_PublishesFilesSnapshotted(t *testing.T) {
	w, bus := newTestWorker(t)

	snapshotsDir := t.TempDir()
	tr := repository.NewTestRepository(t, snapshotsDir)

	const (
		owner = "test"
		repo  = "repo"
		runId = "run-files-resolved"
	)
	w.repos[repoKey(owner, repo)] = tr.Repo

	snapshottedCh := testutil.SubscribeOnce(t, bus, subjects.GitFilesSnapshotted)

	data, err := internalnats.Marshal([]string{"new.yaml"})
	require.NoError(t, err)

	headers := internalnats.Headers{
		"RunId":      runId,
		"pr.number":  "1",
		"pr.owner":   owner,
		"pr.repo":    repo,
		"sha.active": tr.HeadSHA,
	}

	ctx := context.Background()
	w.handleFilesResolved(ctx, headers, data, testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-snapshottedCh:
		assert.Equal(t, owner, hdrs["pr.owner"])
		assert.Equal(t, repo, hdrs["pr.repo"])
		assert.Equal(t, tr.HeadSHA, hdrs["sha.active"])
		assert.NotEmpty(t, hdrs["pr.files.snapshot"], "snapshot path should be set in headers")
		assert.Equal(t, keys.MsgIDSnapshot(owner, repo, "1", runId, tr.HeadSHA), hdrs[keys.MsgIDHeader])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for git.files.snapshotted message")
	}
}

// TestHandleHelmGitParsed_PublishesChartFetched verifies that fetchSource, when
// invoked via handleHelmGitParsed with a valid ArgoAppSpec referencing a
// pre-populated repository, publishes subjects.GitChartFetched with the
// chart.location header set.
func TestHandleHelmGitParsed_PublishesChartFetched(t *testing.T) {
	w, bus := newTestWorker(t)

	snapshotsDir := t.TempDir()
	tr := repository.NewTestRepository(t, snapshotsDir)

	const repoURL = "https://github.com/test/charts"
	w.repos[repoURL] = tr.Repo

	chartFetchedCh := testutil.SubscribeOnce(t, bus, subjects.GitChartFetched)

	spec := models.ArgoAppSpec{
		AppName:   "my-app",
		Namespace: "default",
		Source: models.ArgoAppSource{
			RepoURL:  repoURL,
			Revision: tr.HeadSHA,
			Path:     ".",
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"RunId":      "run-helm-git",
		"pr.owner":   "test",
		"pr.repo":    "charts",
		"pr.number":  "5",
		"sha.active": tr.HeadSHA,
		"app.origin": "apps/my-app.yaml",
	}

	ctx := context.Background()
	w.handleHelmGitParsed(ctx, headers, data, testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-chartFetchedCh:
		assert.NotEmpty(t, hdrs["chart.location"], "chart.location header must be set on success")
		assert.Empty(t, hdrs["error.msg"])
		assert.Equal(t, keys.MsgIDFetched("test", "charts", "5", "run-helm-git", tr.HeadSHA, "apps/my-app.yaml", "my-app"), hdrs[keys.MsgIDHeader])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for git.chart.fetched message")
	}
}

// TestHandleDirectoryGitParsed_PublishesDirectoryFetched verifies that
// fetchSource, when invoked via handleDirectoryGitParsed with a valid
// ArgoAppSpec referencing a pre-populated repository, publishes
// subjects.GitDirectoryFetched with the chart.location header set.
func TestHandleDirectoryGitParsed_PublishesDirectoryFetched(t *testing.T) {
	w, bus := newTestWorker(t)

	stub := &stubRepo{}
	const repoURL = "https://github.com/test/directory-repo"
	w.repos[repoURL] = stub

	directoryFetchedCh := testutil.SubscribeOnce(t, bus, subjects.GitDirectoryFetched)

	spec := models.ArgoAppSpec{
		AppName:   "my-directory-app",
		Namespace: "default",
		Source: models.ArgoAppSource{
			RepoURL:  repoURL,
			Revision: "abc123",
			Path:     ".",
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"RunId":      "run-dir-git",
		"pr.owner":   "test",
		"pr.repo":    "directory-repo",
		"pr.number":  "9",
		"sha.active": "abc123",
		"app.origin": "apps/my-directory-app.yaml",
	}

	ctx := context.Background()
	w.handleDirectoryGitParsed(ctx, headers, data, testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-directoryFetchedCh:
		assert.NotEmpty(t, hdrs["chart.location"], "chart.location header must be set on success")
		assert.Empty(t, hdrs["error.msg"])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for git.directory.fetched message")
	}
}

// TestFetchSource_SnapshotError_PublishesRenderFinished verifies that when the
// repository's GetOrCreateSnapshot fails, fetchSource publishes
// subjects.ManifestRenderFinished with error.msg set (no chart.location) and
// acks the message.
func TestFetchSource_SnapshotError_PublishesRenderFinished(t *testing.T) {
	w, bus := newTestWorker(t)

	const repoURL = "https://github.com/test/broken"
	w.repos[repoURL] = &stubRepoError{err: errors.New("snapshot failed")}

	finishedCh := testutil.SubscribeOnce(t, bus, subjects.ManifestRenderFinished)
	chartFetchedCh := testutil.SubscribeOnce(t, bus, subjects.GitChartFetched)

	spec := models.ArgoAppSpec{
		AppName: "broken-app",
		Source: models.ArgoAppSource{
			RepoURL:  repoURL,
			Revision: "abc123",
			Path:     "chart",
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"RunId":      "run-snapshot-error",
		"pr.owner":   "test",
		"pr.repo":    "broken",
		"pr.number":  "7",
		"sha.active": "abc123",
		"app.origin": "apps/broken-app.yaml",
	}

	var ackCalled, nakCalled bool
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	ctx := context.Background()
	w.handleHelmGitParsed(ctx, headers, data, ack, nak)

	assert.True(t, ackCalled, "ack should be called on fetch failure")
	assert.False(t, nakCalled, "nak should not be called on fetch failure")

	select {
	case hdrs := <-finishedCh:
		assert.Equal(t, "snapshot failed", hdrs["error.msg"])
		assert.Empty(t, hdrs["chart.location"])
		assert.Equal(t, keys.MsgIDRender("test", "broken", "7", "run-snapshot-error", "abc123", "apps/broken-app.yaml", "broken-app"), hdrs[keys.MsgIDHeader])
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for coordinator.manifest.render.complete message")
	}

	select {
	case <-chartFetchedCh:
		t.Fatal("unexpected message published to git.chart.fetched")
	case <-time.After(200 * time.Millisecond):
		// silence confirmed
	}
}

// ---------------------------------------------------------------------------
// Part 3: Unmarshal error tests
// ---------------------------------------------------------------------------

// TestHandlePRChanged_UnmarshalError_Naks verifies that handlePRChanged calls
// nak (not ack) and publishes nothing when given unparseable data.
func TestHandlePRChanged_UnmarshalError_Naks(t *testing.T) {
	w, bus := newTestWorker(t)

	resolvedCh := testutil.SubscribeOnce(t, bus, subjects.GitFilesResolved)

	var ackCalled, nakCalled bool
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	ctx := context.Background()
	w.handlePRChanged(ctx, internalnats.Headers{"RunId": "run-pr-unmarshal"}, []byte("invalid"), ack, nak)

	assert.True(t, nakCalled, "nak should be called on unmarshal error")
	assert.False(t, ackCalled, "ack should not be called on unmarshal error")

	select {
	case <-resolvedCh:
		t.Fatal("expected no message on git.files.resolved")
	case <-time.After(200 * time.Millisecond):
		// correct — no message
	}
}

// TestHandleFilesResolved_UnmarshalError_Naks verifies that handleFilesResolved
// calls nak (not ack) and publishes nothing when given unparseable data.
func TestHandleFilesResolved_UnmarshalError_Naks(t *testing.T) {
	w, bus := newTestWorker(t)

	snapshottedCh := testutil.SubscribeOnce(t, bus, subjects.GitFilesSnapshotted)

	var ackCalled, nakCalled bool
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"RunId":      "run-files-unmarshal",
		"pr.number":  "1",
		"pr.owner":   "org",
		"pr.repo":    "repo",
		"sha.active": "abc123",
	}

	ctx := context.Background()
	w.handleFilesResolved(ctx, headers, []byte("invalid"), ack, nak)

	assert.True(t, nakCalled, "nak should be called on unmarshal error")
	assert.False(t, ackCalled, "ack should not be called on unmarshal error")

	select {
	case <-snapshottedCh:
		t.Fatal("expected no message on git.files.snapshotted")
	case <-time.After(200 * time.Millisecond):
		// correct — no message
	}
}

// TestHandleHelmGitParsed_UnmarshalError_Naks verifies that handleHelmGitParsed
// calls nak (not ack) and publishes nothing when given unparseable data.
func TestHandleHelmGitParsed_UnmarshalError_Naks(t *testing.T) {
	w, bus := newTestWorker(t)

	chartFetchedCh := testutil.SubscribeOnce(t, bus, subjects.GitChartFetched)

	var ackCalled, nakCalled bool
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"RunId":      "run-helm-unmarshal",
		"pr.owner":   "org",
		"pr.repo":    "repo",
		"pr.number":  "1",
		"app.origin": "apps/my-app.yaml",
	}

	ctx := context.Background()
	w.handleHelmGitParsed(ctx, headers, []byte("invalid"), ack, nak)

	assert.True(t, nakCalled, "nak should be called on unmarshal error")
	assert.False(t, ackCalled, "ack should not be called on unmarshal error")

	select {
	case <-chartFetchedCh:
		t.Fatal("expected no message on git.chart.fetched")
	case <-time.After(200 * time.Millisecond):
		// correct — no message
	}
}

// TestHandleDirectoryGitParsed_UnmarshalError_Naks verifies that
// handleDirectoryGitParsed calls nak (not ack) and publishes nothing when given
// unparseable data.
func TestHandleDirectoryGitParsed_UnmarshalError_Naks(t *testing.T) {
	w, bus := newTestWorker(t)

	directoryFetchedCh := testutil.SubscribeOnce(t, bus, subjects.GitDirectoryFetched)

	var ackCalled, nakCalled bool
	ack := func() error { ackCalled = true; return nil }
	nak := func() error { nakCalled = true; return nil }

	headers := internalnats.Headers{
		"RunId":      "run-dir-unmarshal",
		"pr.owner":   "org",
		"pr.repo":    "repo",
		"pr.number":  "1",
		"app.origin": "apps/my-app.yaml",
	}

	ctx := context.Background()
	w.handleDirectoryGitParsed(ctx, headers, []byte("invalid"), ack, nak)

	assert.True(t, nakCalled, "nak should be called on unmarshal error")
	assert.False(t, ackCalled, "ack should not be called on unmarshal error")

	select {
	case <-directoryFetchedCh:
		t.Fatal("expected no message on git.directory.fetched")
	case <-time.After(200 * time.Millisecond):
		// correct — no message
	}
}
