package gitworker

// Integration tests for GitWorker — same package so unexported fields are accessible.
//
// Part 1: Pure function tests (no NATS, no git I/O).
//   - TestGlobMatches
//   - TestFilterAndSplitChanges
//   - TestFilterChanges
//
// Part 2: Handler integration tests.
//   Because getOrCreateRepo requires a live *githubauth.GithubCredManager, the
//   handler tests bypass it entirely by pre-populating w.repos with an
//   in-memory repository built via repository.NewTestRepository. This avoids
//   any network calls and keeps the tests fast and hermetic.
//
//   Tests in this section:
//   - TestHandlePRChanged_PublishesFilesResolved
//   - TestHandleFilesResolved_PublishesFilesSnapshotted
//   - TestHandleChartFetch_PublishesChartFetched (via snapshotFetchHandler)

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
	subjects.GitFilesResolved,
	subjects.GitFilesSnapshotted,
	subjects.GitChartFetched,
	subjects.GitChartFetchFailed,
	subjects.ArgoHelmGitParsed,
	subjects.ArgoHelmHTTPParsed,
	subjects.ArgoHelmOCIParsed,
	subjects.ArgoDirectoryGitParsed,
	subjects.GitDirectoryFetched,
	subjects.GitDirectoryFetchFailed,
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

// TestFilterChanges verifies that filterChanges keeps only changes where at
// least one side matches a glob, and blanks out sides that do not match.
func TestFilterChanges(t *testing.T) {
	globs := []string{"*.yaml"}

	t.Run("both sides match — both retained", func(t *testing.T) {
		changes := []repository.Change{
			{From: "base.yaml", To: "head.yaml"},
		}
		result := filterChanges(changes, globs)
		require.Len(t, result, 1)
		assert.Equal(t, "base.yaml", result[0].From)
		assert.Equal(t, "head.yaml", result[0].To)
	})

	t.Run("only from matches — to is blanked", func(t *testing.T) {
		changes := []repository.Change{
			{From: "base.yaml", To: "head.json"},
		}
		result := filterChanges(changes, globs)
		require.Len(t, result, 1)
		assert.Equal(t, "base.yaml", result[0].From)
		assert.Equal(t, "", result[0].To, "non-matching To should be blanked")
	})

	t.Run("only to matches — from is blanked", func(t *testing.T) {
		changes := []repository.Change{
			{From: "base.json", To: "head.yaml"},
		}
		result := filterChanges(changes, globs)
		require.Len(t, result, 1)
		assert.Equal(t, "", result[0].From, "non-matching From should be blanked")
		assert.Equal(t, "head.yaml", result[0].To)
	})

	t.Run("neither side matches — change excluded", func(t *testing.T) {
		changes := []repository.Change{
			{From: "base.json", To: "head.json"},
		}
		result := filterChanges(changes, globs)
		assert.Empty(t, result)
	})

	t.Run("mixed changes — only matching ones retained", func(t *testing.T) {
		changes := []repository.Change{
			{From: "app.yaml", To: "app.yaml"},
			{From: "script.sh", To: "script.sh"},
			{From: "config.yaml", To: ""},
		}
		result := filterChanges(changes, globs)
		require.Len(t, result, 2)
	})

	t.Run("empty input returns empty slice", func(t *testing.T) {
		result := filterChanges(nil, globs)
		assert.Nil(t, result)
	})

	t.Run("empty globs excludes all changes", func(t *testing.T) {
		changes := []repository.Change{
			{From: "app.yaml", To: "app.yaml"},
		}
		result := filterChanges(changes, []string{})
		assert.Empty(t, result)
	})
}

// TestFilterAndSplitChanges verifies that filterAndSplitChanges correctly
// populates from and to slices and sets HasNoCounterpart where appropriate.
func TestFilterAndSplitChanges(t *testing.T) {
	globs := []string{"*.yaml"}

	t.Run("modified file — both sides match same name", func(t *testing.T) {
		changes := []repository.Change{
			{From: "app.yaml", To: "app.yaml"},
		}
		from, to := filterAndSplitChanges(changes, globs)

		require.Len(t, from, 1)
		assert.Equal(t, "app.yaml", from[0].FileName)
		assert.Equal(t, "app.yaml", from[0].ArtifactName)
		assert.False(t, from[0].HasNoCounterpart)

		require.Len(t, to, 1)
		assert.Equal(t, "app.yaml", to[0].FileName)
		assert.False(t, to[0].HasNoCounterpart)
	})

	t.Run("renamed file — ArtifactName uses To name on from side", func(t *testing.T) {
		changes := []repository.Change{
			{From: "old.yaml", To: "new.yaml"},
		}
		from, to := filterAndSplitChanges(changes, globs)

		require.Len(t, from, 1)
		assert.Equal(t, "old.yaml", from[0].FileName)
		assert.Equal(t, "new.yaml", from[0].ArtifactName, "ArtifactName should use the To name when different from From")
		assert.False(t, from[0].HasNoCounterpart)

		require.Len(t, to, 1)
		assert.Equal(t, "new.yaml", to[0].FileName)
		assert.False(t, to[0].HasNoCounterpart)
	})

	t.Run("deleted file — from has no counterpart", func(t *testing.T) {
		changes := []repository.Change{
			{From: "deleted.yaml", To: ""},
		}
		from, to := filterAndSplitChanges(changes, globs)

		require.Len(t, from, 1)
		assert.Equal(t, "deleted.yaml", from[0].FileName)
		assert.True(t, from[0].HasNoCounterpart, "HasNoCounterpart should be true when To is empty")

		assert.Empty(t, to)
	})

	t.Run("added file — to has no counterpart", func(t *testing.T) {
		changes := []repository.Change{
			{From: "", To: "added.yaml"},
		}
		from, to := filterAndSplitChanges(changes, globs)

		assert.Empty(t, from)

		require.Len(t, to, 1)
		assert.Equal(t, "added.yaml", to[0].FileName)
		assert.True(t, to[0].HasNoCounterpart, "HasNoCounterpart should be true when From is empty")
	})

	t.Run("non-matching file excluded from both slices", func(t *testing.T) {
		changes := []repository.Change{
			{From: "script.sh", To: "script.sh"},
		}
		from, to := filterAndSplitChanges(changes, globs)

		assert.Empty(t, from)
		assert.Empty(t, to)
	})

	t.Run("mixed changes — only matching ones appear", func(t *testing.T) {
		changes := []repository.Change{
			{From: "app.yaml", To: "app.yaml"},   // both match
			{From: "script.sh", To: "script.sh"}, // neither matches
			{From: "", To: "new.yaml"},           // only to matches
		}
		from, to := filterAndSplitChanges(changes, globs)

		// from: only "app.yaml" (script.sh excluded, new.yaml has no from)
		require.Len(t, from, 1)
		assert.Equal(t, "app.yaml", from[0].FileName)

		// to: "app.yaml" + "new.yaml" (script.sh excluded)
		require.Len(t, to, 2)
		var toNames []string
		for _, f := range to {
			toNames = append(toNames, f.FileName)
		}
		assert.Contains(t, toNames, "app.yaml")
		assert.Contains(t, toNames, "new.yaml")
	})

	t.Run("only from side matches — to blanked, from has no counterpart", func(t *testing.T) {
		changes := []repository.Change{
			{From: "app.yaml", To: "app.json"},
		}
		from, to := filterAndSplitChanges(changes, globs)

		require.Len(t, from, 1)
		assert.Equal(t, "app.yaml", from[0].FileName)
		// ArtifactName stays as FileName when To was blanked.
		assert.Equal(t, "app.yaml", from[0].ArtifactName)
		assert.True(t, from[0].HasNoCounterpart, "To side was blanked so HasNoCounterpart should be true")

		assert.Empty(t, to)
	})

	t.Run("only to side matches — from blanked, to has no counterpart", func(t *testing.T) {
		changes := []repository.Change{
			{From: "app.json", To: "app.yaml"},
		}
		from, to := filterAndSplitChanges(changes, globs)

		assert.Empty(t, from)

		require.Len(t, to, 1)
		assert.Equal(t, "app.yaml", to[0].FileName)
		assert.True(t, to[0].HasNoCounterpart, "From side was blanked so HasNoCounterpart should be true")
	})

	t.Run("empty input returns nil slices", func(t *testing.T) {
		from, to := filterAndSplitChanges(nil, globs)
		assert.Nil(t, from)
		assert.Nil(t, to)
	})
}

// ---------------------------------------------------------------------------
// Part 2: Handler integration tests
// ---------------------------------------------------------------------------

// repoKey returns the canonical map key used by the git worker for a GitHub repo.
func repoKey(owner, repo string) string {
	return fmt.Sprintf("https://github.com/%s/%s", owner, repo)
}

// NOTE: A full happy-path test for handlePRChanged (publishing GitFilesResolved)
// is not feasible without a production code change. The reason is:
//
//   r.ListChangedFiles() always goes through fetchAndListChangedFiles(), which
//   first performs a git fetch using fetchRefSpecs(sha:sha, sha:sha). The local
//   file:// transport does not support raw-SHA refspecs ("server does not support
//   exact SHA1 refspec"), so the call always fails even when both commits are
//   present in the local object store.
//
//   Fixing this would require either:
//     a) Introducing an interface around Repository so tests can inject a stub, or
//     b) Adding a "try local first" fast-path to fetchAndListChangedFiles.
//
//   NOTE: The missing-repo nak path is also untestable without a refactor. When
//   w.repos does not contain the requested URL, getOrCreateRepo falls through to
//   repository.NewRepository, which calls w.auth.GetBasicHTTPAuth(). Because
//   w.auth is the concrete type *githubauth.GithubCredManager (not an interface),
//   there is no way to supply a non-nil fake implementation from outside the
//   githubauth package. Calling the method on a nil pointer panics.

// TestHandleFilesResolved_PublishesFilesSnapshotted verifies that
// handleFilesResolved, given a valid FileProcessingSpec list and a
// pre-populated repository, publishes subjects.GitFilesSnapshotted.
func TestHandleFilesResolved_PublishesFilesSnapshotted(t *testing.T) {
	w, bus := newTestWorker(t)

	snapshotsDir := t.TempDir()
	tr := repository.NewTestRepository(t, snapshotsDir)

	const (
		owner = "test"
		repo  = "repo"
	)
	w.repos[repoKey(owner, repo)] = tr.Repo

	snapshottedCh := testutil.SubscribeOnce(t, bus, subjects.GitFilesSnapshotted)

	specs := []models.FileProcessingSpec{
		{FileName: "new.yaml", ArtifactName: "new.yaml"},
	}
	data, err := internalnats.Marshal(specs)
	require.NoError(t, err)

	headers := internalnats.Headers{
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
		assert.NotEmpty(t, hdrs["pr.files.snapshot"], "snapshot path should be set in headers")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for git.files.snapshotted message")
	}
}

// TestHandleChartFetch_PublishesChartFetched verifies that snapshotFetchHandler,
// when parameterized for Helm chart subjects and given a valid AppSpec
// referencing a pre-populated repository, publishes subjects.GitChartFetched
// with the chart.location header set.
func TestHandleChartFetch_PublishesChartFetched(t *testing.T) {
	w, bus := newTestWorker(t)

	snapshotsDir := t.TempDir()
	tr := repository.NewTestRepository(t, snapshotsDir)

	const repoURL = "https://github.com/test/charts"
	w.repos[repoURL] = tr.Repo

	chartFetchedCh := testutil.SubscribeOnce(t, bus, subjects.GitChartFetched)

	spec := models.AppSpec{
		AppName:   "my-app",
		Namespace: "default",
		Source: models.AppSource{
			RepoURL:  repoURL,
			Revision: tr.HeadSHA,
			Path:     ".",
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":   "test",
		"pr.repo":    "charts",
		"pr.number":  "5",
		"app.origin": "apps/my-app.yaml",
	}

	ctx := context.Background()
	w.handleHelmGitParsed(ctx, headers, data, testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-chartFetchedCh:
		assert.NotEmpty(t, hdrs["chart.location"], "chart.location header must be set on success")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for git.chart.fetched message")
	}
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

// TestHandlePRChanged_PublishesFilesResolved verifies that handlePRChanged,
// given a stubbed repository returning one changed file, publishes two
// subjects.GitFilesResolved messages — one for the base SHA and one for the
// head SHA — with the correct pr.owner, pr.repo, and sha.active headers.
func TestHandlePRChanged_PublishesFilesResolved(t *testing.T) {
	w, bus := newTestWorker(t)

	stub := &stubRepo{
		changes: []repository.Change{
			{From: "app.yaml", To: "app.yaml"},
		},
	}
	w.repos[repoKey("myorg", "myrepo")] = stub

	resolvedCh := testutil.SubscribeN(t, bus, subjects.GitFilesResolved, 2)

	pr := models.PullRequest{
		Owner:   "myorg",
		Repo:    "myrepo",
		Number:  "1",
		BaseSHA: "base-sha",
		HeadSHA: "head-sha",
	}
	data, err := internalnats.Marshal(pr)
	require.NoError(t, err)

	ctx := context.Background()
	w.handlePRChanged(ctx, internalnats.Headers{}, data, testutil.NoopAck, testutil.NoopNak)

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

	require.Len(t, received, 2)

	var shaActiveValues []string
	for _, hdrs := range received {
		assert.Equal(t, "myorg", hdrs["pr.owner"])
		assert.Equal(t, "myrepo", hdrs["pr.repo"])
		shaActiveValues = append(shaActiveValues, hdrs["sha.active"])
	}
	assert.Contains(t, shaActiveValues, "base-sha")
	assert.Contains(t, shaActiveValues, "head-sha")
}

// TestHandlePRChanged_RepositoryError_Naks verifies that when the repository's
// ListChangedFiles returns an error, handlePRChanged calls nak and does not
// publish any message to subjects.GitFilesResolved.
func TestHandlePRChanged_RepositoryError_Naks(t *testing.T) {
	w, bus := newTestWorker(t)

	w.repos[repoKey("org", "repo")] = &stubRepoError{err: errors.New("list failed")}

	// Register a consumer so any accidental publish would be captured.
	resolvedCh := testutil.SubscribeN(t, bus, subjects.GitFilesResolved, 1)

	pr := models.PullRequest{
		Owner:   "org",
		Repo:    "repo",
		Number:  "1",
		BaseSHA: "b",
		HeadSHA: "h",
	}
	data, err := internalnats.Marshal(pr)
	require.NoError(t, err)

	var nakCalled, ackCalled bool
	nak := func() error { nakCalled = true; return nil }
	ack := func() error { ackCalled = true; return nil }

	ctx := context.Background()
	w.handlePRChanged(ctx, internalnats.Headers{}, data, ack, nak)

	assert.True(t, nakCalled, "nak should be called on repository error")
	assert.False(t, ackCalled, "ack should not be called on repository error")

	select {
	case <-resolvedCh:
		t.Fatal("unexpected message published to git.files.resolved")
	case <-time.After(200 * time.Millisecond):
		// silence confirmed
	}
}

// TestHandleDirectoryFetch_PublishesDirectoryFetched verifies that snapshotFetchHandler,
// when parameterized for Directory subjects and given a valid AppSpec
// referencing a pre-populated repository, publishes subjects.GitDirectoryFetched
// with the chart.location header set.
func TestHandleDirectoryFetch_PublishesDirectoryFetched(t *testing.T) {
	w, bus := newTestWorker(t)

	stub := &stubRepo{}
	const repoURL = "https://github.com/test/directory-repo"
	w.repos[repoURL] = stub

	directoryFetchedCh := testutil.SubscribeOnce(t, bus, subjects.GitDirectoryFetched)

	spec := models.AppSpec{
		AppName:   "my-directory-app",
		Namespace: "default",
		Source: models.AppSource{
			RepoURL:  repoURL,
			Revision: "abc123",
			Path:     ".",
		},
	}
	data, err := internalnats.Marshal(spec)
	require.NoError(t, err)

	headers := internalnats.Headers{
		"pr.owner":   "test",
		"pr.repo":    "directory-repo",
		"pr.number":  "9",
		"app.origin": "apps/my-directory-app.yaml",
	}

	ctx := context.Background()
	w.handleDirectoryGitParsed(ctx, headers, data, testutil.NoopAck, testutil.NoopNak)

	select {
	case hdrs := <-directoryFetchedCh:
		assert.NotEmpty(t, hdrs["chart.location"], "chart.location header must be set on success")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for git.directory.fetched message")
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

	pr := models.PullRequest{
		Owner:   "org2",
		Repo:    "repo2",
		Number:  "2",
		BaseSHA: "b2",
		HeadSHA: "h2",
	}
	data, err := internalnats.Marshal(pr)
	require.NoError(t, err)

	var nakCalled, ackCalled bool
	nak := func() error { nakCalled = true; return nil }
	ack := func() error { ackCalled = true; return nil }

	ctx := context.Background()
	w.handlePRChanged(ctx, internalnats.Headers{}, data, ack, nak)

	assert.True(t, nakCalled, "nak should be called when auth fails during repo creation")
	assert.False(t, ackCalled, "ack should not be called when auth fails during repo creation")

	select {
	case <-resolvedCh:
		t.Fatal("unexpected message published to git.files.resolved")
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
	w.handlePRChanged(ctx, internalnats.Headers{}, []byte("invalid"), ack, nak)

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
