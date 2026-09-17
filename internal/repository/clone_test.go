package repository

// Tests for the CLI-based init/fetch path. A local source repository stands
// in for the remote — the git CLI accepts plain paths, which also makes the
// previously untestable NewRepository flow testable.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gitOut runs a git command for fixture setup and returns trimmed stdout.
func gitOut(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@test",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@test",
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "git %v: %s", args, out)
	return strings.TrimSpace(string(out))
}

// commitFile writes content to name in srcDir, commits, and returns the SHA.
func commitFile(t *testing.T, srcDir, name, content, msg string) string {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, name), []byte(content), 0o644))
	gitOut(t, "-C", srcDir, "add", name)
	gitOut(t, "-C", srcDir, "commit", "-q", "-m", msg)
	return gitOut(t, "-C", srcDir, "rev-parse", "HEAD")
}

func TestNewRepository_CloneFetchSnapshotDiff(t *testing.T) {
	srcDir := t.TempDir()
	gitOut(t, "init", "-q", srcDir)
	// Allow fetching arbitrary SHAs, like GitHub does — fetchRefSpecs fetches
	// PR commits by raw SHA.
	gitOut(t, "-C", srcDir, "config", "uploadpack.allowAnySHA1InWant", "true")

	base := commitFile(t, srcDir, "app.yaml", "version: 1\n", "base")
	head := commitFile(t, srcDir, "app.yaml", "version: 2\n", "head")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	r, err := NewRepository(ctx, srcDir, t.TempDir(), t.TempDir(), noopAuth{}, log)
	require.NoError(t, err, "init of a local repository should succeed")

	// The repo starts empty — the diff must fetch both SHAs with history.
	changes, err := r.ListChangedFiles(base, head)
	require.NoError(t, err, "diff fetch via the CLI should succeed")
	assert.Equal(t, []Change{{From: "app.yaml", To: "app.yaml"}}, changes)

	// Snapshot from the object database (bare repo, no worktree).
	snapDir, err := r.GetOrCreateSnapshot(base, "", []string{"app.yaml"})
	require.NoError(t, err)
	content, err := os.ReadFile(filepath.Join(snapDir, "app.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "version: 1\n", string(content))

	// A commit created after the initial fetch; the diff then requires go-git
	// to see the pack the CLI wrote (reopen-after-fetch).
	third := commitFile(t, srcDir, "app.yaml", "version: 3\n", "third")
	changes, err = r.ListChangedFiles(head, third)
	require.NoError(t, err, "fetch of a new SHA should succeed via the CLI")
	assert.Equal(t, []Change{{From: "app.yaml", To: "app.yaml"}}, changes)

	// And a fourth commit exercises fetchRef's depth-1 fetch inside snapshotting.
	fourth := commitFile(t, srcDir, "app.yaml", "version: 4\n", "fourth")
	snapDir, err = r.GetOrCreateSnapshot(fourth, "", []string{"app.yaml"})
	require.NoError(t, err, "snapshot of a new SHA should fetch via the CLI")
	content, err = os.ReadFile(filepath.Join(snapDir, "app.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "version: 4\n", string(content))
}

// TestNewRepository_SnapshotByHEAD covers targetRevision: HEAD — the most
// common ArgoCD revision — which must resolve via the remote's HEAD even
// though the local bare repo's HEAD is unborn.
func TestNewRepository_SnapshotByHEAD(t *testing.T) {
	srcDir := t.TempDir()
	gitOut(t, "init", "-q", srcDir)
	commitFile(t, srcDir, "app.yaml", "version: 1\n", "base")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	r, err := NewRepository(ctx, srcDir, t.TempDir(), t.TempDir(), noopAuth{}, log)
	require.NoError(t, err)

	snapDir, err := r.GetOrCreateSnapshot("HEAD", "", []string{"app.yaml"})
	require.NoError(t, err, "snapshot by HEAD should fetch the remote's HEAD")
	content, err := os.ReadFile(filepath.Join(snapDir, "app.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "version: 1\n", string(content))

	// A named ref must not serve a stale snapshot once the remote moves on.
	branch := gitOut(t, "-C", srcDir, "rev-parse", "--abbrev-ref", "HEAD")
	commitFile(t, srcDir, "app.yaml", "version: 2\n", "second")
	for _, ref := range []string{"HEAD", branch} {
		newDir, err := r.GetOrCreateSnapshot(ref, "", []string{"app.yaml"})
		require.NoError(t, err, "snapshot by %s after a new commit", ref)
		require.NotEqual(t, snapDir, newDir, "moved ref %s should yield a new snapshot", ref)
		content, err = os.ReadFile(filepath.Join(newDir, "app.yaml"))
		require.NoError(t, err)
		assert.Equal(t, "version: 2\n", string(content), "ref %s", ref)
	}
}

// TestNewRepository_ShallowSnapshotThenDiff covers the role transition: a repo
// used only for snapshots (depth-1, shallow history) later serves a PR diff,
// which must unshallow to compute the merge-base.
func TestNewRepository_ShallowSnapshotThenDiff(t *testing.T) {
	srcDir := t.TempDir()
	gitOut(t, "init", "-q", srcDir)
	gitOut(t, "-C", srcDir, "config", "uploadpack.allowAnySHA1InWant", "true")

	base := commitFile(t, srcDir, "app.yaml", "version: 1\n", "base")
	head := commitFile(t, srcDir, "app.yaml", "version: 2\n", "head")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	r, err := NewRepository(ctx, srcDir, t.TempDir(), t.TempDir(), noopAuth{}, log)
	require.NoError(t, err)

	// Snapshot first: depth-1 fetch leaves the repo shallow.
	snapDir, err := r.GetOrCreateSnapshot(head, "", []string{"app.yaml"})
	require.NoError(t, err)
	content, err := os.ReadFile(filepath.Join(snapDir, "app.yaml"))
	require.NoError(t, err)
	assert.Equal(t, "version: 2\n", string(content))
	assert.True(t, r.isShallow(), "a depth-1 snapshot fetch should leave shallow history")

	// The diff over the same repo must unshallow and find the merge-base.
	changes, err := r.ListChangedFiles(base, head)
	require.NoError(t, err, "diff over a previously shallow repo should unshallow")
	assert.Equal(t, []Change{{From: "app.yaml", To: "app.yaml"}}, changes)
	assert.False(t, r.isShallow(), "the diff fetch should have unshallowed the repository")
}

func TestNewRepository_ReopensExistingClone(t *testing.T) {
	srcDir := t.TempDir()
	gitOut(t, "init", "-q", srcDir)
	commitFile(t, srcDir, "app.yaml", "version: 1\n", "base")

	cloneRoot := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	r1, err := NewRepository(ctx, srcDir, cloneRoot, t.TempDir(), noopAuth{}, log)
	require.NoError(t, err)

	// Second construction over the same cloneRoot must reuse the clone.
	r2, err := NewRepository(ctx, srcDir, cloneRoot, t.TempDir(), noopAuth{}, log)
	require.NoError(t, err)
	assert.Equal(t, r1.cloneDir, r2.cloneDir)
}
