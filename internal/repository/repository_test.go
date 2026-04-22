package repository

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixtureRepo creates an in-memory git repository with two commits:
//
//   - base commit: contains "unchanged.yaml" and "old.yaml"
//   - head commit: contains "unchanged.yaml", "new.yaml" (old.yaml renamed/replaced)
//
// It returns the Repository struct (with a started queue poller), the base SHA,
// and the head SHA.
func buildFixtureRepo(t *testing.T, snapshotsDir string) (*Repository, string, string) {
	t.Helper()

	store := memory.NewStorage()
	r, err := git.Init(store, nil)
	require.NoError(t, err)

	sig := &object.Signature{
		Name:  "Test Author",
		Email: "test@example.com",
		When:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	// Helper: create a blob in the object store and return its hash.
	storeBlob := func(content string) plumbing.Hash {
		t.Helper()
		obj := store.NewEncodedObject()
		obj.SetType(plumbing.BlobObject)
		w, werr := obj.Writer()
		require.NoError(t, werr)
		_, werr = w.Write([]byte(content))
		require.NoError(t, werr)
		require.NoError(t, w.Close())
		h, serr := store.SetEncodedObject(obj)
		require.NoError(t, serr)
		return h
	}

	// Helper: build a tree from name->hash pairs.
	buildTree := func(entries []object.TreeEntry) plumbing.Hash {
		t.Helper()
		tree := &object.Tree{Entries: entries}
		obj := store.NewEncodedObject()
		require.NoError(t, tree.Encode(obj))
		h, serr := store.SetEncodedObject(obj)
		require.NoError(t, serr)
		return h
	}

	// Helper: create a commit with the given tree and parents.
	makeCommit := func(treeHash plumbing.Hash, parents []plumbing.Hash, msg string) plumbing.Hash {
		t.Helper()
		commit := &object.Commit{
			Author:       *sig,
			Committer:    *sig,
			Message:      msg,
			TreeHash:     treeHash,
			ParentHashes: parents,
		}
		obj := store.NewEncodedObject()
		require.NoError(t, commit.Encode(obj))
		h, serr := store.SetEncodedObject(obj)
		require.NoError(t, serr)
		return h
	}

	unchangedBlob := storeBlob("unchanged content\n")
	oldBlob := storeBlob("old file content\n")

	// go-git requires tree entries to be sorted lexicographically by name.
	baseTreeHash := buildTree([]object.TreeEntry{
		{Name: "old.yaml", Mode: 0100644, Hash: oldBlob},
		{Name: "unchanged.yaml", Mode: 0100644, Hash: unchangedBlob},
	})
	baseHash := makeCommit(baseTreeHash, nil, "base commit")

	newBlob := storeBlob("new file content\n")

	headTreeHash := buildTree([]object.TreeEntry{
		{Name: "new.yaml", Mode: 0100644, Hash: newBlob},
		{Name: "unchanged.yaml", Mode: 0100644, Hash: unchangedBlob},
	})
	headHash := makeCommit(headTreeHash, []plumbing.Hash{baseHash}, "head commit")

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	repo := &Repository{
		repo:         r,
		snapshotsDir: snapshotsDir,
		log:          log,
		queue:        make(chan request, 8),
	}
	go repo.startQueuePoller(ctx)

	return repo, baseHash.String(), headHash.String()
}

// ---- listChangedFiles -------------------------------------------------------

func TestListChangedFiles_ReturnsCorrectChanges(t *testing.T) {
	snapshotsDir := t.TempDir()
	repo, base, head := buildFixtureRepo(t, snapshotsDir)

	changes, err := repo.listChangedFiles(base, head)
	require.NoError(t, err)

	// Collect From and To names for easy assertion.
	var fromNames, toNames []string
	for _, c := range changes {
		if c.From != "" {
			fromNames = append(fromNames, c.From)
		}
		if c.To != "" {
			toNames = append(toNames, c.To)
		}
	}
	sort.Strings(fromNames)
	sort.Strings(toNames)

	// old.yaml was removed (From="old.yaml", To="")
	// new.yaml was added (From="", To="new.yaml")
	assert.Contains(t, fromNames, "old.yaml", "old.yaml should appear as a removed file")
	assert.Contains(t, toNames, "new.yaml", "new.yaml should appear as an added file")

	// unchanged.yaml must not appear in the diff.
	assert.NotContains(t, fromNames, "unchanged.yaml")
	assert.NotContains(t, toNames, "unchanged.yaml")
}

func TestListChangedFiles_EmptyBaseOrHead(t *testing.T) {
	snapshotsDir := t.TempDir()
	repo, base, head := buildFixtureRepo(t, snapshotsDir)

	_, err := repo.listChangedFiles("", head)
	// go-git resolves empty hash to zero hash; the zero hash will not exist in
	// our in-memory store, so we expect an error.
	assert.Error(t, err, "empty base should return an error")

	_, err = repo.listChangedFiles(base, "")
	assert.Error(t, err, "empty head should return an error")
}

func TestListChangedFiles_InvalidSHAs(t *testing.T) {
	snapshotsDir := t.TempDir()
	repo, _, _ := buildFixtureRepo(t, snapshotsDir)

	_, err := repo.listChangedFiles("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", "cafecafecafecafecafecafecafecafecafecafe0")
	assert.Error(t, err)
}

// ---- GetOrCreateSnapshot ----------------------------------------------------

func TestGetOrCreateSnapshot_CreateOnFirstCall(t *testing.T) {
	snapshotsDir := t.TempDir()
	repo, _, head := buildFixtureRepo(t, snapshotsDir)

	// Ask for a snapshot of "new.yaml" at the head commit.
	dir, err := repo.GetOrCreateSnapshot(head, "", []string{"new.yaml"})
	require.NoError(t, err)
	assert.NotEmpty(t, dir)

	// The snapshot directory must exist on disk.
	info, err := os.Stat(dir)
	require.NoError(t, err, "snapshot directory should exist after creation")
	assert.True(t, info.IsDir())

	// new.yaml must be present inside the snapshot.
	assert.FileExists(t, filepath.Join(dir, "new.yaml"))
}

func TestGetOrCreateSnapshot_CacheHitOnSecondCall(t *testing.T) {
	snapshotsDir := t.TempDir()
	repo, _, head := buildFixtureRepo(t, snapshotsDir)

	dir1, err := repo.GetOrCreateSnapshot(head, "", []string{"new.yaml"})
	require.NoError(t, err)

	// Second call must return the same directory and must not fail.
	dir2, err := repo.GetOrCreateSnapshot(head, "", []string{"new.yaml"})
	require.NoError(t, err)

	assert.Equal(t, dir1, dir2, "second call should return the cached snapshot directory")
}

func TestGetOrCreateSnapshot_DifferentFilesDifferentDirs(t *testing.T) {
	snapshotsDir := t.TempDir()
	repo, _, head := buildFixtureRepo(t, snapshotsDir)

	dir1, err := repo.GetOrCreateSnapshot(head, "", []string{"new.yaml"})
	require.NoError(t, err)

	dir2, err := repo.GetOrCreateSnapshot(head, "", []string{"unchanged.yaml"})
	require.NoError(t, err)

	assert.NotEqual(t, dir1, dir2, "different file sets should produce different snapshot directories")
}

func TestGetOrCreateSnapshot_DirSnapshot(t *testing.T) {
	snapshotsDir := t.TempDir()
	repo, _, head := buildFixtureRepo(t, snapshotsDir)

	// Request a full-directory snapshot (repoDir=".", all root files).
	dir, err := repo.GetOrCreateSnapshot(head, ".", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, dir)

	info, err := os.Stat(dir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())

	// Both files from the head tree should be present.
	assert.FileExists(t, filepath.Join(dir, "unchanged.yaml"))
	assert.FileExists(t, filepath.Join(dir, "new.yaml"))
}

func TestGetOrCreateSnapshot_ValidationErrors(t *testing.T) {
	snapshotsDir := t.TempDir()
	repo, _, head := buildFixtureRepo(t, snapshotsDir)

	// ref must be set.
	_, err := repo.GetOrCreateSnapshot("", "", []string{"new.yaml"})
	assert.Error(t, err)

	// repoDir and files are mutually exclusive.
	_, err = repo.GetOrCreateSnapshot(head, "some/dir", []string{"new.yaml"})
	assert.Error(t, err)

	// Either repoDir or files must be set.
	_, err = repo.GetOrCreateSnapshot(head, "", nil)
	assert.Error(t, err)
}
