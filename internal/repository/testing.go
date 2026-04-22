package repository

// This file exposes test-only helpers for constructing local-filesystem-backed
// Repository instances. It intentionally lives outside of any _test.go file so
// that other packages (e.g. gitworker) can import it in their own tests.
//
// IMPORTANT: Do not use these helpers in production code.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	gogithttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

// noopAuth is a GitAuthProvider that returns an empty credential pair. It is
// used by test repositories that point at a local file:// remote, where no
// real authentication is needed.
type noopAuth struct{}

func (noopAuth) GetBasicHTTPAuth() (*gogithttp.BasicAuth, error) {
	return &gogithttp.BasicAuth{}, nil
}

// TestRepo bundles a Repository together with the SHAs of its two
// fixture commits so callers can use them without knowing the internals.
type TestRepo struct {
	Repo    *Repository
	BaseSHA string
	HeadSHA string
}

// NewTestRepository creates a self-contained Repository suitable for handler
// integration tests. It:
//
//  1. Initialises a bare "server" git repository on the filesystem.
//  2. Commits two commits:
//     - base commit: "unchanged.yaml" + "old.yaml"
//     - head commit: "unchanged.yaml" + "new.yaml" (old.yaml replaced by new.yaml)
//  3. Clones the bare repo into a second temp directory so the clone has a
//     valid "origin" remote pointing at the bare repo via file://.
//  4. Wraps the clone in a Repository with a noopAuth so that fetchRefSpecs
//     calls succeed (they return NoErrAlreadyUpToDate because both commits are
//     already present locally after the clone).
//
// The returned Repository has its queue poller already running; t.Cleanup
// cancels it automatically when the test ends.
func NewTestRepository(t *testing.T, snapshotsDir string) *TestRepo {
	t.Helper()

	// ---- 1. Create a bare repo -------------------------------------------
	bareDir := t.TempDir()
	bareRepo, err := git.PlainInit(bareDir, true) // bare=true
	if err != nil {
		t.Fatalf("NewTestRepository: init bare repo: %v", err)
	}

	sig := &object.Signature{
		Name:  "Test Author",
		Email: "test@example.com",
		When:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	storer := bareRepo.Storer

	// storeBlob writes raw content as a git blob and returns its hash.
	storeBlob := func(content string) plumbing.Hash {
		t.Helper()
		obj := storer.NewEncodedObject()
		obj.SetType(plumbing.BlobObject)
		w, err := obj.Writer()
		if err != nil {
			t.Fatalf("NewTestRepository: blob writer: %v", err)
		}
		if _, err = w.Write([]byte(content)); err != nil {
			t.Fatalf("NewTestRepository: blob write: %v", err)
		}
		if err = w.Close(); err != nil {
			t.Fatalf("NewTestRepository: blob close: %v", err)
		}
		h, err := storer.SetEncodedObject(obj)
		if err != nil {
			t.Fatalf("NewTestRepository: set blob: %v", err)
		}
		return h
	}

	// buildTree encodes a tree from the given entries and stores it.
	buildTree := func(entries []object.TreeEntry) plumbing.Hash {
		t.Helper()
		tree := &object.Tree{Entries: entries}
		obj := storer.NewEncodedObject()
		if err := tree.Encode(obj); err != nil {
			t.Fatalf("NewTestRepository: encode tree: %v", err)
		}
		h, err := storer.SetEncodedObject(obj)
		if err != nil {
			t.Fatalf("NewTestRepository: set tree: %v", err)
		}
		return h
	}

	// makeCommit encodes and stores a commit, updating HEAD.
	makeCommit := func(treeHash plumbing.Hash, parents []plumbing.Hash, msg string) plumbing.Hash {
		t.Helper()
		commit := &object.Commit{
			Author:       *sig,
			Committer:    *sig,
			Message:      msg,
			TreeHash:     treeHash,
			ParentHashes: parents,
		}
		obj := storer.NewEncodedObject()
		if err := commit.Encode(obj); err != nil {
			t.Fatalf("NewTestRepository: encode commit: %v", err)
		}
		h, err := storer.SetEncodedObject(obj)
		if err != nil {
			t.Fatalf("NewTestRepository: set commit: %v", err)
		}
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

	// Point HEAD at the head commit so the clone knows the default branch.
	headRef := plumbing.NewHashReference(plumbing.Master, headHash)
	if err := storer.SetReference(headRef); err != nil {
		t.Fatalf("NewTestRepository: set HEAD ref: %v", err)
	}

	// ---- 2. Clone the bare repo into a working directory ------------------
	cloneDir := t.TempDir()
	bareURL := fmt.Sprintf("file://%s", bareDir)
	clonedRepo, err := git.PlainClone(cloneDir, false, &git.CloneOptions{
		URL: bareURL,
	})
	if err != nil {
		t.Fatalf("NewTestRepository: clone: %v", err)
	}

	// ---- 3. Wrap in Repository --------------------------------------------
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	repo := &Repository{
		repo:         clonedRepo,
		cloneDir:     cloneDir,
		snapshotsDir: snapshotsDir,
		auth:         noopAuth{},
		log:          log,
		queue:        make(chan request, 8),
	}
	go repo.startQueuePoller(ctx)

	return &TestRepo{
		Repo:    repo,
		BaseSHA: baseHash.String(),
		HeadSHA: headHash.String(),
	}
}
