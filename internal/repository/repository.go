package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/trolleksii/argocd-diff-reporter/internal/githubauth"
)

// Repository represents a git repository
type Repository struct {
	repo         *git.Repository
	auth         *githubauth.GithubCredManager
	cloneDir     string
	snapshotsDir string
	log          *slog.Logger
	queue        chan request
}

type request struct {
	Kind     requestKind
	Snapshot snapshotRequest
	Fetch    fetchRequest
	Result   chan Result
}

type requestKind int

const (
	SNAPSHOT_REQUEST requestKind = 0
	FETCH_REQUEST    requestKind = 1
)

type snapshotRequest struct {
	SHA       string
	Directory string
	Files     []string
}

type fetchRequest struct {
	Base, Head string
}

type Result struct {
	SnapshotDir string
	Changes     []Change
	Error       error
}

type Change struct {
	From string
	To   string
}

func (r *Repository) EnqueueFetchRequest(base, head string) chan Result {
	out := make(chan Result, 1)
	r.queue <- request{
		Kind: FETCH_REQUEST,
		Fetch: fetchRequest{
			Base: base,
			Head: head,
		},
		Result: out,
	}
	return out
}

func (r *Repository) EnqueueSnapshotRequest(sha, dir string, files []string) chan Result {
	out := make(chan Result, 1)
	r.queue <- request{
		Kind: SNAPSHOT_REQUEST,
		Snapshot: snapshotRequest{
			SHA:       sha,
			Files:     files,
			Directory: dir,
		},
		Result: out,
	}
	return out
}

func validateFetchRequest(req fetchRequest) error {
	if req.Base == "" || req.Head == "" {
		return errors.New("invalid fetch request: both base and head sha must be set")
	}
	return nil
}

func validateSnapshotRequest(req snapshotRequest) error {
	if req.SHA == "" {
		return errors.New("invalid snapshot request: commit sha is not set")
	}
	if req.Directory != "" && len(req.Files) != 0 {
		return errors.New("invalid snapshot request: files and directory snapshots are mutually exclusive")
	}
	if req.Directory == "" && len(req.Files) == 0 {
		return errors.New("invalid snapshot request: neither directory or files were set")
	}
	return nil
}

func (r *Repository) startQueuePoller(ctx context.Context) {
	defer close(r.queue)
	for {
		select {
		case req, ok := <-r.queue:
			if !ok {
				return
			}
			switch req.Kind {
			case FETCH_REQUEST:
				defer close(req.Result)
				if err := validateFetchRequest(req.Fetch); err != nil {
					r.log.Error("invalid fetch request", "error", err)
				}

				changes, err := r.WrapperThingThatNeedsAName(req.Fetch.Base, req.Fetch.Head)
				if err != nil {
					r.log.Error("failed to pre-fetch branch", "error", err)
				}
				req.Result <- Result{
					Changes: changes,
					Error:   err,
				}
			case SNAPSHOT_REQUEST:
				defer close(req.Result)
				if err := validateSnapshotRequest(req.Snapshot); err != nil {
					r.log.Error("invalid snapshot request", "error", err)
				}
				snapshotDir, err := r.GetOrCreateSnapshot(req.Snapshot.SHA, req.Snapshot.Directory, req.Snapshot.Files)
				req.Result <- Result{
					SnapshotDir: snapshotDir,
					Error:       err,
				}
			default:
				panic("unknown git repository request kind")
			}
		case <-ctx.Done():
			return
		}
	}
}

// NewRepository creates a new repository instance
func NewRepository(ctx context.Context, url, cloneRootDir, snapshotsRootDir string, auth *githubauth.GithubCredManager) (*Repository, error) {
	id := string(xxhash.Sum64String(url))
	cloneDir := filepath.Join(cloneRootDir, id)
	snapshotsDir := filepath.Join(snapshotsRootDir, id)
	// Ensure clone directory exists
	if err := os.MkdirAll(cloneDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create clone directory: %w", err)
	}
	r := &Repository{
		cloneDir:     cloneDir,
		snapshotsDir: snapshotsDir,
		auth:         auth,
		queue:        make(chan request, 2),
	}

	// Try to open existing repository
	if repo, err := git.PlainOpen(cloneDir); err == nil {
		// Verify the remote URL matches
		remote, err := repo.Remote("origin")
		if err == nil {
			remoteConfig := remote.Config()
			if len(remoteConfig.URLs) > 0 && remoteConfig.URLs[0] == url {
				r.repo = repo
				return r, nil
			}
		}
		// Remove the directory and re-clone
		os.RemoveAll(cloneDir)
		os.MkdirAll(cloneDir, 0755)
	}

	// Clone the repository (full clone to ensure we can access any commit)
	httpAuth, err := r.auth.GetBasicHTTPAuth()
	if err != nil {
		return nil, fmt.Errorf("failed to get HTTP auth: %w", err)
	}

	cloneOptions := &git.CloneOptions{
		Auth: httpAuth,
		URL:  url,
	}

	repo, err := git.PlainClone(cloneDir, false, cloneOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to clone repository: %w", err)
	}
	r.repo = repo
	go r.startQueuePoller(ctx)

	return r, nil
}

// TODO: find a better name
func (r *Repository) WrapperThingThatNeedsAName(base, head string) ([]Change, error) {
	if err := r.fetchRefSpecs([]config.RefSpec{
		config.RefSpec(fmt.Sprintf("%s:%s", base, base)),
		config.RefSpec(fmt.Sprintf("%s:%s", head, head)),
	}); err != nil {
		return nil, err
	}
	return r.listChangedFiles(base, head)
}

func (r *Repository) GetOrCreateSnapshot(ref, repoDir string, files []string) (string, error) {
	// TODO: check if snapshot already exists and return it's location right away

	// fetch the ref
	hash, err := r.fetchUnknownRef(ref)
	if err != nil {
		return "", fmt.Errorf("failed to fetch target revision: %w", err)
	}
	var snapshotDir string
	if repoDir != "" {
		fingerprint := string(xxhash.Sum64String(repoDir))
		snapshotDir = filepath.Join(r.snapshotsDir, ref, fingerprint)
		err = r.createDirSnapshot(*hash, repoDir, snapshotDir)
	} else {
		sort.Strings(files)
		fingerprint := string(xxhash.Sum64String(strings.Join(files, ",")))
		snapshotDir = filepath.Join(r.snapshotsDir, ref, fingerprint)
		err = r.createFilesSnapshot(*hash, files, snapshotDir)
	}

	if err != nil {
		os.RemoveAll(snapshotDir)
		return "", fmt.Errorf("failed to resolve target revision: %w", err)
	}
	return snapshotDir, nil
}

// TakeFilesSnapshot takes a snapshot of a list of files at a given git `ref`.
func (r *Repository) TakeFilesSnapshot(ref string, files []string) (string, error) {

	snapshotPath := filepath.Join(r.snapshotsDir, ref)
	hash, _ := r.repo.ResolveRevision(plumbing.Revision(ref))

	err := r.createFilesSnapshot(*hash, files, snapshotPath)
	if err != nil {
		os.RemoveAll(snapshotPath)
		return "", fmt.Errorf("failed to create files snapshot: %w", err)
	}
	return snapshotPath, nil
}

// TakeDirSnapshots take a snapshot of a directory `dir` at a given git `ref`.
func (r *Repository) TakeDirSnapshot(ref, targetDir string) (string, error) {
	snapshotPath := filepath.Join(r.snapshotsDir, ref)

	hash, err := r.fetchUnknownRef(ref)
	if err != nil {
		return "", fmt.Errorf("failed to fetch target revision: %w", err)
	}

	err = r.createDirSnapshot(*hash, targetDir, snapshotPath)
	if err != nil {
		os.RemoveAll(snapshotPath)
		return "", fmt.Errorf("failed to create directory snapshot: %w", err)
	}
	return snapshotPath, nil
}

func (r *Repository) listChangedFiles(base, head string) ([]Change, error) {
	var changes []Change
	headCommit, err := r.repo.CommitObject(plumbing.NewHash(head))
	if err != nil {
		return changes, err
	}
	baseCommit, err := r.repo.CommitObject(plumbing.NewHash(base))
	if err != nil {
		return changes, err
	}

	headTree, err := headCommit.Tree()
	if err != nil {
		return changes, err
	}
	baseTree, err := baseCommit.Tree()
	if err != nil {
		return changes, err
	}

	diff, err := baseTree.Diff(headTree)
	if err != nil {
		return changes, err
	}

	for _, change := range diff {
		changes = append(changes, Change{From: change.From.Name, To: change.To.Name})
	}
	return changes, nil
}

func (r *Repository) fetchRefSpecs(refSpecs []config.RefSpec) error {
	httpAuth, err := r.auth.GetBasicHTTPAuth()
	if err != nil {
		return fmt.Errorf("failed to get HTTP auth: %w", err)
	}
	err = r.repo.Fetch(&git.FetchOptions{
		Auth:       httpAuth,
		RemoteName: "origin",
		Depth:      0,
		RefSpecs:   refSpecs,
	})
	if err == git.NoErrAlreadyUpToDate {
		return nil
	}
	return err
}

func (r *Repository) fetchUnknownRef(ref string) (*plumbing.Hash, error) {
	// Check locally first
	if hash, err := r.repo.ResolveRevision(plumbing.Revision(ref)); err == nil {
		return hash, nil
	}

	// try fetch the commit
	if err := r.fetchRefSpecs([]config.RefSpec{
		config.RefSpec(fmt.Sprintf("+%s:%s", ref, ref)),
	}); err == nil {
		return r.repo.ResolveRevision(plumbing.Revision(ref))
	}

	// as a last resort list remote and see if it has the ref we need
	remote, err := r.repo.Remote("origin")
	if err != nil {
		return nil, fmt.Errorf("failed to get remote: %w", err)
	}

	httpAuth, err := r.auth.GetBasicHTTPAuth()
	if err != nil {
		return nil, fmt.Errorf("failed to get HTTP auth: %w", err)
	}

	refs, err := remote.List(&git.ListOptions{Auth: httpAuth})
	if err != nil {
		return nil, err
	}

	var refSpecs []config.RefSpec
	for _, remoteRef := range refs {
		refName := remoteRef.Name().String()
		if !strings.HasSuffix(refName, "/"+ref) {
			continue
		}
		switch {
		case strings.HasPrefix(refName, "refs/heads/"):
			refSpecs = append(refSpecs, config.RefSpec(fmt.Sprintf("+%s:refs/remotes/origin/%s", refName, ref)))
		case strings.HasPrefix(refName, "refs/tags/"):
			refSpecs = append(refSpecs, config.RefSpec(fmt.Sprintf("+%s:%s", refName, refName)))
		}
		break
	}

	if len(refSpecs) == 0 {
		return nil, fmt.Errorf("ref %q not found in remote", ref)
	}

	// Not found in refs - try as commit SHA
	if err := r.repo.Fetch(&git.FetchOptions{
		Auth:       httpAuth,
		RemoteName: "origin",
		Depth:      0,
		RefSpecs:   refSpecs,
	}); err != nil {
		return nil, err
	}
	return r.repo.ResolveRevision(plumbing.Revision(ref))
}

// CreateSnapshotNew creates a snapshot of the repository at the given commit SHA and recursively extracts a repoDir with all it's content
func (r *Repository) createDirSnapshot(hash plumbing.Hash, repoDir, snapshotDir string) error {
	// Get the commit object
	commit, err := r.repo.CommitObject(hash)
	if err != nil {
		return fmt.Errorf("failed to get commit object: %w", err)
	}

	// Get the root tree
	rootTree, err := commit.Tree()
	if err != nil {
		return fmt.Errorf("failed to get tree for commit: %w", err)
	}

	// Navigate to the target directory if not root
	var targetTree *object.Tree
	if repoDir == "" || repoDir == "." || repoDir == "/" {
		targetTree = rootTree
	} else {
		// Use existing helper to navigate to the directory
		treeCache := make(map[string]*object.Tree)
		treeCache[""] = rootTree

		repoDir = strings.Trim(repoDir, "/")
		targetTree, err = r.getTreeForPath(rootTree, repoDir, treeCache)
		if err != nil {
			return fmt.Errorf("failed to get tree for directory %s: %w", repoDir, err)
		}
	}

	// Recursively extract all files from the target tree
	return r.extractTreeRecursively(targetTree, repoDir, snapshotDir)
}

// extractTreeRecursively extracts all files from a tree recursively
func (r *Repository) extractTreeRecursively(tree *object.Tree, currentPath, snapshotDir string) error {
	return tree.Files().ForEach(func(f *object.File) error {
		// Construct the relative path
		relativePath := f.Name
		if currentPath != "" && currentPath != "." {
			relativePath = filepath.Join(currentPath, f.Name)
		}

		// Construct the full target path
		targetPath := filepath.Join(snapshotDir, relativePath)

		// Create directory structure
		targetDir := filepath.Dir(targetPath)
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", targetDir, err)
		}

		// Create the file
		file, err := os.Create(targetPath)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", targetPath, err)
		}
		defer file.Close()

		// Get file content
		reader, err := f.Reader()
		if err != nil {
			return fmt.Errorf("failed to get file reader for %s: %w", relativePath, err)
		}
		defer reader.Close()

		// Copy content
		if _, err := io.Copy(file, reader); err != nil {
			return fmt.Errorf("failed to copy content for %s: %w", relativePath, err)
		}

		return nil
	})
}

// CreateSnapshotNew creates a snapshot of the repository at the given commit SHA end extracts all files specified in the argument
func (r *Repository) createFilesSnapshot(hash plumbing.Hash, files []string, snapshotDir string) error {
	commit, err := r.repo.CommitObject(hash)
	if err != nil {
		return fmt.Errorf("failed to get commit object: %w", err)
	}

	// Get the tree
	tree, err := commit.Tree()
	if err != nil {
		return fmt.Errorf("failed to get tree for commit: %w", err)
	}

	// Optimized extraction using shared path traversal and tree caching
	return r.extractFilesFromObjectTree(tree, files, snapshotDir)
}

// extractFilesFromObjectTree extracts multiple files efficiently by sharing tree traversals and caching
func (r *Repository) extractFilesFromObjectTree(rootTree *object.Tree, files []string, snapshotDir string) error {
	// Group files by their directory prefixes to optimize tree traversal
	pathGroups := make(map[string][]string)

	for _, filePath := range files {
		filePath = strings.Trim(filePath, "/")
		if filePath == "" {
			continue
		}

		dir := filepath.Dir(filePath)
		if dir == "." {
			dir = ""
		}
		pathGroups[dir] = append(pathGroups[dir], filePath)
	}

	// Tree cache to avoid repeated object decoding
	treeCache := make(map[string]*object.Tree)
	treeCache[""] = rootTree // Root tree

	// Extract files grouped by directory
	for dirPath, filePaths := range pathGroups {
		// Get or navigate to the directory tree
		tree, err := r.getTreeForPath(rootTree, dirPath, treeCache)
		if err != nil {
			return fmt.Errorf("failed to get tree for directory %s: %w", dirPath, err)
		}

		// Extract all files in this directory
		for _, fp := range filePaths {
			if err := r.extractFileFromTree(tree, fp, snapshotDir); err != nil {
				return err
			}
		}
	}

	return nil
}

// getTreeForPath navigates to a directory path and caches intermediate trees
func (r *Repository) getTreeForPath(rootTree *object.Tree, dirPath string, treeCache map[string]*object.Tree) (*object.Tree, error) {
	if dirPath == "" {
		return rootTree, nil
	}

	// Check cache first
	if tree, exists := treeCache[dirPath]; exists {
		return tree, nil
	}

	// Navigate to the path, caching intermediate trees
	parts := strings.Split(dirPath, "/")
	currentTree := rootTree
	currentPath := ""

	for _, part := range parts {
		if part == "" {
			continue
		}

		// Update current path
		if currentPath == "" {
			currentPath = part
		} else {
			currentPath = currentPath + "/" + part
		}

		// Check cache for this intermediate path
		if cachedTree, exists := treeCache[currentPath]; exists {
			currentTree = cachedTree
			continue
		}

		// Navigate to the next level
		entry, err := currentTree.FindEntry(part)
		if err != nil {
			return nil, fmt.Errorf("directory %s not found in path %s: %w", part, currentPath, err)
		}

		if entry.Mode.IsFile() {
			return nil, fmt.Errorf("path %s contains a file, not a directory", currentPath)
		}

		// Get the tree object
		obj, err := r.repo.Storer.EncodedObject(plumbing.TreeObject, entry.Hash)
		if err != nil {
			return nil, fmt.Errorf("failed to get tree object for %s: %w", currentPath, err)
		}

		currentTree, err = object.DecodeTree(r.repo.Storer, obj)
		if err != nil {
			return nil, fmt.Errorf("failed to decode tree for %s: %w", currentPath, err)
		}

		// Cache the tree
		treeCache[currentPath] = currentTree
	}

	return currentTree, nil
}

// extractFileFromTree extracts a single file from a tree that's already at the correct directory level
func (r *Repository) extractFileFromTree(tree *object.Tree, filePath, snapshotDir string) error {
	// Get the filename (last component of the path)
	fileName := filepath.Base(filePath)

	// Find the file entry in the tree
	entry, err := tree.FindEntry(fileName)
	if err != nil {
		return fmt.Errorf("file %s not found: %w", fileName, err)
	}

	if !entry.Mode.IsFile() {
		return fmt.Errorf("path %s is not a file", filePath)
	}

	// Create the full directory structure in the snapshot
	fullTargetPath := filepath.Join(snapshotDir, filePath)
	targetDir := filepath.Dir(fullTargetPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory %s: %w", targetDir, err)
	}

	// Extract the file
	return r.extractFile(entry, fullTargetPath)
}

// extractFile extracts a file content from the git TreeEntry and puts it into a targetPath
func (r *Repository) extractFile(entry *object.TreeEntry, targetPath string) error {
	// Get the blob object
	obj, err := r.repo.Storer.EncodedObject(plumbing.BlobObject, entry.Hash)
	if err != nil {
		return fmt.Errorf("failed to get blob object: %w", err)
	}

	// Create the target file
	file, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", targetPath, err)
	}
	defer file.Close()

	// Stream the content
	reader, err := obj.Reader()
	if err != nil {
		return fmt.Errorf("failed to get object reader: %w", err)
	}
	defer reader.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("failed to copy blob content: %w", err)
	}

	// Set file permissions
	return os.Chmod(targetPath, os.FileMode(entry.Mode))
}
