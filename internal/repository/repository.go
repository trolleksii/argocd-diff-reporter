package repository

import (
	"context"
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

// Change represents a file change between two commits.
type Change struct {
	From string
	To   string
}

type requestKind int

const (
	kindSnapshot requestKind = iota
	kindNameDiff
)

type snapshotRequest struct {
	ref       string
	directory string
	files     []string
}

type diffRequest struct {
	base, head string
}

type result struct {
	snapshotDir string
	changes     []Change
	error       error
}

type request struct {
	kind     requestKind
	snapshot snapshotRequest
	diff     diffRequest
	result   chan result
}

// NewRepository creates a new Repository, reusing a local clone if one already exists.
func NewRepository(ctx context.Context, url, cloneRootDir, snapshotsRootDir string, auth *githubauth.GithubCredManager, log *slog.Logger) (*Repository, error) {
	id := fmt.Sprintf("%x", xxhash.Sum64String(url))
	cloneDir := filepath.Join(cloneRootDir, id)
	snapshotsDir := filepath.Join(snapshotsRootDir, id)

	if err := os.MkdirAll(cloneDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create clone directory: %w", err)
	}

	r := &Repository{
		cloneDir:     cloneDir,
		snapshotsDir: snapshotsDir,
		auth:         auth,
		queue:        make(chan request, 2),
		log:          log,
	}

	if repo, err := git.PlainOpen(cloneDir); err == nil {
		if remote, err := repo.Remote("origin"); err == nil {
			if cfg := remote.Config(); len(cfg.URLs) > 0 && cfg.URLs[0] == url {
				r.repo = repo
				go r.startQueuePoller(ctx)
				return r, nil
			}
		}
		os.RemoveAll(cloneDir)
		os.MkdirAll(cloneDir, 0755)
	}

	httpAuth, err := r.auth.GetBasicHTTPAuth()
	if err != nil {
		return nil, fmt.Errorf("failed to get HTTP auth: %w", err)
	}

	repo, err := git.PlainClone(cloneDir, false, &git.CloneOptions{Auth: httpAuth, URL: url})
	if err != nil {
		return nil, fmt.Errorf("failed to clone repository: %w", err)
	}
	r.repo = repo
	go r.startQueuePoller(ctx)
	return r, nil
}

func (r *Repository) startQueuePoller(ctx context.Context) {
	for {
		select {
		case req := <-r.queue:
			switch req.kind {
			case kindNameDiff:
				changes, err := r.fetchAndListChangedFiles(req.diff.base, req.diff.head) 
				if err != nil {
					r.log.Error("failed to list changed files", "error", err)
				}
				req.result <- result{changes: changes, error: err}
				close(req.result)
			case kindSnapshot:
				dir, err := r.getOrCreateSnapshot(req.snapshot.ref, req.snapshot.directory, req.snapshot.files)
				if err != nil {
					r.log.Error("failed to create snapshot", "error", err)
				}
				req.result <- result{snapshotDir: dir, error: err}
				close(req.result)
			default:
				panic("unknown request kind")
			}
		case <-ctx.Done():
			return
		}
	}
}

// ListChangedFiles returns the files that changed between base and head commits.
// Requests are serialized through the internal queue to preserve order.
func (r *Repository) ListChangedFiles(base, head string) ([]Change, error) {
	if base == "" || head == "" {
		return nil, fmt.Errorf("both base and head sha must be set")
	}
	out := make(chan result, 1)
	r.queue <- request{
		kind:   kindNameDiff,
		diff:   diffRequest{base: base, head: head},
		result: out,
	}
	res := <-out
	return res.changes, res.error
}

// GetOrCreateSnapshot returns a local directory containing a snapshot of the
// given ref. Checks for a cached snapshot before going through the queue.
// repoDir and files are mutually exclusive: set one or the other.
func (r *Repository) GetOrCreateSnapshot(ref, repoDir string, files []string) (string, error) {
	if ref == "" {
		return "", fmt.Errorf("ref must be set")
	}
	if repoDir != "" && len(files) != 0 {
		return "", fmt.Errorf("repoDir and files are mutually exclusive")
	}
	if repoDir == "" && len(files) == 0 {
		return "", fmt.Errorf("either repoDir or files must be set")
	}

	// Fast path: return immediately if snapshot already exists on disk.
	if dir := r.computeSnapshotDir(ref, repoDir, files); r.snapshotExists(dir) {
		return dir, nil
	}

	out := make(chan result, 1)
	r.queue <- request{
		kind:     kindSnapshot,
		snapshot: snapshotRequest{ref: ref, directory: repoDir, files: files},
		result:   out,
	}
	res := <-out
	return res.snapshotDir, res.error
}

// computeSnapshotDir returns the deterministic local path for a snapshot.
func (r *Repository) computeSnapshotDir(ref, repoDir string, files []string) string {
	if repoDir != "" {
		fingerprint := fmt.Sprintf("%x", xxhash.Sum64String(repoDir))
		return filepath.Join(r.snapshotsDir, ref, fingerprint)
	}
	sorted := make([]string, len(files))
	copy(sorted, files)
	sort.Strings(sorted)
	fingerprint := fmt.Sprintf("%x", xxhash.Sum64String(strings.Join(sorted, ",")))
	return filepath.Join(r.snapshotsDir, ref, fingerprint)
}

func (r *Repository) snapshotExists(dir string) bool {
	_, err := os.Stat(dir)
	return err == nil
}

func (r *Repository) fetchAndListChangedFiles(base, head string) ([]Change, error) {
	if err := r.fetchRefSpecs([]config.RefSpec{
		config.RefSpec(fmt.Sprintf("%s:%s", base, base)),
		config.RefSpec(fmt.Sprintf("%s:%s", head, head)),
	}); err != nil {
		return nil, err
	}
	return r.listChangedFiles(base, head)
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

func (r *Repository) getOrCreateSnapshot(ref, repoDir string, files []string) (string, error) {
	snapshotDir := r.computeSnapshotDir(ref, repoDir, files)

	hash, err := r.fetchRef(ref)
	if err != nil {
		return "", fmt.Errorf("failed to fetch ref: %w", err)
	}

	if repoDir != "" {
		err = r.createDirSnapshot(*hash, repoDir, snapshotDir)
	} else {
		err = r.createFilesSnapshot(*hash, files, snapshotDir)
	}

	if err != nil {
		os.RemoveAll(snapshotDir)
		return "", fmt.Errorf("failed to create snapshot: %w", err)
	}
	return snapshotDir, nil
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

func (r *Repository) fetchRef(ref string) (*plumbing.Hash, error) {
	// Check locally first
	if hash, err := r.repo.ResolveRevision(plumbing.Revision(ref)); err == nil {
		return hash, nil
	}

	// Try a direct fetch
	if err := r.fetchRefSpecs([]config.RefSpec{
		config.RefSpec(fmt.Sprintf("+%s:%s", ref, ref)),
	}); err == nil {
		return r.repo.ResolveRevision(plumbing.Revision(ref))
	}

	// As a last resort, list remote refs and match by suffix
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

func (r *Repository) createDirSnapshot(hash plumbing.Hash, repoDir, snapshotDir string) error {
	commit, err := r.repo.CommitObject(hash)
	if err != nil {
		return fmt.Errorf("failed to get commit object: %w", err)
	}

	rootTree, err := commit.Tree()
	if err != nil {
		return fmt.Errorf("failed to get tree for commit: %w", err)
	}

	var targetTree *object.Tree
	if repoDir == "" || repoDir == "." || repoDir == "/" {
		targetTree = rootTree
	} else {
		treeCache := make(map[string]*object.Tree)
		treeCache[""] = rootTree
		repoDir = strings.Trim(repoDir, "/")
		targetTree, err = r.getTreeForPath(rootTree, repoDir, treeCache)
		if err != nil {
			return fmt.Errorf("failed to get tree for directory %s: %w", repoDir, err)
		}
	}

	return r.extractTreeRecursively(targetTree, repoDir, snapshotDir)
}

func (r *Repository) extractTreeRecursively(tree *object.Tree, currentPath, snapshotDir string) error {
	return tree.Files().ForEach(func(f *object.File) error {
		relativePath := f.Name
		if currentPath != "" && currentPath != "." {
			relativePath = filepath.Join(currentPath, f.Name)
		}

		targetPath := filepath.Join(snapshotDir, relativePath)
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		file, err := os.Create(targetPath)
		if err != nil {
			return fmt.Errorf("failed to create file %s: %w", targetPath, err)
		}
		defer file.Close()

		reader, err := f.Reader()
		if err != nil {
			return fmt.Errorf("failed to get file reader for %s: %w", relativePath, err)
		}
		defer reader.Close()

		if _, err := io.Copy(file, reader); err != nil {
			return fmt.Errorf("failed to copy content for %s: %w", relativePath, err)
		}
		return nil
	})
}

func (r *Repository) createFilesSnapshot(hash plumbing.Hash, files []string, snapshotDir string) error {
	commit, err := r.repo.CommitObject(hash)
	if err != nil {
		return fmt.Errorf("failed to get commit object: %w", err)
	}

	tree, err := commit.Tree()
	if err != nil {
		return fmt.Errorf("failed to get tree for commit: %w", err)
	}

	return r.extractFilesFromObjectTree(tree, files, snapshotDir)
}

func (r *Repository) extractFilesFromObjectTree(rootTree *object.Tree, files []string, snapshotDir string) error {
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

	treeCache := make(map[string]*object.Tree)
	treeCache[""] = rootTree

	for dirPath, filePaths := range pathGroups {
		tree, err := r.getTreeForPath(rootTree, dirPath, treeCache)
		if err != nil {
			return fmt.Errorf("failed to get tree for directory %s: %w", dirPath, err)
		}
		for _, fp := range filePaths {
			if err := r.extractFileFromTree(tree, fp, snapshotDir); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Repository) getTreeForPath(rootTree *object.Tree, dirPath string, treeCache map[string]*object.Tree) (*object.Tree, error) {
	if dirPath == "" {
		return rootTree, nil
	}
	if tree, exists := treeCache[dirPath]; exists {
		return tree, nil
	}

	parts := strings.Split(dirPath, "/")
	currentTree := rootTree
	currentPath := ""

	for _, part := range parts {
		if part == "" {
			continue
		}
		if currentPath == "" {
			currentPath = part
		} else {
			currentPath = currentPath + "/" + part
		}

		if cachedTree, exists := treeCache[currentPath]; exists {
			currentTree = cachedTree
			continue
		}

		entry, err := currentTree.FindEntry(part)
		if err != nil {
			return nil, fmt.Errorf("directory %s not found in path %s: %w", part, currentPath, err)
		}
		if entry.Mode.IsFile() {
			return nil, fmt.Errorf("path %s contains a file, not a directory", currentPath)
		}

		obj, err := r.repo.Storer.EncodedObject(plumbing.TreeObject, entry.Hash)
		if err != nil {
			return nil, fmt.Errorf("failed to get tree object for %s: %w", currentPath, err)
		}

		currentTree, err = object.DecodeTree(r.repo.Storer, obj)
		if err != nil {
			return nil, fmt.Errorf("failed to decode tree for %s: %w", currentPath, err)
		}
		treeCache[currentPath] = currentTree
	}
	return currentTree, nil
}

func (r *Repository) extractFileFromTree(tree *object.Tree, filePath, snapshotDir string) error {
	fileName := filepath.Base(filePath)
	entry, err := tree.FindEntry(fileName)
	if err != nil {
		return fmt.Errorf("file %s not found: %w", fileName, err)
	}
	if !entry.Mode.IsFile() {
		return fmt.Errorf("path %s is not a file", filePath)
	}

	fullTargetPath := filepath.Join(snapshotDir, filePath)
	if err := os.MkdirAll(filepath.Dir(fullTargetPath), 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}
	return r.extractFile(entry, fullTargetPath)
}

func (r *Repository) extractFile(entry *object.TreeEntry, targetPath string) error {
	obj, err := r.repo.Storer.EncodedObject(plumbing.BlobObject, entry.Hash)
	if err != nil {
		return fmt.Errorf("failed to get blob object: %w", err)
	}

	file, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", targetPath, err)
	}
	defer file.Close()

	reader, err := obj.Reader()
	if err != nil {
		return fmt.Errorf("failed to get object reader: %w", err)
	}
	defer reader.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("failed to copy blob content: %w", err)
	}
	return os.Chmod(targetPath, os.FileMode(entry.Mode))
}
