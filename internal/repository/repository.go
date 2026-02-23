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
	SNAPSHOT_REQUEST requestKind = iota
	FETCH_REQUEST    
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
	for {
		select {
		case req := <-r.queue:
			switch req.Kind {
			case FETCH_REQUEST:
				if err := validateFetchRequest(req.Fetch); err != nil {
					r.log.Error("invalid fetch request", "error", err)
					req.Result <- Result{Error: err}
					close(req.Result)
					continue
				}
				changes, err := r.ListChangedFiles(req.Fetch.Base, req.Fetch.Head)
				if err != nil {
					r.log.Error("failed to pre-fetch branch", "error", err)
				}
				req.Result <- Result{
					Changes: changes,
					Error:   err,
				}
				close(req.Result)
			case SNAPSHOT_REQUEST:
				if err := validateSnapshotRequest(req.Snapshot); err != nil {
					r.log.Error("invalid snapshot request", "error", err)
					req.Result <- Result{Error: err}
					close(req.Result)
					continue
				}
				snapshotDir, err := r.GetOrCreateSnapshot(req.Snapshot.SHA, req.Snapshot.Directory, req.Snapshot.Files)
				req.Result <- Result{
					SnapshotDir: snapshotDir,
					Error:       err,
				}
				close(req.Result)
			default:
				panic("unknown git repository request kind")
			}
		case <-ctx.Done():
			return
		}
	}
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


func (r *Repository) ListChangedFiles(base, head string) ([]Change, error) {
	if err := r.fetchRefSpecs([]config.RefSpec{
		config.RefSpec(fmt.Sprintf("%s:%s", base, base)),
		config.RefSpec(fmt.Sprintf("%s:%s", head, head)),
	}); err != nil {
		return nil, err
	}
	return r.listChangedFiles(base, head)
}

func (r *Repository) GetOrCreateSnapshot(ref, repoDir string, files []string) (string, error) {
	var snapshotDir string
	var createFn func(hash plumbing.Hash) error

	if repoDir != "" {
		fingerprint := fmt.Sprintf("%x", xxhash.Sum64String(repoDir))
		snapshotDir = filepath.Join(r.snapshotsDir, ref, fingerprint)
		createFn = func(hash plumbing.Hash) error {
			return r.createDirSnapshot(hash, repoDir, snapshotDir)
		}
	} else {
		sort.Strings(files)
		fingerprint := fmt.Sprintf("%x", xxhash.Sum64String(strings.Join(files, ",")))
		snapshotDir = filepath.Join(r.snapshotsDir, ref, fingerprint)
		createFn = func(hash plumbing.Hash) error {
			return r.createFilesSnapshot(hash, files, snapshotDir)
		}
	}

	if _, err := os.Stat(snapshotDir); err == nil {
		return snapshotDir, nil
	}

	hash, err := r.fetchRef(ref)
	if err != nil {
		return "", fmt.Errorf("failed to fetch target revision: %w", err)
	}

	if err := createFn(*hash); err != nil {
		os.RemoveAll(snapshotDir)
		return "", fmt.Errorf("failed to create snapshot: %w", err)
	}
	return snapshotDir, nil
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
