package repository 

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/trolleksii/argocd-diff-reporter/internal/githubauth"
)

// Repository represents a git repository
type Repository struct {
	repo     *git.Repository
	auth     *githubauth.GithubCredManager
	cloneDir string
	url      string
	nameHash string
}

// NewRepository creates a new repository instance
func NewRepository(url, cloneDir string, auth *githubauth.GithubCredManager) (*Repository, error) {

	// Ensure clone directory exists
	if err := os.MkdirAll(cloneDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create clone directory: %w", err)
	}

	r := &Repository{
		url:      url,
		cloneDir: cloneDir,
		auth:     auth,
		nameHash: fmt.Sprintf("%x", sha256.Sum256([]byte(url))),
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
	return r, nil
}

func (r *Repository) fetchCommitSHA(commitSHA string) (*plumbing.Hash, error) {
	hash, err := r.repo.ResolveRevision(plumbing.Revision(commitSHA))
	if err == nil {
		return hash, nil
	}

	httpAuth, err := r.auth.GetBasicHTTPAuth()
	if err != nil {
		return nil, fmt.Errorf("failed to get HTTP auth: %w", err)
	}

	err = r.repo.Fetch(&git.FetchOptions{
		Auth:       httpAuth,
		RemoteName: "origin",
		Depth:      0,
		RefSpecs: []config.RefSpec{
			config.RefSpec(fmt.Sprintf("+%s:%s", commitSHA, commitSHA)),
		},
	})
	if err != nil {
		return nil, err
	}
	return r.repo.ResolveRevision(plumbing.Revision(commitSHA))
}

func (r *Repository) fetchUnknownRef(ref string) (*plumbing.Hash, error) {
	// Check locally first
	hash, err := r.repo.ResolveRevision(plumbing.Revision(ref))
	if err == nil {
		return hash, nil
	}

	httpAuth, err := r.auth.GetBasicHTTPAuth()
	if err != nil {
		return nil, fmt.Errorf("failed to get HTTP auth: %w", err)
	}

	// Get remote references
	remote, _ := r.repo.Remote("origin")
	refs, err := remote.List(&git.ListOptions{
		Auth: httpAuth,
	})
	if err != nil {
		return nil, err
	}

	var refSpecs []config.RefSpec
	for _, remoteRef := range refs {
		refName := remoteRef.Name().String()

		if strings.HasSuffix(refName, "/"+ref) {
			// Found it - determine RefSpec and fetch
			if strings.HasPrefix(refName, "refs/heads/") {
				refSpecs = append(refSpecs, config.RefSpec(fmt.Sprintf("+%s:refs/remotes/origin/%s", refName, ref)))
			} else if strings.HasPrefix(refName, "refs/tags/") {
				refSpecs = append(refSpecs, config.RefSpec(fmt.Sprintf("+%s:%s", refName, refName)))
			}
			break
		}
	}

	// Not found in refs - try as commit SHA
	refSpecs = append(refSpecs, config.RefSpec(fmt.Sprintf("+%s:%s", ref, ref)))
	err = r.repo.Fetch(&git.FetchOptions{
		Auth:       httpAuth,
		RemoteName: "origin",
		Depth:      0,
		RefSpecs:   refSpecs,
	})
	if err != nil {
		return nil, err
	}
	return r.repo.ResolveRevision(plumbing.Revision(ref))
}

func (r *Repository) FetchBranch(branch string) error {
	httpAuth, err := r.auth.GetBasicHTTPAuth()
	if err != nil {
		return fmt.Errorf("failed to get HTTP auth: %w", err)
	}
	err = r.repo.Fetch(&git.FetchOptions{
		RemoteName: "origin",
		Auth:       httpAuth,
		RefSpecs: []config.RefSpec{
			"+refs/heads/main:refs/remotes/origin/main",
			config.RefSpec(fmt.Sprintf("+refs/heads/%s:refs/remotes/origin/%s", branch, branch)),
		},
	})
	if err == git.NoErrAlreadyUpToDate {
		return nil
	}
	return err
}
func (r *Repository) TakeFilesSnapshot(snapshotBase, commitSHA string, files []string) (string, error) {
	cacheKey := r.buildCacheKey(commitSHA, files)
	snapshotDir := filepath.Join(snapshotBase, r.nameHash, cacheKey)

	hash, err := r.fetchCommitSHA(commitSHA)
	if err != nil {
		return "", fmt.Errorf("failed to fetch target revision: %w", err)
	}
	err = r.createFilesSnapshot(*hash, files, snapshotDir)

	if err != nil {
		os.RemoveAll(snapshotDir)
		return "", fmt.Errorf("failed to resolve target revision: %w", err)
	}
	return snapshotDir, nil
}

func (r *Repository) TakeDirSnapshot(ctx context.Context, snapshotBase, ref, repoDir string) (string, error) {
	snapshotDir := filepath.Join(snapshotBase, r.nameHash, ref)

	hash, err := r.fetchUnknownRef(ref)
	if err != nil {
		return "", fmt.Errorf("failed to fetch target revision: %w", err)
	}
	err = r.createDirSnapshot(*hash, repoDir, snapshotDir)

	if err != nil {
		os.RemoveAll(snapshotDir)
		return "", fmt.Errorf("failed to resolve target revision: %w", err)
	}
	return snapshotDir, nil
}

// buildCacheKey returns a unique cache key per ref/[]files combination
func (r *Repository) buildCacheKey(ref string, files []string) string {
	sorted := make([]string, len(files))
	copy(sorted, files)
	sort.Strings(sorted)

	filesHash := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(sorted, ","))))
	return fmt.Sprintf("%s/%s", ref, filesHash)
}

// CreateSnapshotNew creates a snapshot of the repository at the given commit SHA and recursively extracts a repoDir with all it's content
func (r *Repository) createDirSnapshot(hash plumbing.Hash, repoDir, snapshotDir string) error {
	// Create the snapshot directory first (outside the lock)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

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
	// Create the snapshot directory first (outside the lock)
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create snapshot directory: %w", err)
	}

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
