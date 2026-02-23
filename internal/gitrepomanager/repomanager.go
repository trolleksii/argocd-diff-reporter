package gitrepomanager

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/trolleksii/argocd-diff-reporter/internal/bus"
	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/githubauth"
	"github.com/trolleksii/argocd-diff-reporter/internal/repository"
)

const snapshotCompletedSubject = "repo.snapshot.completed"

type GitRepoManager struct {
	cfg  config.GitWorkerConfig
	auth *githubauth.GithubCredManager
	log  *slog.Logger
	bus  *bus.Bus

	mu    sync.RWMutex
	repos map[string]*repository.Repository
}

func NewGitRepoManager(cfg config.GitWorkerConfig, auth *githubauth.GithubCredManager, b *bus.Bus, log *slog.Logger) *GitRepoManager {
	return &GitRepoManager{
		cfg:   cfg,
		auth:  auth,
		log:   log.With("component", "gitrepomanager"),
		bus:   b,
		repos: make(map[string]*repository.Repository),
	}
}

// Run starts consuming snapshot requests.
// Blocks until ctx is cancelled, then shuts down all repos.
func (m *GitRepoManager) Run(ctx context.Context) error {
	err := m.bus.Consume(ctx, bus.ConsumerConfig{
		StreamName:   m.cfg.StreamName,
		ConsumerName: m.cfg.ConsumerName,
		Subject:      m.cfg.Subject,
	}, m.process)
	if err != nil {
		return fmt.Errorf("gitrepomanager: consume: %w", err)
	}
	return nil
}

// process handles one incoming snapshot request.
func (m *GitRepoManager) process(ctx context.Context, msg bus.Message, ack, nak func() error) {
	repoURL := msg.Headers["repo-url"]
	sha := msg.Headers["sha"]
	snapshotType := msg.Headers["snapshot-type"]
	repoDir := msg.Headers["repo-dir"]
	filesRaw := msg.Headers["files"]

	if repoURL == "" || sha == "" {
		m.log.Error("malformed snapshot request, missing required headers",
			"repo-url", repoURL, "sha", sha)
		// no nak — malformed messages should not be retried
		return
	}

	log := m.log.With("repo", repoURL, "sha", sha)

	nakf := func() {
		if err := nak(); err != nil {
			log.Error("failed to nak message", "error", err)
		}
	}

	var files []string
	if filesRaw != "" {
		files = strings.Split(filesRaw, ",")
	}

	entry, err := m.getOrCreateRepo(ctx, repoURL)
	if err != nil {
		log.Error("failed to initialize repo", "error", err)
		nakf()
		return
	}

	snapshotDir, err := entry.repo.GetOrCreateSnapshot(sha, repoDir, files)
	if err != nil {
		log.Error("snapshot failed", "error", err)
		nakf()
		return
	}

	log.Info("snapshot created", "dir", snapshotDir)

	if err := m.bus.Publish(ctx, bus.Message{
		Subject: snapshotCompletedSubject,
		Headers: map[string]string{
			"repo-url":      repoURL,
			"sha":           sha,
			"snapshot-dir":  snapshotDir,
			"snapshot-type": snapshotType,
		},
	}); err != nil {
		log.Error("failed to publish snapshot result", "error", err)
		nakf()
		return
	}

	if err := ack(); err != nil {
		log.Error("failed to ack message", "error", err)
	}
}

// getOrCreateRepo returns the entry for a repo URL, initializing it on first access.
func (m *GitRepoManager) getOrCreateRepo(ctx context.Context, repoURL string) (*repository.Repository, error) {
	m.mu.RLock()
	repo, ok := m.repos[repoURL]
	m.mu.RUnlock()
	if ok {
		return repo, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if repo, ok = m.repos[repoURL]; ok {
		return repo, nil
	}

	repo, err := repository.NewRepository(
		ctx,
		repoURL,
		m.cfg.CloneBaseDir,
		m.cfg.SnapshotBaseDir,
		m.auth,
		m.log.With("repo", repoURL),
	)
	if err != nil {
		return nil, fmt.Errorf("init repo %s: %w", repoURL, err)
	}

	m.repos[repoURL] = repo
	return repo, nil
}
