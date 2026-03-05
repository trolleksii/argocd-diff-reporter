package gitrepomanager

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/trolleksii/argocd-diff-reporter/internal/bus"
	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/githubauth"
	"github.com/trolleksii/argocd-diff-reporter/internal/repository"
)

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
		Name:       "gitrepomanager",
		MaxDeliver: 3,
		AckWait:    3 * time.Second,
		Handlers: map[string]bus.Handler{
			"webhook.pr.changed": m.handlePRChanged,
			"git.files.resolved": m.handleFilesResolved,
		},
	})
	if err != nil {
		return fmt.Errorf("gitrepomanager: consume: %w", err)
	}
	return nil
}

func try(log *slog.Logger, msg string, fn func() error) {
	if err := fn(); err != nil {
		log.Error(msg, "error", err)
	}
}

func (m *GitRepoManager) handlePRChanged(ctx context.Context, headers map[string]string, data []byte, ack, nak func() error) {
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", headers["owner"], headers["repository"])
	r, err := m.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		m.log.Error("failed to find git repo", "error", err)
		try(m.log, "failed to nak the message", nak)
		return
	}
	changes, err := r.ListChangedFiles(headers["baseSha"], headers["headSha"])
	if err != nil {
		m.log.Error("failed to list changed files")
		try(m.log, "failed to nak the message", nak)
		return
	}
	var from, to []string
	for _, change := range changes {
		from = append(from, change.From)
		to = append(to, change.To)
	}
	if len(from) > 0 {
		headers["ref"] = headers["baseSha"]
		data, err := bus.Marshal(FileGlobFilter(from, m.cfg.FileGlobs))
		if err != nil {
			m.log.Error("failed to marshal base files", "error", err)
			try(m.log, "failed to nak the message", nak)
			return
		}
		m.bus.Publish(ctx, "git.files.resolved", headers, data)
	}
	if len(to) > 0 {
		headers["ref"] = headers["headSha"]
		data, err = bus.Marshal(FileGlobFilter(to, m.cfg.FileGlobs))
		if err != nil {
			m.log.Error("failed to marshal head files", "error", err)
			try(m.log, "failed to nak the message", nak)
			return
		}
		m.bus.Publish(ctx, "git.files.resolved", headers, data)
	}
	try(m.log, "failed to ack message", ack)
}

func (m *GitRepoManager) handleFilesResolved(ctx context.Context, headers map[string]string, data []byte, ack, nak func() error) {
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", headers["owner"], headers["repository"])
	r, err := m.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		m.log.Error("failed to find git repo", "error", err)
		try(m.log, "failed to nak the message", nak)
		return
	}
	files, err := bus.Unmarshal[[]string](data)
	snapshotDir, err := r.GetOrCreateSnapshot(headers["ref"], "", files)
	if err != nil {
		m.log.Error("failed to create snapshot", "error", err)
	}
	headers["snapshotDir"] = snapshotDir
	m.bus.Publish(ctx, "git.files.snapshotted", headers, data)
	try(m.log, "failed to ack message", ack)
}

func (m *GitRepoManager) handleChartFetch(ctx context.Context, headers map[string]string, data []byte, ack, nak func() error) {
	repoUrl := fmt.Sprintf("https://github.com/%s/%s", headers["owner"], headers["repository"])
	r, err := m.getOrCreateRepo(ctx, repoUrl)
	if err != nil {
		m.log.Error("failed to find git repo", "error", err)
		try(m.log, "failed to nak the message", nak)
		return
	}
	snapshotDir, err := r.GetOrCreateSnapshot(headers["ref"], headers["path"], nil)
	if err != nil {
		m.log.Error("failed to create snapshot", "error", err)
	}
	headers["snapshotDir"] = snapshotDir
	m.bus.Publish(ctx, "git.chart.snapshotted", headers, data)
	try(m.log, "failed to ack message", ack)
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

func FileGlobFilter(files []string, globs []string) []string {
	var result []string
	for _, f := range files {
		for _, g := range globs {
			res, err := filepath.Match(g, f)
			if res && err == nil {
				result = append(result, f)
				break
			}
		}
	}
	return result
}
