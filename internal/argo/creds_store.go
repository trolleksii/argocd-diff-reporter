package argo

import (
	"context"
	"log/slog"
	"strings"
	"sync"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

// CredsLoader fetches credentials for a repo from the source of truth,
// scoped by ArgoCD project.
type CredsLoader func(ctx context.Context, repoURL, project string) (models.Creds, error)

// CredsStore is a read-through credentials cache keyed by ArgoCD project and
// repo URL. Get serves cached entries and falls back to the loader on miss;
// Refresh forces a reload when cached credentials turn out to be invalid
// (e.g. expired TTL tokens).
type CredsStore struct {
	mu     sync.RWMutex
	creds  map[string]models.Creds
	loader CredsLoader
}

// NewCredsStore creates a store backed by the given loader. A nil loader
// yields empty (anonymous) credentials for every repo.
func NewCredsStore(loader CredsLoader) *CredsStore {
	if loader == nil {
		slog.Debug("no creds loader configured, helm fetch will use anonymous access")
	}
	return &CredsStore{
		creds:  make(map[string]models.Creds),
		loader: loader,
	}
}

// Scoped binds an ArgoCD project and returns a project-agnostic view of the
// store; its Get/Refresh satisfy helm.CredsProvider.
func (s *CredsStore) Scoped(project string) ScopedCreds {
	return ScopedCreds{store: s, project: project}
}

func (s *CredsStore) get(ctx context.Context, repoURL, project string) (models.Creds, error) {
	if s.loader == nil {
		return models.Creds{}, nil
	}
	key := credsKey(repoURL, project)
	s.mu.RLock()
	c, ok := s.creds[key]
	s.mu.RUnlock()
	if ok {
		return c, nil
	}
	return s.refresh(ctx, repoURL, project)
}

func (s *CredsStore) refresh(ctx context.Context, repoURL, project string) (models.Creds, error) {
	if s.loader == nil {
		return models.Creds{}, nil
	}
	c, err := s.loader(ctx, repoURL, project)
	if err != nil {
		return models.Creds{}, err
	}
	s.mu.Lock()
	s.creds[credsKey(repoURL, project)] = c
	s.mu.Unlock()
	return c, nil
}

func credsKey(repoURL, project string) string {
	// oci repo url may have an optional oci:// prefix or may not, thus normalizing the key
	return project + "|" + strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(repoURL), "oci://"), "/")
}

// ScopedCreds is the store with an ArgoCD project bound in.
type ScopedCreds struct {
	store   *CredsStore
	project string
}

func (p ScopedCreds) Get(ctx context.Context, repoURL string) (models.Creds, error) {
	return p.store.get(ctx, repoURL, p.project)
}

func (p ScopedCreds) Refresh(ctx context.Context, repoURL string) (models.Creds, error) {
	return p.store.refresh(ctx, repoURL, p.project)
}
