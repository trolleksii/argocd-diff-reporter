package gitrepomanager

import (
	"context"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
)

type GitRepoManager struct{}

func NewGitRepoManager(cfg config.GitWorkerConfig) *GitRepoManager {
	return &GitRepoManager{}
}

func (r *GitRepoManager) Run(ctx context.Context) error {
	return nil
}
