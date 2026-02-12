package store

import (
	"context"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

func (s *Store) GetIndex(ctx context.Context) models.Index {
	return models.Index{}
}

func (s *Store) GetSummary(ctx context.Context, prNumber string) (models.PullRequest, error) {
	return models.PullRequest{}, nil
}

func (s *Store) GetReport(ctx context.Context, id string) (models.Report, error) {
	return models.Report{}, nil
}

type Store struct {}

func NewStore() *Store {
	return &Store{}
}
