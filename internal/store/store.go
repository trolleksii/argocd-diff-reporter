package store

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

func (s *Store) GetIndex(ctx context.Context) models.Index {
	return models.Index{}
}

func (s *Store) PutIndex(ctx context.Context, index models.Index) error {
	return nil
}

func (s *Store) GetSummary(ctx context.Context, prNumber string) (models.PullRequest, error) {
	return models.PullRequest{}, nil
}

func (s *Store) PutSummary(ctx context.Context, index models.PullRequest) error {
	return nil
}

func (s *Store) GetReport(ctx context.Context, id string) (models.Report, error) {
	return models.Report{}, nil
}

func (s *Store) PutReport(ctx context.Context, report models.Report) error {
	return nil
}

type Store struct {
	kvStore  jetstream.KeyValue
	objStore jetstream.ObjectStore
}

func NewStore(kv jetstream.KeyValue, obj jetstream.ObjectStore) *Store {
	return &Store{kvStore: kv, objStore: obj}
}
