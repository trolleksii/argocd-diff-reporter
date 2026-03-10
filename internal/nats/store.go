package nats

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

func (s *Store) GetIndex(ctx context.Context) models.Index {
	i, err := GetValue[models.Index](ctx, s, "index")
	if err != nil {
		i = models.Index{}
	}
	return i
}

func (s *Store) PutIndex(ctx context.Context, index models.Index) error {
	return s.SetValue(ctx, "index", index)
}

func (s *Store) GetSummary(ctx context.Context, id string) (models.PullRequest, error) {
	return GetValue[models.PullRequest](ctx, s, id)
}

func (s *Store) PutSummary(ctx context.Context, id string, pr models.PullRequest) error {
	return s.SetValue(ctx, id, pr)
}

func (s *Store) GetReport(ctx context.Context, id string) (models.Report, error) {
	return GetObject[models.Report](ctx, s, id)
}

func (s *Store) PutReport(ctx context.Context, id string, report models.Report) error {
	return s.StoreObject(ctx, id, report)
}

func (s *Store) SetValue(ctx context.Context, key string, value any) error {
	data, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	_, err = s.kvStore.Put(ctx, key, data)
	return err
}

func GetValue[T any](ctx context.Context, s *Store, key string) (T, error) {
	var zero T
	entry, err := s.kvStore.Get(ctx, key)
	if err != nil {
		return zero, err
	}
	var result T
	if err := msgpack.Unmarshal(entry.Value(), &result); err != nil {
		return zero, err
	}
	return result, nil
}

func (s *Store) StoreObject(ctx context.Context, key string, value any) error {
	data, err := msgpack.Marshal(value)
	if err != nil {
		return err
	}
	_, err = s.objStore.PutBytes(ctx, key, data)
	return err
}

func GetObject[T any](ctx context.Context, s *Store, key string) (T, error) {
	var zero T
	entry, err := s.objStore.GetBytes(ctx, key)
	if err != nil {
		return zero, err
	}
	var result T
	if err := msgpack.Unmarshal(entry, &result); err != nil {
		return zero, err
	}
	return result, nil
}

type Store struct {
	kvStore  jetstream.KeyValue
	objStore jetstream.ObjectStore
}
