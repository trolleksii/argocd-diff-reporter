package nats

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

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
