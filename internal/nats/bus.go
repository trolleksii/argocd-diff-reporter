package nats

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

// Handler is the callback invoked for each consumed message.
// ack and nak are pre-bound to the underlying jetstream.Msg methods.
type Handler func(ctx context.Context, headers Headers, data []byte, ack, nak func() error)

type Headers map[string]string

func (c Headers) Get(key string) string { return c[key] }
func (c Headers) Set(key, val string)   { c[key] = val }
func (c Headers) Keys() []string        { return slices.Collect(maps.Keys(c)) }

// Route binds one or more subjects to a single handler.
type Route struct {
	Subjects []string
	Handler  Handler
}

// ConsumerConfig describes how to set up a JetStream pull consumer.
type ConsumerConfig struct {
	// Durable consumer name
	Name string
	// Routes maps subjects to handlers. Multiple subjects can share the same handler.
	Routes []Route
	// Max delivery attempts
	MaxDeliver int
	// Ack wait time
	AckWait time.Duration
	// Concurrency is the max number of handlers that may run in parallel.
	// 0 or 1 means serial (default behaviour).
	Concurrency int
}

// Bus provides both publish and consume capabilities over JetStream.
type Bus struct {
	js     jetstream.JetStream
	stream jetstream.Stream
}

func (b *Bus) Publish(ctx context.Context, subject string, headers Headers, data []byte) error {
	h := nats.Header{}
	for k, v := range headers {
		h[k] = []string{v}
	}
	msg := nats.Msg{
		Subject: subject,
		Header:  h,
	}
	if data != nil {
		msg.Data = data
	}
	_, err := b.js.PublishMsg(ctx, &msg)
	return err
}

func Marshal(t any) ([]byte, error) {
	return msgpack.Marshal(t)
}

func Unmarshal[T any](data []byte) (T, error) {
	var result T
	err := msgpack.Unmarshal(data, &result)
	return result, err
}

// Consume creates (or updates) a JetStream consumer and starts delivering
// messages to fn. It blocks until ctx is cancelled, then stops the consumer.
func (b *Bus) Consume(ctx context.Context, cfg ConsumerConfig) error {
	filterSubjects := make([]string, 0)
	handlers := make(map[string]Handler)
	for _, r := range cfg.Routes {
		filterSubjects = append(filterSubjects, r.Subjects...)
		for _, s := range r.Subjects {
			handlers[s] = r.Handler
		}
	}

	cons, err := b.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        cfg.Name,
		FilterSubjects: filterSubjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxDeliver:     cfg.MaxDeliver,
		AckWait:        cfg.AckWait,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
	})
	if err != nil {
		return fmt.Errorf("bus: create consumer %s: %w", cfg.Name, err)
	}

	var (
		sem chan struct{}
		wg  sync.WaitGroup
	)
	if cfg.Concurrency > 1 {
		sem = make(chan struct{}, cfg.Concurrency)
	}

	dispatch := func(msg jetstream.Msg) {
		handler, ok := handlers[msg.Subject()]
		if !ok {
			_ = msg.Nak()
			return
		}
		hdrs := msg.Headers()
		var headers Headers = make(map[string]string)
		if hdrs != nil {
			for k, vs := range hdrs {
				if len(vs) > 0 {
					headers[k] = vs[0]
				}
			}
		}
		if sem != nil {
			sem <- struct{}{} // blocks until a slot is free
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				handler(ctx, headers, msg.Data(), msg.Ack, msg.Nak)
			}()
		} else {
			handler(ctx, headers, msg.Data(), msg.Ack, msg.Nak)
		}
	}

	cc, err := cons.Consume(dispatch)
	if err != nil {
		return fmt.Errorf("bus: start consuming %s: %w", cfg.Name, err)
	}

	<-ctx.Done()
	cc.Stop()
	wg.Wait() // drain in-flight handlers before returning
	return nil
}

func (b *Bus) EnsureStream(ctx context.Context, name string, subjects []string) error {
	stream, err := b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	if err != nil {
		return fmt.Errorf("bus: create stream %s: %w", name, err)
	}
	b.stream = stream
	return nil
}
