package bus

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type consumerConfig struct {
	maxDeliver int
	ackWait    time.Duration
}

type ConsumerOpt func(*consumerConfig)

func WithMaxDeliver(n int) ConsumerOpt {
	return func(c *consumerConfig) { c.maxDeliver = n }
}

func WithAckWait(d time.Duration) ConsumerOpt {
	return func(c *consumerConfig) { c.ackWait = d }
}

func Subscribe(js jetstream.JetStream, log *slog.Logger, stream string, name string, subjects []string, handler Handler, opts ...ConsumerOpt) *Subscriber {
	cfg := consumerConfig{
		maxDeliver: 3,
		ackWait:    30 * time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}

	return &Subscriber{
		js:   js,
		log:  log.With("component", "worker", "consumer", name),
		stream: stream,
		name: name,
		config: jetstream.ConsumerConfig{
			Durable:        name,
			AckPolicy:      jetstream.AckExplicitPolicy,
			MaxDeliver:     cfg.maxDeliver,
			AckWait:        cfg.ackWait,
			FilterSubjects: subjects,
		},
		handler: handler,
	}
}

type Handler interface {
	Handle(ctx context.Context, msg jetstream.Msg)
}

type HandlerFunc func(ctx context.Context, msg jetstream.Msg)

func (f HandlerFunc) Handle(ctx context.Context, msg jetstream.Msg) { f(ctx, msg) }

type Subscriber struct {
	js      jetstream.JetStream
	log     *slog.Logger
	stream  string
	name    string
	config  jetstream.ConsumerConfig
	handler Handler
}

func (s *Subscriber) Run(ctx context.Context) error {
	stream, err := s.js.Stream(ctx, s.stream)
	if err != nil {
		return fmt.Errorf("worker %s: get stream %s: %w", s.name, s.stream, err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, s.config)
	if err != nil {
		return fmt.Errorf("worker %s: create consumer: %w", s.name, err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		s.handler.Handle(ctx, msg)
	})
	if err != nil {
		return fmt.Errorf("worker %s: consume: %w", s.name, err)
	}

	s.log.Info("started", "subjects", s.config.FilterSubjects)
	<-ctx.Done()
	cc.Stop()
	s.log.Info("stopped")
	return nil
}

func EnsureStream(ctx context.Context, js jetstream.JetStream, name string, subjects []string) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	if err != nil {
		return fmt.Errorf("bus: create stream %s: %w", name, err)
	}
	return err
}
