package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

// Handler is the callback invoked for each consumed message.
// ack and nak are pre-bound to the underlying jetstream.Msg methods.
type Handler func(ctx context.Context, headers map[string]string, data []byte, ack, nak func() error)

// ConsumerConfig describes how to set up a JetStream pull consumer.
type ConsumerConfig struct {
	// Durable consumer name
	Name string
	// Handlers maps each subject to its handler.
	// The subjects are used as FilterSubjects on the JetStream consumer.
	Handlers map[string]Handler
	// Max delivery attempts
	MaxDeliver int
	// Ack wait time
	AckWait time.Duration
}

// Bus provides both publish and consume capabilities over JetStream.
type Bus struct {
	js     jetstream.JetStream
	stream jetstream.Stream
}

func (b *Bus) Publish(ctx context.Context, subject string, headers map[string]string, data []byte) error {
	h := nats.Header{}
	for k, v := range headers {
		h[k] = []string{v}
	}
	_, err := b.js.PublishMsg(ctx, &nats.Msg{
		Subject: subject,
		Header:  h,
		Data:    data,
	})
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
	subjects := make([]string, 0, len(cfg.Handlers))
	for s := range cfg.Handlers {
		subjects = append(subjects, s)
	}

	cons, err := b.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        cfg.Name,
		FilterSubjects: subjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxDeliver:     cfg.MaxDeliver,
		AckWait:        cfg.AckWait,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
	})
	if err != nil {
		return fmt.Errorf("bus: create consumer %s: %w", cfg.Name, err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		handler, ok := cfg.Handlers[msg.Subject()]
		if !ok {
			_ = msg.Nak()
			return
		}
		hdrs := msg.Headers()
		headers := make(map[string]string)
		if hdrs != nil {
			for k, vs := range hdrs {
				if len(vs) > 0 {
					headers[k] = vs[0]
				}
			}
		}
		handler(ctx, headers, msg.Data(), msg.Ack, msg.Nak)
	})
	if err != nil {
		return fmt.Errorf("bus: start consuming %s: %w", cfg.Name, err)
	}

	<-ctx.Done()
	cc.Stop()
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
