package bus

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vmihailenco/msgpack/v5"
)

// Handler is the callback invoked for each consumed message.
// ack and nak are pre-bound to the underlying jetstream.Msg methods.
type Handler func(ctx context.Context, subject string, headers map[string]string, data []byte, ack, nak func() error)

// ConsumerConfig describes how to set up a JetStream pull consumer.
type ConsumerConfig struct {
	StreamName   string
	ConsumerName string
	Subject      string
}

// Bus provides both publish and consume capabilities over JetStream.
type Bus struct {
	js jetstream.JetStream
}

func NewBus(js jetstream.JetStream) *Bus {
	return &Bus{js: js}
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
func (b *Bus) Consume(ctx context.Context, cfg ConsumerConfig, fn Handler) error {
	cons, err := b.js.CreateOrUpdateConsumer(ctx, cfg.StreamName, jetstream.ConsumerConfig{
		Name:          cfg.ConsumerName,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("bus: create consumer %s: %w", cfg.ConsumerName, err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		hdrs := msg.Headers()
		headers := make(map[string]string)
		if hdrs != nil {
			for k, vs := range hdrs {
				if len(vs) > 0 {
					headers[k] = vs[0]
				}
			}
		}
		fn(ctx, msg.Subject(), headers, msg.Data(), msg.Ack, msg.Nak)
	})

	if err != nil {
		return fmt.Errorf("bus: start consuming %s: %w", cfg.ConsumerName, err)
	}

	<-ctx.Done()
	cc.Stop()
	return nil
}

func EnsureStream(ctx context.Context, js jetstream.JetStream, name string, subjects []string) (jetstream.Stream, error) {
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	if err != nil {
		return nil, fmt.Errorf("bus: create stream %s: %w", name, err)
	}
	return stream, nil
}
