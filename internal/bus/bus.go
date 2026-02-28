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
type Handler func(ctx context.Context, msg Message, ack, nak func() error)

// ConsumerConfig describes how to set up a JetStream pull consumer.
type ConsumerConfig struct {
	StreamName   string
	ConsumerName string
	Subject      string
}

// Message is the bus-level representation of a NATS message.
type Message struct {
	Subject string
	Headers map[string]string
	Data    any
}

// Bus provides both publish and consume capabilities over JetStream.
type Bus struct {
	js jetstream.JetStream
}

func NewBus(js jetstream.JetStream) *Bus {
	return &Bus{js: js}
}

func (b *Bus) Publish(ctx context.Context, msg Message) error {
	header := nats.Header{}
	for k, v := range msg.Headers {
		header[k] = []string{v}
	}
	m := &nats.Msg{
		Subject: msg.Subject,
		Header:  header,
	}
	if msg.Data != nil {
		data, err := msgpack.Marshal(msg.Data)
		if err != nil {
			return err
		}
		m.Data = data
	}

	_, err := b.js.PublishMsg(ctx, m)
	return err
}

// TODO: find better name
// WrapHandler provides means to unmarshal message data into concrete type retur
func WrapHandler[T any](ctx context.Context, fn Handler) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		hdrs := msg.Headers()
		var result T
		if err := msgpack.Unmarshal(msg.Data(), &result); err != nil {
			msg.Nak()
			return
		}
		m := Message{
			Subject: msg.Subject(),
			Data:    result,
		}
		if hdrs != nil {
			m.Headers = make(map[string]string, len(hdrs))
			for k, vs := range hdrs {
				if len(vs) > 0 {
					m.Headers[k] = vs[0]
				}
			}
		}
		fn(ctx, m, msg.Ack, msg.Nak)
	}
} 

// Consume creates (or updates) a JetStream consumer and starts delivering
// messages to fn. It blocks until ctx is cancelled, then stops the consumer.
func (b *Bus) Consume(ctx context.Context, cfg ConsumerConfig, fn jetstream.MessageHandler) error {
	cons, err := b.js.CreateOrUpdateConsumer(ctx, cfg.StreamName, jetstream.ConsumerConfig{
		Name:          cfg.ConsumerName,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return fmt.Errorf("bus: create consumer %s: %w", cfg.ConsumerName, err)
	}

	cc, err := cons.Consume(fn)
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
