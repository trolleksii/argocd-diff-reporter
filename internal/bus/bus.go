package bus

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
	Data    []byte
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
	if len(msg.Data) != 0 {
		m.Data = msg.Data
	}

	_, err := b.js.PublishMsg(ctx, m)
	return err
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

	cc, err := cons.Consume(func(jmsg jetstream.Msg) {
		hdrs := jmsg.Headers()
		msg := Message{
			Subject: jmsg.Subject(),
			Data:    jmsg.Data(),
		}
		if hdrs != nil {
			msg.Headers = make(map[string]string, len(hdrs))
			for k, vs := range hdrs {
				if len(vs) > 0 {
					msg.Headers[k] = vs[0]
				}
			}
		}
		fn(ctx, msg, jmsg.Ack, jmsg.Nak)
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
