package bus

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func (b *Publisher) Publish(ctx context.Context, msg Message) error {
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

type Publisher struct {
	js jetstream.JetStream
}

type Message struct {
	Subject string
	Headers map[string]string
	Data    []byte
}

func NewPublisher(js jetstream.JetStream) *Publisher {
	return &Publisher{js: js}
}
