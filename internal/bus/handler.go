package bus

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

type Handler interface {
	Handle(ctx context.Context, msg jetstream.Msg)
}

type HandlerFunc func(ctx context.Context, msg jetstream.Msg)

func (f HandlerFunc) Handle(ctx context.Context, msg jetstream.Msg) { f(ctx, msg) }
