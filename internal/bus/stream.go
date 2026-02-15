package bus

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

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
