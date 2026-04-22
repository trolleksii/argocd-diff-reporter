package nats_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

type received struct {
	Headers internalnats.Headers
	Data    []byte
}

func TestBus_PublishConsumeAck(t *testing.T) {
	bus, _, _ := testutil.StartNATS(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Create a test stream.
	err := bus.EnsureStream(ctx, "TEST-STREAM", []string{"test.subject"})
	require.NoError(t, err, "EnsureStream should succeed")

	// 2. Set up handler plumbing.
	var callCount atomic.Int64
	msgCh := make(chan received, 1)

	handler := func(_ context.Context, headers internalnats.Headers, data []byte, ack, _ func() error) {
		callCount.Add(1)
		msgCh <- received{Headers: headers, Data: data}
		_ = ack()
	}

	cfg := internalnats.ConsumerConfig{
		Name: "test-consumer",
		Routes: []internalnats.Route{
			{
				Subjects: []string{"test.subject"},
				Handler:  handler,
			},
		},
		MaxDeliver:  3,
		AckWait:     5 * time.Second,
		Concurrency: 1,
	}

	// 3. Start consumer in background goroutine (blocks until ctx cancelled).
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- bus.Consume(ctx, cfg)
	}()

	// Give the consumer a moment to register with JetStream.
	time.Sleep(100 * time.Millisecond)

	// 4. Publish a message.
	payload := "hello-world"
	data, err := internalnats.Marshal(payload)
	require.NoError(t, err, "Marshal should succeed")

	err = bus.Publish(ctx, "test.subject", internalnats.Headers{"X-Test": "hello"}, data)
	require.NoError(t, err, "Publish should succeed")

	// 5. Wait for the handler to receive the message.
	select {
	case msg := <-msgCh:
		assert.Equal(t, "hello", msg.Headers.Get("X-Test"), "header X-Test should match")

		got, err := internalnats.Unmarshal[string](msg.Data)
		require.NoError(t, err, "Unmarshal should succeed")
		assert.Equal(t, payload, got, "payload should round-trip")

	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for handler to receive message")
	}

	// 6. Verify no redelivery after ack.
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int64(1), callCount.Load(), "handler should be called exactly once (no redelivery)")

	// 7. Stop consumer.
	cancel()
	select {
	case err := <-consumeErr:
		assert.NoError(t, err, "Consume should return nil after context cancellation")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Consume to return")
	}
}

func TestBus_NakRedelivery(t *testing.T) {
	bus, _, _ := testutil.StartNATS(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Create a dedicated stream for this test.
	err := bus.EnsureStream(ctx, "TEST-NAK-STREAM", []string{"test.nak.subject"})
	require.NoError(t, err, "EnsureStream should succeed")

	// 2. Set up handler plumbing.
	var callCount atomic.Int64
	dataCh := make(chan []byte, 2)

	handler := func(_ context.Context, _ internalnats.Headers, data []byte, ack, nak func() error) {
		n := callCount.Add(1)
		dataCh <- data
		if n == 1 {
			_ = nak()
		} else {
			_ = ack()
		}
	}

	cfg := internalnats.ConsumerConfig{
		Name: "test-nak-consumer",
		Routes: []internalnats.Route{
			{
				Subjects: []string{"test.nak.subject"},
				Handler:  handler,
			},
		},
		MaxDeliver:  3,
		AckWait:     1 * time.Second,
		Concurrency: 1,
	}

	// 3. Start consumer in background goroutine (blocks until ctx cancelled).
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- bus.Consume(ctx, cfg)
	}()

	// Give the consumer a moment to register with JetStream.
	time.Sleep(100 * time.Millisecond)

	// 4. Publish a message.
	payload := "nak-test-payload"
	data, err := internalnats.Marshal(payload)
	require.NoError(t, err, "Marshal should succeed")

	err = bus.Publish(ctx, "test.nak.subject", nil, data)
	require.NoError(t, err, "Publish should succeed")

	// 5. Read from channel twice — first delivery (nak'd) and second delivery (ack'd).
	for i := 0; i < 2; i++ {
		select {
		case got := <-dataCh:
			assert.Equal(t, data, got, "delivery %d payload should match published data", i+1)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for delivery %d", i+1)
		}
	}

	// 6. Verify handler was called exactly twice (one nak + one ack, no further redelivery).
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int64(2), callCount.Load(), "handler should be called exactly twice")

	// 7. Stop consumer.
	cancel()
	select {
	case err := <-consumeErr:
		assert.NoError(t, err, "Consume should return nil after context cancellation")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Consume to return")
	}
}

type kvTestData struct {
	Name  string
	Count int
}

func TestStore_KVPutGet(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	ctx := context.Background()

	// 1. Set a value and verify no error.
	original := kvTestData{Name: "hello", Count: 42}
	err := store.SetValue(ctx, "test-key", original)
	require.NoError(t, err, "SetValue should succeed")

	// 2. Get the value back and verify it round-trips.
	got, err := internalnats.GetValue[kvTestData](ctx, store, "test-key")
	require.NoError(t, err, "GetValue should succeed for existing key")
	assert.Equal(t, original, got, "retrieved value should equal the original")

	// 3. Get a nonexistent key and verify an error is returned.
	_, err = internalnats.GetValue[kvTestData](ctx, store, "nonexistent-key")
	assert.Error(t, err, "GetValue should return an error for a nonexistent key")
}

func TestStore_ObjectPutGet(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	ctx := context.Background()

	// 1. Store an object and verify no error.
	original := kvTestData{Name: "compressed", Count: 99}
	err := store.StoreObject(ctx, "test-object", original)
	require.NoError(t, err, "StoreObject should succeed")

	// 2. Get the object back and verify it round-trips.
	got, err := internalnats.GetObject[kvTestData](ctx, store, "test-object")
	require.NoError(t, err, "GetObject should succeed for existing key")
	assert.Equal(t, original, got, "retrieved object should equal the original")

	// 3. Get a nonexistent object and verify an error is returned.
	_, err = internalnats.GetObject[kvTestData](ctx, store, "nonexistent-object")
	assert.Error(t, err, "GetObject should return an error for a nonexistent key")
}
