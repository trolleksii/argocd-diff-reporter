package nats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/modules"
)

func SetupNats(cfg config.NatsConfig, ctx context.Context, r *modules.Registry) error {
	var nc *nats.Conn
	var err error
	if cfg.Addr != "" {
		nc, err = nats.Connect(cfg.Addr)
		if err != nil {
			return err
		}
	} else {
		opts := &server.Options{
			DontListen:      true,
			JetStream:       true,
			JetStreamDomain: cfg.Domain,
			ServerName:      cfg.ServerName,
			StoreDir:        cfg.StoreDir,
		}
		srv, err := server.NewServer(opts)
		if err != nil {
			return err
		}

		go srv.Start()
		defer srv.Shutdown()
		if !srv.ReadyForConnections(5 * time.Second) {
			return errors.New("NATS Server timeout")
		}

		nc, err = nats.Connect(srv.ClientURL(), nats.InProcessServer(srv))
		if err != nil {
			return err
		}
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	r.Set("jetstream", js)

	kvStore, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      "orchestration",
		Description: "Task orchestration",
		TTL:         1 * time.Hour,
	})
	if err != nil {
		return err
	}
	r.Set("kvstore", kvStore)
	objs, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      "reports",
		Description: "Diff reports",
		TTL:         31 * 24 * time.Hour,
	})
	if err != nil {
		return err
	}
	r.Set("objectstore", objs)

	<-ctx.Done()
	return nil
}
