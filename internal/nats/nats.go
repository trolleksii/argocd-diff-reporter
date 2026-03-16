package nats

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
)

func New(ctx context.Context, cfg config.NatsConfig, log *slog.Logger) (*Nats, error) {
	var nc *nats.Conn
	var srv *server.Server

	closedCh := make(chan struct{})
	closedHandler := nats.ClosedHandler(func(_ *nats.Conn) {
		log.With("component", "nats").Debug("all connections drained")
		close(closedCh)
	})

	opts := &server.Options{
		DontListen:      true,
		JetStream:       true,
		JetStreamDomain: cfg.Domain,
		ServerName:      cfg.ServerName,
		StoreDir:        cfg.StoreDir,
		NoSigs:          true,
	}
	var err error
	srv, err = server.NewServer(opts)
	if err != nil {
		return nil, err
	}

	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		return nil, errors.New("nats: server timeout")
	}

	nc, err = nats.Connect(srv.ClientURL(), nats.InProcessServer(srv), closedHandler)
	if err != nil {
		srv.Shutdown()
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Drain()
		return nil, err
	}

	kvStore, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      "orchestration",
		Description: "Task orchestration",
		TTL:         1 * time.Hour,
	})
	if err != nil {
		nc.Drain()
		return nil, err
	}

	objs, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      "reports",
		Description: "Diff reports",
		TTL:         31 * 24 * time.Hour,
	})
	if err != nil {
		nc.Drain()
		return nil, err
	}

	return &Nats{nc: nc, srv: srv, js: js, kv: kvStore, obj: objs, log: log.With("component", "nats"), closedCh: closedCh}, nil
}

type Nats struct {
	nc       *nats.Conn
	srv      *server.Server
	js       jetstream.JetStream
	kv       jetstream.KeyValue
	obj      jetstream.ObjectStore
	log      *slog.Logger
	closedCh chan struct{}
}

func (n *Nats) Run(ctx context.Context) error {
	n.log.Info("nats started")
	<-ctx.Done()
	n.log.Debug("nats: draining connections...")
	n.nc.Drain()
	<-n.closedCh
	if n.srv != nil {
		n.srv.Shutdown()
		n.srv.WaitForShutdown()
		n.log.Debug("nats: server shutdown complete")
	}
	return nil
}

func (n *Nats) NewBus() *Bus {
	return &Bus{js: n.js}
}

func (n *Nats) NewStore() *Store {
	return &Store{kvStore: n.kv, objStore: n.obj}
}
