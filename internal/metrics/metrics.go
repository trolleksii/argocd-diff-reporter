package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
)

func Init(ctx context.Context, cfg config.TelemetryConfig, log *slog.Logger) (func(), error) {
	noop := func() {}
	if !cfg.EmitMetrics || cfg.Endpoint == "" {
		return noop, nil
	}

	var exporter sdkmetric.Exporter
	var err error
	switch cfg.Protocol {
	case "", "http":
		exporter, err = otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpoint(cfg.Endpoint),
			otlpmetrichttp.WithInsecure(),
		)
	case "grpc":
		exporter, err = otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
			otlpmetricgrpc.WithInsecure(),
		)
	default:
		return noop, fmt.Errorf("unsupported metrics protocol %q (want http or grpc)", cfg.Protocol)
	}
	if err != nil {
		return noop, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.Service),
			semconv.ServiceVersionKey.String(cfg.Version),
		),
	)
	if err != nil {
		return noop, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(30*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return func() {
		if err := mp.Shutdown(context.Background()); err != nil {
			log.Error("failed to shut down meter provider", "error", err)
		}
	}, nil
}
