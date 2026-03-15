package tracing

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
)

func InitTracer(ctx context.Context, cfg config.TracingConfig, log *slog.Logger) (func(), error) {
	defaultCleanup := func() {}
	otlpEndpoint := cfg.Endpoint
	if otlpEndpoint == "" {
		otel.SetTracerProvider(
			noop.NewTracerProvider(),
		)
		return defaultCleanup, nil
	}

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(otlpEndpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return defaultCleanup, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.Service),
			semconv.ServiceVersionKey.String(cfg.Version),
		),
	)
	if err != nil {
		return defaultCleanup, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))

	return func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Error("failed to shut down tracing provider", "error", err)
		}
	}, nil
}
