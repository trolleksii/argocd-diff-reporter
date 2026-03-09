package mock

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"

	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/server/mock")

type mockPayload struct {
	Action     string `json:"action"`
	Repository string `json:"repository"`
	Owner      string `json:"owner"`
	PrNum      string `json:"prNum"`
	Title      string `json:"title"`
	Author     string `json:"author"`
	Branch     string `json:"branch"`
	BaseSha    string `json:"baseSha"`
	HeadSha    string `json:"headSha"`
}

type MockHandler struct {
	bus *nats.Bus
	log *slog.Logger
}

// NewRouteFunc returns a function that registers the webhook handler on the provided mux.
func NewRouteFunc(b *nats.Bus) func(*http.ServeMux, *slog.Logger) {
	return func(mux *http.ServeMux, log *slog.Logger) {
		mux.Handle("/mock", newMockHandler(log, b))
	}
}

func newMockHandler(log *slog.Logger, b *nats.Bus) *MockHandler {
	return &MockHandler{
		log: log.With("module", "server", "handler", "mock"),
		bus: b,
	}
}

func (h *MockHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	trCtx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header)),
		"ServeHTTP",
	)
	defer span.End()
	var payload mockPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		h.log.Warn("Failed to parse mock payload", "error", err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	span.SetStatus(codes.Ok, "event parsed")
	w.WriteHeader(http.StatusOK)
	switch payload.Action {
	case "closed":
		var headers nats.Headers = map[string]string{
			"repository": payload.Repository,
			"owner":      payload.Owner,
			"prNum":      payload.PrNum,
		}
		span.SetAttributes(
			attribute.String("pr.repo", payload.Repository),
			attribute.String("pr.owner", payload.Owner),
			attribute.String("pr.number", payload.PrNum),
		)
		otel.GetTextMapPropagator().Inject(trCtx, headers)
		err := h.bus.Publish(r.Context(),
			"pr.closed",
			headers,
			nil,
		)
		if err != nil {
			h.log.Error("Failed to publish PR event", "error", err)
		}
	case "opened", "synchronize", "reopened":
		var headers nats.Headers = map[string]string{
			"repository": payload.Repository,
			"owner":      payload.Owner,
			"prNum":      payload.PrNum,
			"title":      payload.Title,
			"author":     payload.Author,
			"branch":     payload.Branch,
			"baseSha":    payload.BaseSha,
			"headSha":    payload.HeadSha,
		}
		span.SetAttributes(
			attribute.String("pr.repo", payload.Repository),
			attribute.String("pr.owner", payload.Owner),
			attribute.String("pr.number", payload.PrNum),
			attribute.String("pr.title", payload.Title),
			attribute.String("pr.author", payload.Author),
			attribute.String("pr.branch", payload.Branch),
			attribute.String("pr.baseSha", payload.BaseSha),
			attribute.String("pr.headSha", payload.HeadSha),
		)
		otel.GetTextMapPropagator().Inject(trCtx, headers)
		err := h.bus.Publish(r.Context(),
			"webhook.pr.changed",
			headers,
			nil,
		)
		if err != nil {
			h.log.Error("Failed to publish PR event", "error", err)
		}
		h.log.Debug("new request enqued", "pr", payload.PrNum)
	}
}
