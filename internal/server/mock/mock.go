package mock

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/trolleksii/argocd-diff-reporter/internal/bus"
	"github.com/trolleksii/argocd-diff-reporter/internal/server"
)

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
	bus *bus.Bus
	log *slog.Logger
}

func Route(log *slog.Logger, b *bus.Bus) server.Route {
	return func(mux *http.ServeMux) {
		mux.Handle("/mock", newMockHandler(log, b))
	}
}

func newMockHandler(log *slog.Logger, b *bus.Bus) *MockHandler {
	return &MockHandler{
		log: log.With("module", "server", "handler", "mock"),
		bus: b,
	}
}

func (h *MockHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.log.Info("something happened")
	var payload mockPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		h.log.Warn("Failed to parse mock payload", "error", err)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	switch payload.Action {
	case "closed":
		err := h.bus.Publish(r.Context(),
			"pr.closed",
			map[string]string{
				"repository": payload.Repository,
				"owner":      payload.Owner,
				"prNum":      payload.PrNum,
			},
			nil,
		)
		if err != nil {
			h.log.Error("Failed to publish PR event", "error", err)
		}
	case "opened", "synchronize", "reopened":
		err := h.bus.Publish(r.Context(),
			"pr.changed",
			map[string]string{
				"repository": payload.Repository,
				"owner":      payload.Owner,
				"prNum":      payload.PrNum,
				"title":      payload.Title,
				"author":     payload.Author,
				"branch":     payload.Branch,
				"baseSha":    payload.BaseSha,
				"headSha":    payload.HeadSha,
			},
			nil,
		)
		if err != nil {
			h.log.Error("Failed to publish PR event", "error", err)
		}
		h.log.Debug("new request enqued", "pr", payload.PrNum)
	}
}
