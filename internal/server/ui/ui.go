package ui

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/templates"
)

type UIHandler struct {
	log             *slog.Logger
	templateManager templates.Catalog
	store           *nats.Store
}

func NewUIHandler(log *slog.Logger, store *nats.Store) *UIHandler {
	return &UIHandler{
		log:             log.With("module", "server", "handler", "ui"),
		templateManager: templates.NewCatalog(),
		store:           store,
	}
}

// NewRouteFunc returns a function that registers the webhook handler on the provided mux.
func NewRouteFunc(store *nats.Store) func(*http.ServeMux, *slog.Logger) {
	return func(mux *http.ServeMux, log *slog.Logger) {
		uh := NewUIHandler(log, store)
		mux.HandleFunc("GET /{$}", uh.ServeIndex)
		mux.HandleFunc("GET /pulls/{owner}/{repo}/{pr}", uh.ServeSummary)
		mux.HandleFunc("GET /reports/{owner}/{repo}/{pr}/{id...}", uh.ServeReport)
	}
}

func (h *UIHandler) ServeIndex(w http.ResponseWriter, r *http.Request) {
	// Check if this is a partial request for the PR list only
	partial := r.URL.Query().Get("partial") == "true" || r.Header.Get("HX-Request") != ""

	w.Header().Set("Content-Type", "text/html")
	templateName := "index"
	if partial {
		templateName = "latestprs"
	}
	index, err := nats.GetValue[[]models.PullRequest](r.Context(), h.store, "index")
	if err != nil {
		h.log.Error("failed to fetch index", "error", err)
	}
	if err := h.templateManager[templateName].Execute(w, index); err != nil {
		h.log.Error("failed to execute index template", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (h *UIHandler) ServeSummary(w http.ResponseWriter, r *http.Request) {
	owner := r.PathValue("owner")
	repo := r.PathValue("repo")
	number := r.PathValue("pr")
	key := fmt.Sprintf("%s.%s.%s", owner, repo, number)
	summary, err := nats.GetValue[models.PullRequest](r.Context(), h.store, key)
	if err != nil {
		h.log.Error("failed to find summary for a pr", "error", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	partial := r.URL.Query().Get("partial") == "true" || r.Header.Get("HX-Request") != ""
	templateName := "summary"
	if partial {
		templateName = "table"
	}
	if err := h.templateManager[templateName].Execute(w, summary); err != nil {
		h.log.Error("failed to execute summary template")
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func ParseReportId(r *http.Request) (string, error) {
	owner := r.PathValue("owner")
	repo := r.PathValue("repo")
	number := r.PathValue("pr")
	reportId := r.PathValue("id")
	chunks := strings.Split(reportId, ":")

	if len(chunks) != 4 {
		return "", errors.New("invalid report ID format")
	}

	return fmt.Sprintf("%s.%s.%s.%s.%s.%s.%s", owner, repo, number, chunks[0], chunks[1], chunks[2], chunks[3]), nil
}

func (h *UIHandler) ServeReport(w http.ResponseWriter, r *http.Request) {
	id, err := ParseReportId(r)
	if err != nil {
		h.log.Error("invalid pr number or report id", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Get the report from storage
	report, err := nats.GetObject[models.Report](r.Context(), h.store, id)
	if err != nil {
		h.log.Error("report not found", "id", id)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	err = h.templateManager["report"].Execute(w, report)
	if err != nil {
		h.log.Error("Failed to execute report template", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}
