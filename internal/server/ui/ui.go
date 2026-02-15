package ui

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/trolleksii/argocd-diff-reporter/internal/server"
	"github.com/trolleksii/argocd-diff-reporter/internal/server/templates"
	"github.com/trolleksii/argocd-diff-reporter/internal/store"
)

type UIHandler struct {
	log             *slog.Logger
	templateManager templates.Manager
	store           *store.Store
}

func NewUIHandler(log *slog.Logger, store *store.Store) *UIHandler {
	return &UIHandler{
		log:             log.With("module", "server", "handler", "ui"),
		templateManager: templates.NewManager(),
		store:           store,
	}
}

func Route(log *slog.Logger, store *store.Store) server.Route {
	uh := NewUIHandler(log, store)
	return func(mux *http.ServeMux) {
		mux.HandleFunc("GET /{$}", uh.ServeIndex)
		mux.HandleFunc("GET /pulls/{pr}", uh.ServeSummary)
		mux.HandleFunc("GET /reports/{pr}/{id...}", uh.ServeReport)
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
	if err := h.templateManager[templateName].Execute(w, h.store.GetIndex(r.Context())); err != nil {
		h.log.Error("failed to execute index template", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (h *UIHandler) ServeSummary(w http.ResponseWriter, r *http.Request) {
	prNumber := r.PathValue("pr")
	summary, err := h.store.GetSummary(r.Context(), prNumber)
	if err != nil {
		h.log.Error("failed to find summary for a pr", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
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
	prNumber, err := strconv.Atoi(r.PathValue("pr"))
	if err != nil {
		return "", errors.New("invalid PR number")
	}

	reportId := r.PathValue("id")
	chunks := strings.Split(reportId, ":")

	if len(chunks) != 4 {
		return "", errors.New("invalid report ID format")
	}

	return fmt.Sprintf("%d.%s.%s.%s.%s", prNumber, chunks[0], chunks[1], chunks[2], chunks[3]), nil
}

func (h *UIHandler) ServeReport(w http.ResponseWriter, r *http.Request) {
	id, err := ParseReportId(r)
	if err != nil {
		h.log.Error("invalid pr number or report id")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Get the report from storage
	report, err := h.store.GetReport(r.Context(), id)
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
