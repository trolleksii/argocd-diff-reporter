package diffapi

import (
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

// nameRe restricts owner/repo to characters GitHub allows; values also become
// NATS key parts.
var nameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

type DiffAPIHandler struct {
	log   *slog.Logger
	store *nats.Store
}

func NewDiffAPIHandler(log *slog.Logger, store *nats.Store) *DiffAPIHandler {
	return &DiffAPIHandler{
		log:   log.With("module", "server", "handler", "diffapi"),
		store: store,
	}
}

// NewRouteFunc returns a function that registers the diff API handler on the provided mux.
func NewRouteFunc(store *nats.Store) func(*http.ServeMux, *slog.Logger) {
	return func(mux *http.ServeMux, log *slog.Logger) {
		h := NewDiffAPIHandler(log, store)
		mux.HandleFunc("GET /api/diff/{owner}/{repo}/{pr}", h.ServeDiff)
	}
}

// ServeDiff returns the full PR diff as a single markdown document, intended
// to be embedded into an AI agent's prompt context.
func (h *DiffAPIHandler) ServeDiff(w http.ResponseWriter, r *http.Request) {
	owner := r.PathValue("owner")
	repo := r.PathValue("repo")
	number := r.PathValue("pr")
	if !nameRe.MatchString(owner) || !nameRe.MatchString(repo) {
		http.Error(w, "invalid owner or repo", http.StatusBadRequest)
		return
	}
	if n, err := strconv.Atoi(number); err != nil || n < 1 {
		http.Error(w, "invalid pr number", http.StatusBadRequest)
		return
	}
	// maxBytes caps each app's diff body; 0 or invalid means unlimited.
	maxBytes, _ := strconv.Atoi(r.URL.Query().Get("maxBytes"))

	pr, err := nats.GetValue[models.PullRequest](r.Context(), h.store, fmt.Sprintf("%s.%s.%s", owner, repo, number))
	if err != nil {
		h.log.Info("pr summary not found", "owner", owner, "repo", repo, "pr", number, "error", err)
		http.Error(w, "PR not found", http.StatusNotFound)
		return
	}
	if pr.Status == models.PipelineInProgress {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, "diff generation in progress")
		return
	}

	var doc strings.Builder
	fmt.Fprintf(&doc, "# Diff: %s/%s#%s (%s → %s)\n\n", owner, repo, number, pr.BaseSHA, pr.HeadSHA)
	doc.WriteString("_Unchanged paths are omitted._\n\n")

	for _, origin := range slices.Sorted(maps.Keys(pr.Files)) {
		file := pr.Files[origin]
		if len(file.Errors) > 0 {
			fmt.Fprintf(&doc, "## %s\n\n", origin)
			writeErrors(&doc, file.Errors)
		}
		for _, appName := range slices.Sorted(maps.Keys(file.Apps)) {
			fmt.Fprintf(&doc, "## %s (%s)\n\n", appName, origin)
			writeErrors(&doc, file.Apps[appName].Errors)

			key := fmt.Sprintf("%s.%s.%s.%s.%s.%s.%s", owner, repo, number, pr.BaseSHA, pr.HeadSHA, origin, appName)
			report, err := nats.GetObject[models.Report](r.Context(), h.store, key)
			if err != nil {
				doc.WriteString("_Report not available._\n\n")
				continue
			}
			if report.DiffStats.DiffCount == 0 {
				doc.WriteString("_No changes._\n\n")
				continue
			}
			s := report.DiffStats
			noun := "changes"
			if s.DiffCount == 1 {
				noun = "change"
			}
			fmt.Fprintf(&doc, "**%d %s** · %d added · %d removed · %d modified · %d reordered\n\n",
				s.DiffCount, noun, s.Additions, s.Removals, s.Modifications, s.OrderChanges)
			doc.WriteString(truncate(report.BodyMarkdown, maxBytes))
		}
	}

	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Write([]byte(doc.String()))
}

func writeErrors(doc *strings.Builder, errs []string) {
	if len(errs) == 0 {
		return
	}
	doc.WriteString("**Errors:**\n")
	for _, e := range errs {
		fmt.Fprintf(doc, "- %s\n", e)
	}
	doc.WriteString("\n")
}

// truncate cuts md at the last newline before max bytes, closing a dangling
// code fence so the markdown stays valid for the consumer.
func truncate(md string, max int) string {
	if max < 1 || len(md) <= max {
		return md
	}
	cut := strings.LastIndexByte(md[:max], '\n')
	kept := md[:cut+1] // cut == -1 keeps nothing
	openFence := ""
	for line := range strings.SplitSeq(kept, "\n") {
		if !strings.HasPrefix(line, "```") {
			continue
		}
		ticks := line[:len(line)-len(strings.TrimLeft(line, "`"))]
		if openFence == "" {
			openFence = ticks
		} else if len(ticks) >= len(openFence) {
			// per CommonMark, only a fence at least as long closes the block
			openFence = ""
		}
	}
	if openFence != "" {
		kept += openFence + "\n"
	}
	return kept + "\n[truncated]\n\n"
}
