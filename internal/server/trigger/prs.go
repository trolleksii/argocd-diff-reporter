package trigger

import (
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/go-github/v82/github"

	"github.com/trolleksii/argocd-diff-reporter/internal/templates"
)

type PRSearchHandler struct {
	log             *slog.Logger
	client          *github.Client
	templateManager templates.Catalog
}

func newPRSearchHandler(log *slog.Logger, client *github.Client) *PRSearchHandler {
	return &PRSearchHandler{
		log:             log.With("module", "server", "handler", "prsearch"),
		client:          client,
		templateManager: templates.NewCatalog(),
	}
}

type prItem struct {
	Number int
	Title  string
	Author string
}

type prSearchData struct {
	Error string
	PRs   []prItem
}

func (h *PRSearchHandler) render(w http.ResponseWriter, data prSearchData) {
	w.Header().Set("Content-Type", "text/html")
	if err := h.templateManager["prsearch"].Execute(w, data); err != nil {
		h.log.Error("failed to execute prsearch template", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (h *PRSearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	owner, repo, ok := strings.Cut(r.URL.Query().Get("repo"), "/")
	authorFilter := r.URL.Query().Get("author")
	if !ok || !nameRe.MatchString(owner) || !nameRe.MatchString(repo) ||
		(authorFilter != "" && !nameRe.MatchString(authorFilter)) {
		http.Error(w, "Invalid repository or author", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	prs, resp, err := h.client.PullRequests.List(ctx, owner, repo, &github.PullRequestListOptions{
		State:       "open",
		Sort:        "created",
		Direction:   "desc",
		ListOptions: github.ListOptions{PerPage: 100},
	})
	if err != nil {
		h.log.ErrorContext(ctx, "failed to list prs from github",
			"owner", owner, "repo", repo, "error", err)
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			h.render(w, prSearchData{Error: "Repository not found or the GitHub App has no access to it"})
			return
		}
		h.render(w, prSearchData{Error: "Failed to fetch PRs from GitHub"})
		return
	}

	data := prSearchData{PRs: make([]prItem, 0, len(prs))}
	for _, pr := range prs {
		prAuthor := pr.GetUser().GetLogin()
		if authorFilter != "" && !strings.EqualFold(prAuthor, authorFilter) {
			continue
		}
		data.PRs = append(data.PRs, prItem{
			Number: pr.GetNumber(),
			Title:  pr.GetTitle(),
			Author: prAuthor,
		})
	}
	h.render(w, data)
}
