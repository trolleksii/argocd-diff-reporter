package webhook

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/go-github/v82/github"

	"github.com/trolleksii/argocd-diff-reporter/internal/bus"
	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/server"
)

type WebhookHandler struct {
	cfg config.WebhookConfig
	bus *bus.Bus
	log *slog.Logger
}

func Route(cfg config.WebhookConfig, log *slog.Logger, b *bus.Bus) server.Route {
	return func(mux *http.ServeMux) {
		mux.Handle("/webhook", newWebhookHandler(cfg, log, b))
	}
}

func newWebhookHandler(cfg config.WebhookConfig, log *slog.Logger, b *bus.Bus) *WebhookHandler {
	return &WebhookHandler{
		cfg: cfg,
		log: log.With("module", "server", "handler", "webhook"),
		bus: b,
	}
}

func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	event, err := parseWebhookRequest(r, h.cfg.Secret)
	if err != nil {
		h.log.Warn("Failed to parse webhook")
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	switch event := event.(type) {
	case *github.PullRequestEvent:
		action := event.GetAction()
		repo := event.GetRepo()
		owner := repo.GetOwner().GetLogin()
		repoName := repo.GetName()
		if !MatchesRepoFilters(repoName, owner, h.cfg.AllowedRepos) {
			return
		}

		switch action {
		case "closed":
			pr := event.GetPullRequest()
			prNum := strconv.Itoa(pr.GetNumber())
			err = h.bus.Publish(r.Context(),
				"webhook.pr.closed",
				map[string]string{
					"repository": repoName,
					"owner":      owner,
					"prNum":      prNum,
				},
				nil,
			)
			if err != nil {
				h.log.Error("Failed to publish PR event", "error", err)
			}
		case "opened", "synchronize", "reopened":
			pr := event.GetPullRequest()
			prNum := strconv.Itoa(pr.GetNumber())
			err = h.bus.Publish(r.Context(),
				"webhook.pr.changed",
				map[string]string{
					"repository": repoName,
					"owner":      owner,
					"prNum":      prNum,
					"title":      pr.GetTitle(),
					"author":     pr.GetUser().GetLogin(),
					"branch":     pr.GetHead().GetRef(),
					"baseSha":    pr.GetBase().GetSHA(),
					"headSha":    pr.GetHead().GetSHA(),
				},
				nil,
			)
			if err != nil {
				h.log.Error("Failed to publish PR event", "error", err)
			}
			h.log.Debug("new request enqued", "pr", prNum)
		}
	}
}

// parseWebhookRequest takes a request and a webhook config and returns a github event if no errors occured
func parseWebhookRequest(r *http.Request, secret string) (any, error) {
	payload, err := github.ValidatePayload(r, []byte(secret))
	if err != nil {
		return nil, fmt.Errorf("invalid webhook signature: %w", err)
	}

	event, err := github.ParseWebHook(github.WebHookType(r), payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse webhook payload: %w", err)
	}

	return event, nil
}

func MatchesRepoFilters(repo, owner string, allowedRepos []config.GitRepoFilter) bool {
	for _, rf := range allowedRepos {
		if rf.Owner == owner && rf.Repo == repo {
			return true
		}
	}
	return false
}
