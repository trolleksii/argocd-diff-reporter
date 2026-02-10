package webhook

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/go-github/v82/github"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/modules"
)

type WebhookHandler struct {
	cfg config.WebhookConfig
	js  jetstream.JetStream
	log *slog.Logger
}

func NewWebhookHandler(cfg config.WebhookConfig, log *slog.Logger, r *modules.Registry) (*WebhookHandler, error) {
	js, err := modules.Get[jetstream.JetStream](r, "jetstream")
	if err != nil {
		return nil, err
	}
	return &WebhookHandler{
		cfg: cfg,
		log: log.With("module", "server", "handler", "webhook"),
		js:  js,
	}, nil
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
		case "opened", "synchronize", "reopened", "closed":
			h.log.Debug("new request enqued")
			pr := event.GetPullRequest()
			prNum := pr.GetNumber()
			branch := pr.GetHead().GetRef()
			baseSha := pr.GetBase().GetSHA()
			headSha := pr.GetHead().GetSHA()
			eventId := fmt.Sprintf("%d.%s.%s", prNum, baseSha, headSha)
			_, err = h.js.PublishMsg(r.Context(), &nats.Msg{
				Subject: fmt.Sprintf("tasks.pr.%d", prNum),
				Header: nats.Header{
					"eventId":    []string{eventId},
					"action":     []string{action},
					"repository": []string{repoName},
					"owner":      []string{owner},
					"branch":     []string{branch},
					"prNum":      []string{strconv.Itoa(prNum)},
					"title":      []string{pr.GetTitle()},
					"author":     []string{pr.GetUser().GetLogin()},
					"baseSha":    []string{baseSha},
					"headSha":    []string{headSha},
				},
			})
			if err != nil {
				h.log.Error("Failed to publish PR event", "error", err)
			}
		}
	}
}

// parseWebhookRequest takes a request and a webhook config and returns a github event in no errors occured
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
