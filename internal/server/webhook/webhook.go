package webhook

import (
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/go-github/v82/github"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/server/webhook")

type WebhookHandler struct {
	cfg config.WebhookConfig
	log *slog.Logger
	bus *nats.Bus
}

// NewRouteFunc returns a function that registers the webhook handler on the provided mux.
func NewRouteFunc(cfg config.WebhookConfig, b *nats.Bus) func(*http.ServeMux, *slog.Logger) {
	return func(mux *http.ServeMux, log *slog.Logger) {
		mux.Handle("/webhook", newWebhookHandler(cfg, log, b))
	}
}

func newWebhookHandler(cfg config.WebhookConfig, log *slog.Logger, b *nats.Bus) *WebhookHandler {
	return &WebhookHandler{
		cfg: cfg,
		log: log.With("module", "server", "handler", "webhook"),
		bus: b,
	}
}

func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	trCtx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header)),
		"webhook",
	)
	defer span.End()

	payload, err := github.ValidatePayload(r, []byte(h.cfg.Secret))
	if err != nil {
		h.log.Error("invalid webhook signature", "error", err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	event, err := github.ParseWebHook(github.WebHookType(r), payload)
	if err != nil {
		h.log.Error("Failed to parse webhook", "error", err)
		span.SetStatus(codes.Error, err.Error())
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
		span.SetAttributes(
			attribute.String("pr.owner", owner),
			attribute.String("pr.repo", repoName),
		)
		if !MatchesRepoFilters(repoName, owner, h.cfg.AllowedRepos) {
			h.log.Info("event blocked by repo filters",
				"repo", repoName,
				"owner", owner,
				"allowedRepos", h.cfg.AllowedRepos)
			span.SetStatus(codes.Ok, "")
			return
		}
		switch action {
		case "closed":
			pr := event.GetPullRequest()
			var headers nats.Headers = make(map[string]string)
			prObj := models.PullRequest{
				Owner:  owner,
				Repo:   repoName,
				Number: strconv.Itoa(pr.GetNumber()),
			}
			span.SetAttributes(
				attribute.String("pr.number", prObj.Number),
				attribute.String("pr.owner", prObj.Owner),
				attribute.String("pr.repo", prObj.Repo),
			)
			otel.GetTextMapPropagator().Inject(trCtx, headers)
			data, err := nats.Marshal(prObj)
			if err != nil {
				h.log.Error("failed to marshal pr object", "error", err)
				span.SetStatus(codes.Error, err.Error())
				return
			}
			h.bus.Publish(r.Context(), "webhook.pr.closed", headers, data)
		case "opened", "synchronize", "reopened":
			pr := event.GetPullRequest()
			prObj := models.PullRequest{
				Owner:   owner,
				Repo:    repoName,
				Number:  strconv.Itoa(pr.GetNumber()),
				Title:   pr.GetTitle(),
				Author:  pr.GetUser().GetLogin(),
				BaseSHA: pr.GetBase().GetSHA(),
				HeadSHA: pr.GetHead().GetSHA(),
				Files:   make(map[string]models.FileResult),
			}

			span.SetAttributes(
				attribute.String("pr.number", prObj.Number),
				attribute.String("pr.title", prObj.Title),
				attribute.String("pr.author", prObj.Author),
				attribute.String("pr.baseSha", prObj.BaseSHA),
				attribute.String("pr.headSha", prObj.HeadSHA),
			)
			var headers nats.Headers = make(map[string]string)
			otel.GetTextMapPropagator().Inject(trCtx, headers)

			data, err := nats.Marshal(prObj)
			if err != nil {
				h.log.Error("failed to marshal pr object", "error", err)
				span.SetStatus(codes.Error, err.Error())
				return
			}
			h.bus.Publish(r.Context(), "webhook.pr.changed", headers, data)
		}
	default:
		h.log.Warn("unknown event type", "etype", github.WebHookType(r))
	}
	span.SetStatus(codes.Ok, "")
}

func MatchesRepoFilters(repo, owner string, allowedRepos []config.GitRepoFilter) bool {
	for _, rf := range allowedRepos {
		if rf.Owner == owner && rf.Repo == repo {
			return true
		}
	}
	return false
}
