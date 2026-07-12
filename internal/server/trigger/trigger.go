package trigger

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"

	"github.com/google/go-github/v82/github"
	"golang.org/x/oauth2"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/server/trigger")

// nameRe restricts owner/repo to characters GitHub allows; values also become
// KV store key segments, so dots aside nothing else may pass through.
var nameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

type TriggerHandler struct {
	log    *slog.Logger
	bus    *nats.Bus
	client *github.Client
}

// NewRouteFunc returns a function that registers the manual trigger and PR
// search handlers on the provided mux.
func NewRouteFunc(b *nats.Bus, tsrc oauth2.TokenSource) func(*http.ServeMux, *slog.Logger) {
	return func(mux *http.ServeMux, log *slog.Logger) {
		client := github.NewClient(oauth2.NewClient(context.Background(), tsrc))
		mux.Handle("POST /api/trigger", newTriggerHandler(log, b, client))
		mux.Handle("GET /api/prs", newPRSearchHandler(log, client))
	}
}

func newTriggerHandler(log *slog.Logger, b *nats.Bus, client *github.Client) *TriggerHandler {
	return &TriggerHandler{
		log:    log.With("module", "server", "handler", "trigger"),
		bus:    b,
		client: client,
	}
}

type triggerRequest struct {
	Owner  string `json:"owner"`
	Repo   string `json:"repo"`
	Number int    `json:"number"`
}

func (h *TriggerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	trCtx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header)),
		"trigger",
	)
	defer span.End()

	var req triggerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	if !nameRe.MatchString(req.Owner) || !nameRe.MatchString(req.Repo) || req.Number < 1 {
		span.SetStatus(codes.Error, "invalid trigger request")
		http.Error(w, "Invalid owner, repo or PR number", http.StatusBadRequest)
		return
	}

	span.SetAttributes(
		attribute.String("pr.owner", req.Owner),
		attribute.String("pr.repo", req.Repo),
		attribute.Int("pr.number", req.Number),
	)

	pr, resp, err := h.client.PullRequests.Get(trCtx, req.Owner, req.Repo, req.Number)
	if err != nil {
		h.log.ErrorContext(trCtx, "failed to fetch pr from github",
			"owner", req.Owner, "repo", req.Repo, "number", req.Number, "error", err)
		span.SetStatus(codes.Error, err.Error())
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			http.Error(w, "PR not found or the GitHub App has no access to this repository", http.StatusNotFound)
			return
		}
		http.Error(w, "Failed to fetch PR from GitHub", http.StatusBadGateway)
		return
	}

	prObj := models.PullRequest{
		Owner:   req.Owner,
		Repo:    req.Repo,
		Number:  strconv.Itoa(req.Number),
		Title:   pr.GetTitle(),
		Author:  pr.GetUser().GetLogin(),
		BaseSHA: pr.GetBase().GetSHA(),
		HeadSHA: pr.GetHead().GetSHA(),
		Files:   make(map[string]models.FileResult),
	}

	span.SetAttributes(
		attribute.String("pr.title", prObj.Title),
		attribute.String("pr.author", prObj.Author),
		attribute.String("pr.baseSha", prObj.BaseSHA),
		attribute.String("pr.headSha", prObj.HeadSHA),
	)

	var headers nats.Headers = make(map[string]string)
	headers["skipChecks"] = "true"
	otel.GetTextMapPropagator().Inject(trCtx, headers)
	data, err := nats.Marshal(prObj)
	if err != nil {
		h.log.ErrorContext(trCtx, "failed to marshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	if err := h.bus.Publish(trCtx, subjects.WebhookPRChanged, headers, data); err != nil {
		h.log.ErrorContext(trCtx, "failed to publish pr changed event", "error", err)
		span.SetStatus(codes.Error, err.Error())
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}

	h.log.InfoContext(trCtx, "manually triggered diff report",
		"owner", prObj.Owner, "repo", prObj.Repo, "number", prObj.Number)
	span.SetStatus(codes.Ok, "")

	w.WriteHeader(http.StatusAccepted)
}
