package githubintegration

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"strings"
	"text/template"
	"time"

	"github.com/google/go-github/v82/github"
	"golang.org/x/oauth2"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
)

const CheckName = "ArgoCD Diff Report"

type GithubChecks struct {
	cfg    config.GithubChecksConfig
	tsrc   oauth2.TokenSource
	log    *slog.Logger
	client *github.Client
	bus    *nats.Bus
	store  *nats.Store
}

func New(cfg config.GithubChecksConfig, log *slog.Logger, b *nats.Bus, s *nats.Store, tokenSource oauth2.TokenSource) *GithubChecks {
	return &GithubChecks{
		cfg:   cfg,
		log:   log.With("worker", "githubintegration"),
		tsrc:  tokenSource,
		bus:   b,
		store: s,
	}
}

func (w *GithubChecks) Run(ctx context.Context) error {
	w.log.Info("starting github checks integration...")
	w.client = github.NewClient(oauth2.NewClient(ctx, w.tsrc))
	err := w.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "githubchecks",
		MaxDeliver: 3,
		AckWait:    10 * time.Second,
		Routes: []nats.Route{
			{Subjects: []string{subjects.WebhookPRChanged}, Handler: w.CreatePendingCheck},
			{Subjects: []string{subjects.PRProcessingCompleted}, Handler: w.UpdateCheckResult},
		},
	})
	if err != nil {
		return fmt.Errorf("githubchecks: consume: %w", err)
	}
	return nil
}

func (w *GithubChecks) CreatePendingCheck(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	pr, err := nats.Unmarshal[models.PullRequest](data)
	if err != nil {
		w.log.Error("failed to unmarshal pr object", "error", err)
		nak()
		return
	}
	cr, _, err := w.client.Checks.CreateCheckRun(ctx, pr.Owner, pr.Repo, github.CreateCheckRunOptions{
		Name:    CheckName,
		HeadSHA: pr.HeadSHA,
		Status:  github.Ptr("in_progress"),
		Output: &github.CheckRunOutput{
			Title:   github.Ptr("Processing ArgoCD Diff"),
			Summary: github.Ptr("Analyzing changes and generating diff reports..."),
		},
		DetailsURL: github.Ptr(fmt.Sprintf("%s/pulls/%s/%s/%s", w.cfg.UIBaseURL, pr.Owner, pr.Repo, pr.Number)),
	})
	if err != nil {
		w.log.Error("failed to create a check run", "error", err)
		nak()
	}

	key := fmt.Sprintf("checks.%s.%s.%s.%s", pr.Owner, pr.Repo, pr.Number, pr.HeadSHA)
	w.store.SetValue(ctx, key, cr.GetID())
	ack()
}

func (w *GithubChecks) UpdateCheckResult(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	number := headers["pr.number"]
	headSHA := headers["pr.sha.head"]
	key := fmt.Sprintf("checks.%s.%s.%s.%s", owner, repo, number, headSHA)
	checkId, err := nats.GetValue[int64](ctx, w.store, key)
	if err != nil {
		w.log.Error("failed to find check id", "owner", owner, "repo", repo, "number", number)
		nak()
	}
	pr, err := nats.Unmarshal[models.PullRequest](data)
	if err != nil {
		w.log.Error("failed to unmarshal pr object", "error", err)
		nak()
		return
	}

	d := ChecksDetails{
		Status:       pr.Status,
		Files:        pr.Files,
		BaseURL:      fmt.Sprintf("%s/reports/%s/%s/%s/%s:%s", w.cfg.UIBaseURL, owner, repo, number, pr.BaseSHA, pr.HeadSHA),
		SuccessCount: 0,
		ErrorCount:   0,
		TotalApps:    0,
		TotalChanges: 0,
	}

	for _, file := range pr.Files {
		d.ErrorCount += len(file.Errors)
		d.TotalApps += len(file.Apps)
		for _, app := range file.Apps {
			d.TotalChanges += app.DiffStats.DiffCount
		}
	}

	conclusion := "neutral"
	title := fmt.Sprintf("Found %d Changes", d.TotalChanges)
	summary := fmt.Sprintf("Found changes in %d file(s) affecting %d application(s). Review the diff reports before deployment.", len(pr.Files), d.TotalApps)

	if pr.Status == models.PipelineFailed {
		conclusion = "failure"
		title = "ApplicationSet Processing Failed"
		summary = fmt.Sprintf("Failed to process %d file(s). Check the errors below for details.", d.ErrorCount)
	} else if d.TotalChanges == 0 {
		conclusion = "success"
		title = "No Changes Detected"
		summary = "All ApplicationSets were processed successfully with no changes to deployed manifests."
	}

	_, _, err = w.client.Checks.UpdateCheckRun(ctx, owner, repo, checkId, github.UpdateCheckRunOptions{
		Name:       CheckName,
		Status:     github.Ptr("completed"),
		Conclusion: github.Ptr(conclusion),
		Output: &github.CheckRunOutput{
			Title:   github.Ptr(title),
			Summary: github.Ptr(summary),
			Text:    github.Ptr(buildDetailedOutput(w.log, d)),
		},
		DetailsURL: github.Ptr(fmt.Sprintf("%s/pulls/%s/%s/%s", w.cfg.UIBaseURL, owner, repo, number)),
	})
	if err != nil {
		w.log.Error("failed to update github check status", "error", err)
		nak()
	}
	ack()
}

type ChecksDetails struct {
	Status       models.PipelineStatus
	Files        map[string]models.FileResult
	BaseURL      string
	ErrorCount   int
	SuccessCount int
	TotalChanges int
	TotalApps    int
}

//go:embed templates/check_report.md.tmpl
var templateFS embed.FS

func buildDetailedOutput(log *slog.Logger, data ChecksDetails) string {
	t, err := templateFS.ReadFile("templates/check_report.md.tmpl")
	if err != nil {
		log.Error("failed to load template from embedfs", "error", err)
		return ""
	}
	helpers := template.FuncMap{
		"statusIcon": func(f models.FileResult) string {
			switch {
			case len(f.Errors) > 0:
				return "❌"
			case len(f.Apps) == 0:
				return "⚪"
			default:
				return "✅"
			}
		},
		"diffIcon": func(count int) string {
			if count > 0 {
				return "📝"
			}
			return "✓"
		},
		"now": func() string {
			return time.Now().Format("2006-01-02 15:04:05 UTC")
		},
		"report": func(file, app string) string {
			return fmt.Sprintf("%s:%s:%s", data.BaseURL, file, app)
		},
	}
	checkReportTmpl := template.Must(template.New("report").Funcs(helpers).Parse(string(t)))
	var b strings.Builder
	if err := checkReportTmpl.Execute(&b, data); err != nil {
		return fmt.Sprintf("error rendering github check details: %v", err)
	}
	return b.String()
}
