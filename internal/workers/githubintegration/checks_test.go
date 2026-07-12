package githubintegration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/google/go-github/v82/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

// newSkipTestWorker builds a GithubChecks worker whose GitHub client points at
// a server that fails the test if any request reaches it.
func newSkipTestWorker(t *testing.T) *GithubChecks {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("github must not be called for manually triggered reports: %s %s", r.Method, r.URL.Path)
	}))
	t.Cleanup(srv.Close)

	_, store, _ := testutil.StartNATS(t)
	w := New(config.GithubChecksConfig{}, testutil.NoopLogger(), nil, store, nil)
	w.client = github.NewClient(nil)
	baseURL, err := url.Parse(srv.URL + "/")
	require.NoError(t, err)
	w.client.BaseURL = baseURL
	return w
}

func manualPRPayload(t *testing.T) []byte {
	t.Helper()
	data, err := internalnats.Marshal(models.PullRequest{
		Owner:   "my-org",
		Repo:    "my-repo",
		Number:  "42",
		BaseSHA: "base-sha-abc",
		HeadSHA: "head-sha-def",
		Files:   make(map[string]models.FileResult),
	})
	require.NoError(t, err)
	return data
}

// TestCreatePendingCheck_ManualTrigger_SkipsGithub verifies that a manually
// triggered PR is acked without creating a check run.
func TestCreatePendingCheck_ManualTrigger_SkipsGithub(t *testing.T) {
	w := newSkipTestWorker(t)

	acked := false
	headers := internalnats.Headers{"skipChecks": "true"}
	w.CreatePendingCheck(context.Background(), headers, manualPRPayload(t),
		func() error { acked = true; return nil },
		func() error { t.Error("expected ack, got nak"); return nil },
	)

	assert.True(t, acked, "expected message to be acked")
}

// TestUpdateCheckResult_ManualTrigger_SkipsGithub verifies that a manually
// triggered PR is acked without looking up a check id or updating a check run.
func TestUpdateCheckResult_ManualTrigger_SkipsGithub(t *testing.T) {
	w := newSkipTestWorker(t)

	acked := false
	headers := internalnats.Headers{
		"pr.owner":    "my-org",
		"pr.repo":     "my-repo",
		"pr.number":   "42",
		"pr.sha.head": "head-sha-def",
		"skipChecks":  "true",
	}
	w.UpdateCheckResult(context.Background(), headers, manualPRPayload(t),
		func() error { acked = true; return nil },
		func() error { t.Error("expected ack, got nak"); return nil },
	)

	assert.True(t, acked, "expected message to be acked")
}
