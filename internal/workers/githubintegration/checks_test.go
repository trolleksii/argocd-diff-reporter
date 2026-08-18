package githubintegration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

// TestCreatePendingCheck_CheckRunName verifies the check run name sent to
// GitHub: the default without an environment id, and the suffixed variant that
// prevents collisions when reporters in multiple clusters process the same PR.
func TestCreatePendingCheck_CheckRunName(t *testing.T) {
	tests := []struct {
		name  string
		envId string
		want  string
	}{
		{"default", "", "ArgoCD Diff Report"},
		{"environment set", "production", "ArgoCD Diff Report (production)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotName string
			srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				var body struct {
					Name string `json:"name"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				gotName = body.Name
				rw.Header().Set("Content-Type", "application/json")
				fmt.Fprint(rw, `{"id": 123}`)
			}))
			t.Cleanup(srv.Close)

			_, store, _ := testutil.StartNATS(t)
			w := New(config.GithubChecksConfig{EnvironmentId: tc.envId}, testutil.NoopLogger(), nil, store, nil)
			w.client = github.NewClient(nil)
			baseURL, err := url.Parse(srv.URL + "/")
			require.NoError(t, err)
			w.client.BaseURL = baseURL

			acked := false
			w.CreatePendingCheck(context.Background(), internalnats.Headers{}, manualPRPayload(t),
				func() error { acked = true; return nil },
				func() error { t.Error("expected ack, got nak"); return nil },
			)

			assert.True(t, acked, "expected message to be acked")
			assert.Equal(t, tc.want, gotName)
		})
	}
}

// TestBuildDetailedOutput_EnvironmentId verifies the report header with and
// without an environment id.
func TestBuildDetailedOutput_EnvironmentId(t *testing.T) {
	tests := []struct {
		name       string
		envId      string
		wantHeader string
	}{
		{"default", "", "## ArgoCD Diff Report"},
		{"environment set", "production", "## ArgoCD Diff Report (production)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out := buildDetailedOutput(testutil.NoopLogger(), ChecksDetails{
				EnvironmentId: tc.envId,
				Files:         map[string]models.FileResult{},
			})
			require.NotContains(t, out, "error rendering")
			assert.Equal(t, tc.wantHeader, strings.Split(out, "\n")[0])
		})
	}
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
