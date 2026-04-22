package ui

import (
	"context"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

// seedIndex stores a []models.PullRequest slice under the "index" KV key.
func seedIndex(t *testing.T, store interface {
	SetValue(ctx context.Context, key string, value any) error
}, prs []models.PullRequest) {
	t.Helper()
	err := store.SetValue(context.Background(), "index", prs)
	require.NoError(t, err, "failed to seed index")
}

func TestServeIndex_FullPage_ContainsPRData(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	prs := []models.PullRequest{
		{
			Number: "42",
			Author: "test-user",
			Owner:  "my-org",
			Repo:   "my-repo",
			Title:  "Add new feature",
			Status: models.PipelineSucceeded,
		},
	}
	seedIndex(t, store, prs)

	handler := NewUIHandler(testutil.NoopLogger(), store)

	req := httptest.NewRequest(http.MethodGet, "/?", nil)
	rr := httptest.NewRecorder()
	handler.ServeIndex(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()

	// Full-page response must include outer HTML structure.
	assert.Contains(t, body, "<!DOCTYPE html>", "full-page response should include DOCTYPE")
	assert.Contains(t, body, "<html", "full-page response should include <html> tag")

	// The index template uses HTMX to load the PR list lazily; the full page
	// itself always contains the application title and the HTMX partial URL.
	assert.Contains(t, body, "Differ", "response should contain application title")
	assert.Contains(t, body, "/?partial=true", "response should reference the partial endpoint")
}

func TestServeIndex_HTMXPartial_ReturnsFragment(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	prs := []models.PullRequest{
		{
			Number: "7",
			Author: "dev",
			Owner:  "acme",
			Repo:   "infra",
			Title:  "Fix rollout",
			Status: models.PipelineInProgress,
		},
	}
	seedIndex(t, store, prs)

	handler := NewUIHandler(testutil.NoopLogger(), store)

	req := httptest.NewRequest(http.MethodGet, "/?partial=true", nil)
	req.Header.Set("HX-Request", "true")
	rr := httptest.NewRecorder()
	handler.ServeIndex(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()

	// Partial response must NOT include the full page wrapper
	assert.False(t, strings.Contains(body, "<!DOCTYPE html>"), "HTMX partial should not include DOCTYPE")
	assert.False(t, strings.Contains(body, "<html"), "HTMX partial should not include <html> tag")

	// But it must still contain the PR data
	assert.Contains(t, body, "acme", "partial response should contain PR owner")
	assert.Contains(t, body, "infra", "partial response should contain PR repo")
	assert.Contains(t, body, "7", "partial response should contain PR number")
	assert.Contains(t, body, "Fix rollout", "partial response should contain PR title")
}

func TestServeIndex_QueryPartial_ReturnsFragment(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	prs := []models.PullRequest{
		{
			Number: "99",
			Owner:  "org",
			Repo:   "repo",
			Title:  "Partial via query param",
			Status: models.PipelineFailed,
		},
	}
	seedIndex(t, store, prs)

	handler := NewUIHandler(testutil.NoopLogger(), store)

	// partial=true query param (no HX-Request header) should also return fragment
	req := httptest.NewRequest(http.MethodGet, "/?partial=true", nil)
	rr := httptest.NewRecorder()
	handler.ServeIndex(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()

	assert.False(t, strings.Contains(body, "<!DOCTYPE html>"), "query-param partial should not include DOCTYPE")
	assert.Contains(t, body, "99", "partial response should contain PR number")
	assert.Contains(t, body, "Partial via query param", "partial response should contain PR title")
}

func TestServeIndex_EmptyIndex_NoError(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	// Seed an empty slice — the template should render without error.
	seedIndex(t, store, []models.PullRequest{})

	handler := NewUIHandler(testutil.NoopLogger(), store)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeIndex(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestServeIndex_ContentTypeHTML(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)
	seedIndex(t, store, []models.PullRequest{})

	handler := NewUIHandler(testutil.NoopLogger(), store)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeIndex(rr, req)

	assert.Equal(t, "text/html", rr.Header().Get("Content-Type"))
}

func TestServeSummary_FullPage_ContainsPRData(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	pr := models.PullRequest{
		Number:  "42",
		Author:  "test-user",
		Owner:   "myorg",
		Repo:    "myrepo",
		Title:   "Add deployment config",
		BaseSHA: "abc123",
		HeadSHA: "def456",
		Status:  models.PipelineSucceeded,
	}
	err := store.SetValue(context.Background(), "myorg.myrepo.42", pr)
	require.NoError(t, err)

	handler := NewUIHandler(testutil.NoopLogger(), store)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /pulls/{owner}/{repo}/{pr}", handler.ServeSummary)

	req := httptest.NewRequest(http.MethodGet, "/pulls/myorg/myrepo/42", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "text/html", rr.Header().Get("Content-Type"))

	body := rr.Body.String()
	assert.Contains(t, body, "<!DOCTYPE html>", "full-page response should include DOCTYPE")
	assert.Contains(t, body, "Add deployment config", "full-page response should contain PR title")
	assert.Contains(t, body, "myorg", "full-page response should contain owner")
	assert.Contains(t, body, "myrepo", "full-page response should contain repo")
}

func TestServeSummary_HTMXPartial_ReturnsFragment(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	pr := models.PullRequest{
		Number:  "42",
		Author:  "dev-user",
		Owner:   "myorg",
		Repo:    "myrepo",
		Title:   "Fix rollback strategy",
		BaseSHA: "aaa111",
		HeadSHA: "bbb222",
		Status:  models.PipelineInProgress,
	}
	err := store.SetValue(context.Background(), "myorg.myrepo.42", pr)
	require.NoError(t, err)

	handler := NewUIHandler(testutil.NoopLogger(), store)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /pulls/{owner}/{repo}/{pr}", handler.ServeSummary)

	req := httptest.NewRequest(http.MethodGet, "/pulls/myorg/myrepo/42", nil)
	req.Header.Set("HX-Request", "true")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	body := rr.Body.String()
	assert.False(t, strings.Contains(body, "<!DOCTYPE html>"), "HTMX partial should not include DOCTYPE")
	assert.False(t, strings.Contains(body, "<html"), "HTMX partial should not include <html> tag")
	assert.Contains(t, body, "Fix rollback strategy", "partial response should contain PR title")
}

func TestParseReportId_Valid(t *testing.T) {
	var result string
	var parseErr error

	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/{owner}/{repo}/{pr}/{id...}", func(w http.ResponseWriter, r *http.Request) {
		result, parseErr = ParseReportId(r)
	})

	req := httptest.NewRequest(http.MethodGet, "/reports/org/repo/1/baseSHA:headSHA:path/to/file.yaml:appName", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	require.NoError(t, parseErr)
	assert.Equal(t, "org.repo.1.baseSHA.headSHA.path/to/file.yaml.appName", result)
}

func TestParseReportId_TooFewChunks(t *testing.T) {
	var parseErr error

	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/{owner}/{repo}/{pr}/{id...}", func(w http.ResponseWriter, r *http.Request) {
		_, parseErr = ParseReportId(r)
	})

	req := httptest.NewRequest(http.MethodGet, "/reports/org/repo/1/only:two:chunks", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Error(t, parseErr)
}

func TestParseReportId_TooManyChunks(t *testing.T) {
	var parseErr error

	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/{owner}/{repo}/{pr}/{id...}", func(w http.ResponseWriter, r *http.Request) {
		_, parseErr = ParseReportId(r)
	})

	req := httptest.NewRequest(http.MethodGet, "/reports/org/repo/1/a:b:c:d:e", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Error(t, parseErr)
}

func TestServeReport_RendersReport(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	report := models.Report{
		Owner:    "myorg",
		Repo:     "myrepo",
		PRNumber: "42",
		BaseSHA:  "base-sha",
		HeadSHA:  "head-sha",
		File:     "file.yaml",
		AppName:  "my-app",
		Body:     template.HTML("<p>diff content here</p>"),
		DiffStats: models.DiffStats{
			DiffCount:     3,
			Additions:     1,
			Removals:      1,
			Modifications: 1,
		},
	}
	compositeKey := "myorg.myrepo.42.base-sha.head-sha.file.yaml.my-app"
	err := store.StoreObject(context.Background(), compositeKey, report)
	require.NoError(t, err)

	handler := NewUIHandler(testutil.NoopLogger(), store)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/{owner}/{repo}/{pr}/{id...}", handler.ServeReport)

	req := httptest.NewRequest(http.MethodGet, "/reports/myorg/myrepo/42/base-sha:head-sha:file.yaml:my-app", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "my-app", "response should contain app name")
	assert.Contains(t, body, "diff content here", "response should contain report body content")
}

func TestServeReport_NotFound_Returns404(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	handler := NewUIHandler(testutil.NoopLogger(), store)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/{owner}/{repo}/{pr}/{id...}", handler.ServeReport)

	req := httptest.NewRequest(http.MethodGet, "/reports/myorg/myrepo/42/no-sha:no-sha:missing.yaml:missing-app", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestServeReport_InvalidId_Returns500(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	handler := NewUIHandler(testutil.NoopLogger(), store)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /reports/{owner}/{repo}/{pr}/{id...}", handler.ServeReport)

	req := httptest.NewRequest(http.MethodGet, "/reports/myorg/myrepo/42/invalid-no-colons", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}
