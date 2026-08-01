package diffapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

func newMux(store *nats.Store) *http.ServeMux {
	mux := http.NewServeMux()
	NewRouteFunc(store)(mux, testutil.NoopLogger())
	return mux
}

func get(t *testing.T, mux *http.ServeMux, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	return rr
}

func seedPR(t *testing.T, store *nats.Store, pr models.PullRequest) {
	t.Helper()
	key := pr.Owner + "." + pr.Repo + "." + pr.Number
	require.NoError(t, store.SetValue(context.Background(), key, pr))
}

func TestServeDiff_HappyPath(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	seedPR(t, store, models.PullRequest{
		Number: "42", Owner: "org", Repo: "repo",
		BaseSHA: "base", HeadSHA: "head",
		Status: models.PipelineSucceeded,
		Files: map[string]models.FileResult{
			"apps/b.yaml": {Apps: map[string]models.App{"app-b": {}}},
			"apps/a.yaml": {Apps: map[string]models.App{"app-a": {}}},
		},
	})
	for _, app := range []string{"app-a", "app-b"} {
		file := "apps/" + strings.TrimPrefix(app, "app-") + ".yaml"
		report := models.Report{
			Owner: "org", Repo: "repo", PRNumber: "42",
			BaseSHA: "base", HeadSHA: "head", File: file, AppName: app,
			BodyMarkdown: "### /spec/replicas\n\n**Modified**\n```diff\n- 2\n+ 3\n```\n\n",
			DiffStats:    models.DiffStats{DiffCount: 1, Modifications: 1},
		}
		key := "org.repo.42.base.head." + file + "." + app
		require.NoError(t, store.StoreObject(context.Background(), key, report))
	}

	rr := get(t, newMux(store), "/api/diff/org/repo/42")

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "text/markdown; charset=utf-8", rr.Header().Get("Content-Type"))

	body := rr.Body.String()
	assert.Contains(t, body, "# Diff: org/repo#42 (base → head)")
	assert.Contains(t, body, "## app-a (apps/a.yaml)")
	assert.Contains(t, body, "## app-b (apps/b.yaml)")
	assert.Contains(t, body, "**1 change** · 0 added · 0 removed · 1 modified · 0 reordered")
	assert.Contains(t, body, "### /spec/replicas")
	assert.Contains(t, body, "```diff\n- 2\n+ 3\n```")
	assert.Less(t, strings.Index(body, "## app-a"), strings.Index(body, "## app-b"), "apps must be in stable sorted order")
}

func TestServeDiff_InvalidNames_Returns400(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)
	mux := newMux(store)

	assert.Equal(t, http.StatusBadRequest, get(t, mux, "/api/diff/bad$owner/repo/1").Code)
	assert.Equal(t, http.StatusBadRequest, get(t, mux, "/api/diff/owner/repo/not-a-number").Code)
	assert.Equal(t, http.StatusBadRequest, get(t, mux, "/api/diff/owner/repo/0").Code)
}

func TestServeDiff_UnknownPR_Returns404(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	rr := get(t, newMux(store), "/api/diff/org/repo/99")
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestServeDiff_InProgress_Returns202(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	seedPR(t, store, models.PullRequest{
		Number: "7", Owner: "org", Repo: "repo",
		Status: models.PipelineInProgress,
	})

	rr := get(t, newMux(store), "/api/diff/org/repo/7")
	assert.Equal(t, http.StatusAccepted, rr.Code)
}

func TestServeDiff_MissingReportAndErrors(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	seedPR(t, store, models.PullRequest{
		Number: "3", Owner: "org", Repo: "repo",
		BaseSHA: "base", HeadSHA: "head",
		Status: models.PipelineFailed,
		Files: map[string]models.FileResult{
			"apps/x.yaml": {
				Errors: []string{"appset expansion failed"},
				Apps:   map[string]models.App{"app-x": {Errors: []string{"helm render failed"}}},
			},
		},
	})

	rr := get(t, newMux(store), "/api/diff/org/repo/3")
	assert.Equal(t, http.StatusOK, rr.Code)

	body := rr.Body.String()
	assert.Contains(t, body, "- appset expansion failed")
	assert.Contains(t, body, "- helm render failed")
	assert.Contains(t, body, "_Report not available._")
}

func TestServeDiff_ZeroDiff_NoChangesNote(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	seedPR(t, store, models.PullRequest{
		Number: "5", Owner: "org", Repo: "repo",
		BaseSHA: "base", HeadSHA: "head",
		Status: models.PipelineSucceeded,
		Files: map[string]models.FileResult{
			"apps/z.yaml": {Apps: map[string]models.App{"app-z": {}}},
		},
	})
	report := models.Report{AppName: "app-z"}
	require.NoError(t, store.StoreObject(context.Background(), "org.repo.5.base.head.apps/z.yaml.app-z", report))

	rr := get(t, newMux(store), "/api/diff/org/repo/5")
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "_No changes._")
}

func TestServeDiff_MaxBytes_Truncates(t *testing.T) {
	_, store, _ := testutil.StartNATS(t)

	var md strings.Builder
	md.WriteString("### /data/values\n\n**Modified**\n```diff\n")
	for range 100 {
		md.WriteString("- old-value-line\n+ new-value-line\n")
	}
	md.WriteString("```\n\n")

	seedPR(t, store, models.PullRequest{
		Number: "9", Owner: "org", Repo: "repo",
		BaseSHA: "base", HeadSHA: "head",
		Status: models.PipelineSucceeded,
		Files: map[string]models.FileResult{
			"apps/big.yaml": {Apps: map[string]models.App{"big-app": {}}},
		},
	})
	report := models.Report{
		AppName: "big-app", BodyMarkdown: md.String(),
		DiffStats: models.DiffStats{DiffCount: 1, Modifications: 1},
	}
	require.NoError(t, store.StoreObject(context.Background(), "org.repo.9.base.head.apps/big.yaml.big-app", report))

	rr := get(t, newMux(store), "/api/diff/org/repo/9?maxBytes=500")
	assert.Equal(t, http.StatusOK, rr.Code)

	body := rr.Body.String()
	assert.Contains(t, body, "[truncated]")
	assert.NotContains(t, body, md.String(), "body must not contain the full diff")
	assert.Equal(t, 0, strings.Count(body, "```")%2, "all fences must be closed")
}

func TestTruncate(t *testing.T) {
	t.Run("under limit unchanged", func(t *testing.T) {
		assert.Equal(t, "short\n", truncate("short\n", 100))
	})
	t.Run("zero means unlimited", func(t *testing.T) {
		assert.Equal(t, "anything\n", truncate("anything\n", 0))
	})
	t.Run("closes open fence", func(t *testing.T) {
		md := "### p\n\n```diff\n- a\n+ b\nlong tail that goes over the limit\n```\n"
		got := truncate(md, 20)
		assert.Contains(t, got, "[truncated]")
		assert.Equal(t, 0, strings.Count(got, "```")%2, "fences balanced")
	})
	t.Run("closed fence stays closed", func(t *testing.T) {
		md := "```diff\n- a\n```\n\nmore text beyond the cut point here\n"
		got := truncate(md, 20)
		assert.Equal(t, 0, strings.Count(got, "```")%2)
		assert.Contains(t, got, "[truncated]")
	})
}
