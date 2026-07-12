package trigger

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/google/go-github/v82/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/templates"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

// prListHandler serves a minimal list-pulls response.
func prListHandler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/repos/%s/%s/pulls", testOwner, testRepo) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		assert.Equal(t, "open", r.URL.Query().Get("state"))
		assert.Equal(t, "100", r.URL.Query().Get("per_page"))
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[
			{"number": 12, "title": "Second PR", "user": {"login": "test-user"}},
			{"number": 11, "title": "First PR", "user": {"login": "other-user"}}
		]`)
	}
}

func newTestPRSearchHandler(client *github.Client) *PRSearchHandler {
	return &PRSearchHandler{
		log:             testutil.NoopLogger(),
		client:          client,
		templateManager: templates.NewCatalog(),
	}
}

func searchPRs(t *testing.T, handler *PRSearchHandler, repo, author string) *httptest.ResponseRecorder {
	t.Helper()
	q := url.Values{"repo": {repo}}
	if author != "" {
		q.Set("author", author)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/prs?"+q.Encode(), nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func TestPRSearch_ReturnsPRItems(t *testing.T) {
	handler := newTestPRSearchHandler(fakeGithub(t, prListHandler(t)))

	rr := searchPRs(t, handler, testOwner+"/"+testRepo, "")

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, "text/html", rr.Header().Get("Content-Type"))
	body := rr.Body.String()
	assert.Contains(t, body, `data-number="12"`)
	assert.Contains(t, body, "Second PR")
	assert.Contains(t, body, `data-number="11"`)
	assert.Contains(t, body, "First PR")
}

func TestPRSearch_AuthorFilter(t *testing.T) {
	handler := newTestPRSearchHandler(fakeGithub(t, prListHandler(t)))

	rr := searchPRs(t, handler, testOwner+"/"+testRepo, "Test-User")

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	body := rr.Body.String()
	assert.Contains(t, body, `data-number="12"`)
	assert.NotContains(t, body, `data-number="11"`)
}

func TestPRSearch_NoOpenPRs_RendersMessage(t *testing.T) {
	handler := newTestPRSearchHandler(fakeGithub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[]`)
	}))

	rr := searchPRs(t, handler, testOwner+"/"+testRepo, "")

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "No open PRs found")
}

func TestPRSearch_InvalidInput_Returns400(t *testing.T) {
	handler := newTestPRSearchHandler(fakeGithub(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("github must not be called for invalid input")
	}))

	cases := []struct {
		name   string
		repo   string
		author string
	}{
		{"empty repo", "", ""},
		{"missing slash", testOwner, ""},
		{"empty owner part", "/" + testRepo, ""},
		{"empty repo part", testOwner + "/", ""},
		{"invalid author", testOwner + "/" + testRepo, "bad author"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := searchPRs(t, handler, tc.repo, tc.author)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
		})
	}
}

func TestPRSearch_GithubNotFound_RendersError(t *testing.T) {
	handler := newTestPRSearchHandler(fakeGithub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	rr := searchPRs(t, handler, testOwner+"/"+testRepo, "")

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "Repository not found")
}

func TestPRSearch_GithubError_RendersError(t *testing.T) {
	handler := newTestPRSearchHandler(fakeGithub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	rr := searchPRs(t, handler, testOwner+"/"+testRepo, "")

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "Failed to fetch PRs from GitHub")
}
