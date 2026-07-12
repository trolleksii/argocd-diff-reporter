package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/google/go-github/v82/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

const (
	testOwner      = "my-org"
	testRepo       = "my-repo"
	testNumber     = 42
	testStreamName = "trigger-test"
)

// fakeGithub returns a github.Client pointed at a test server that serves the
// given handler.
func fakeGithub(t *testing.T, handler http.HandlerFunc) *github.Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	client := github.NewClient(nil)
	baseURL, err := url.Parse(srv.URL + "/")
	require.NoError(t, err)
	client.BaseURL = baseURL
	return client
}

// prGetHandler serves a minimal GET pulls response for the test PR.
func prGetHandler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		expected := fmt.Sprintf("/repos/%s/%s/pulls/%d", testOwner, testRepo, testNumber)
		if r.URL.Path != expected {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"number": 42,
			"title": "Test PR",
			"user": {"login": "test-user"},
			"base": {"sha": "base-sha-abc"},
			"head": {"sha": "head-sha-def"}
		}`)
	}
}

func setupBusWithStream(t *testing.T) *internalnats.Bus {
	t.Helper()
	bus, _, _ := testutil.StartNATS(t)
	err := bus.EnsureStream(context.Background(), testStreamName, []string{
		subjects.WebhookPRChanged,
	})
	require.NoError(t, err, "failed to create NATS stream")
	return bus
}

func newTestHandler(bus *internalnats.Bus, client *github.Client) *TriggerHandler {
	return &TriggerHandler{
		log:    testutil.NoopLogger(),
		bus:    bus,
		client: client,
	}
}

func triggerRequestBody(t *testing.T, owner, repo string, number int) *bytes.Reader {
	t.Helper()
	data, err := json.Marshal(map[string]any{"owner": owner, "repo": repo, "number": number})
	require.NoError(t, err)
	return bytes.NewReader(data)
}

// TestTrigger_ValidRequest_PublishesManualPRChanged verifies that a valid
// trigger publishes a PR-changed event with the manual trigger kind and the
// SHAs fetched from GitHub, and returns 202.
func TestTrigger_ValidRequest_PublishesManualPRChanged(t *testing.T) {
	bus := setupBusWithStream(t)
	msgCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)
	handler := newTestHandler(bus, fakeGithub(t, prGetHandler(t)))

	req := httptest.NewRequest(http.MethodPost, "/api/trigger", triggerRequestBody(t, testOwner, testRepo, testNumber))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusAccepted, rr.Code, rr.Body.String())
	assert.Empty(t, rr.Body.String())

	select {
	case msg := <-msgCh:
		pr, err := internalnats.Unmarshal[models.PullRequest](msg)
		require.NoError(t, err)
		assert.Equal(t, testOwner, pr.Owner)
		assert.Equal(t, testRepo, pr.Repo)
		assert.Equal(t, "42", pr.Number)
		assert.Equal(t, "base-sha-abc", pr.BaseSHA)
		assert.Equal(t, "head-sha-def", pr.HeadSHA)
		assert.Equal(t, "test-user", pr.Author)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for NATS message")
	}
}

// TestTrigger_GithubNotFound_Returns404 verifies that a PR the GitHub App
// cannot access results in a 404 and no published event.
func TestTrigger_GithubNotFound_Returns404(t *testing.T) {
	bus := setupBusWithStream(t)
	msgCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)
	handler := newTestHandler(bus, fakeGithub(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/trigger", triggerRequestBody(t, testOwner, testRepo, testNumber))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)

	select {
	case <-msgCh:
		t.Fatal("expected no NATS message for inaccessible PR")
	case <-time.After(200 * time.Millisecond):
		// no message received — correct behaviour
	}
}

// TestTrigger_InvalidInput_Returns400 verifies that malformed owner/repo/number
// values are rejected before any GitHub call.
func TestTrigger_InvalidInput_Returns400(t *testing.T) {
	bus := setupBusWithStream(t)
	handler := newTestHandler(bus, fakeGithub(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("github must not be called for invalid input")
	}))

	cases := []struct {
		name   string
		owner  string
		repo   string
		number int
	}{
		{"empty owner", "", testRepo, testNumber},
		{"owner with slash", "my/org", testRepo, testNumber},
		{"empty repo", testOwner, "", testNumber},
		{"zero number", testOwner, testRepo, 0},
		{"negative number", testOwner, testRepo, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/trigger", triggerRequestBody(t, tc.owner, tc.repo, tc.number))
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
		})
	}
}
