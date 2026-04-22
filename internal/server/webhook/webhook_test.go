package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	internalnats "github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

const (
	testSecret     = "test-webhook-secret"
	testOwner      = "my-org"
	testRepo       = "my-repo"
	testStreamName = "webhook-test"
)

// prPayload builds a minimal GitHub pull_request webhook payload JSON.
func prPayload(t *testing.T, action, owner, repo string) []byte {
	t.Helper()
	payload := map[string]any{
		"action": action,
		"number": 42,
		"pull_request": map[string]any{
			"number": 42,
			"title":  "Test PR",
			"user":   map[string]any{"login": "test-user"},
			"base":   map[string]any{"sha": "base-sha-abc"},
			"head":   map[string]any{"sha": "head-sha-def"},
		},
		"repository": map[string]any{
			"name": repo,
			"owner": map[string]any{
				"login": owner,
			},
		},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return data
}

// labeledPayload builds a minimal GitHub issues webhook payload (unsupported event).
func labeledPayload(t *testing.T) []byte {
	t.Helper()
	payload := map[string]any{
		"action": "labeled",
		"issue":  map[string]any{"number": 1},
	}
	data, err := json.Marshal(payload)
	require.NoError(t, err)
	return data
}

// signPayload computes the HMAC-SHA256 signature GitHub uses.
func signPayload(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return fmt.Sprintf("sha256=%s", hex.EncodeToString(mac.Sum(nil)))
}

// buildRequest creates an httptest request with the given payload and headers.
func buildRequest(t *testing.T, eventType string, payload []byte, signature string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Github-Event", eventType)
	req.Header.Set("X-Hub-Signature-256", signature)
	return req
}

// setupBusWithStream creates a NATS bus with a JetStream stream covering the webhook subjects.
func setupBusWithStream(t *testing.T) *internalnats.Bus {
	t.Helper()
	bus, _, _ := testutil.StartNATS(t)
	ctx := context.Background()
	err := bus.EnsureStream(ctx, testStreamName, []string{
		subjects.WebhookPRChanged,
		subjects.WebhookPRClosed,
	})
	require.NoError(t, err, "failed to create NATS stream")
	return bus
}

// receiveOnSubject sets up a consumer on the given subject and returns a channel
// that will receive at most one message payload. The consumer stops after the
// test ends via t.Cleanup.
//
// A ready channel is used to block until the goroutine has registered the

// TestWebhook_ValidSignature_MessagePublished verifies that a valid webhook with
// a supported event action publishes a message to the NATS subject.
func TestWebhook_ValidSignature_MessagePublished(t *testing.T) {
	bus := setupBusWithStream(t)
	msgCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)

	cfg := config.WebhookConfig{
		Secret: testSecret,
		AllowedRepos: []config.GitRepoFilter{
			{Owner: testOwner, Repo: testRepo},
		},
	}

	payload := prPayload(t, "opened", testOwner, testRepo)
	sig := signPayload(testSecret, payload)
	req := buildRequest(t, "pull_request", payload, sig)
	rr := httptest.NewRecorder()

	handler := newWebhookHandler(cfg, testutil.NoopLogger(), bus)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	select {
	case msg := <-msgCh:
		assert.NotEmpty(t, msg, "expected a non-empty NATS message payload")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for NATS message")
	}
}

// TestWebhook_InvalidSignature_Returns401 verifies that a request with an
// invalid HMAC signature is rejected with HTTP 401.
func TestWebhook_InvalidSignature_Returns401(t *testing.T) {
	bus := setupBusWithStream(t)
	msgCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)

	cfg := config.WebhookConfig{
		Secret: testSecret,
		AllowedRepos: []config.GitRepoFilter{
			{Owner: testOwner, Repo: testRepo},
		},
	}

	payload := prPayload(t, "opened", testOwner, testRepo)
	badSig := signPayload("wrong-secret", payload)
	req := buildRequest(t, "pull_request", payload, badSig)
	rr := httptest.NewRecorder()

	handler := newWebhookHandler(cfg, testutil.NoopLogger(), bus)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)

	select {
	case <-msgCh:
		t.Fatal("expected no NATS message to be published for invalid signature")
	case <-time.After(200 * time.Millisecond):
		// no message received — correct behaviour
	}
}

// TestWebhook_UnlistedRepo_NoMessagePublished verifies that a PR event for a
// repository not in the configured allow-list is silently dropped.
func TestWebhook_UnlistedRepo_NoMessagePublished(t *testing.T) {
	bus := setupBusWithStream(t)
	msgCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)

	cfg := config.WebhookConfig{
		Secret: testSecret,
		AllowedRepos: []config.GitRepoFilter{
			{Owner: "other-org", Repo: "other-repo"},
		},
	}

	payload := prPayload(t, "opened", testOwner, testRepo)
	sig := signPayload(testSecret, payload)
	req := buildRequest(t, "pull_request", payload, sig)
	rr := httptest.NewRecorder()

	handler := newWebhookHandler(cfg, testutil.NoopLogger(), bus)
	handler.ServeHTTP(rr, req)

	// Handler still returns 200 — the event is silently dropped
	assert.Equal(t, http.StatusOK, rr.Code)

	select {
	case <-msgCh:
		t.Fatal("expected no NATS message for unlisted repo")
	case <-time.After(200 * time.Millisecond):
		// no message received — correct behaviour
	}
}

// TestWebhook_UnsupportedEventType_NoMessagePublished verifies that a
// non-pull_request event (e.g. "issues" with action "labeled") is ignored.
func TestWebhook_UnsupportedEventType_NoMessagePublished(t *testing.T) {
	bus := setupBusWithStream(t)
	msgCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)

	cfg := config.WebhookConfig{
		Secret: testSecret,
		AllowedRepos: []config.GitRepoFilter{
			{Owner: testOwner, Repo: testRepo},
		},
	}

	payload := labeledPayload(t)
	sig := signPayload(testSecret, payload)
	req := buildRequest(t, "issues", payload, sig)
	rr := httptest.NewRecorder()

	handler := newWebhookHandler(cfg, testutil.NoopLogger(), bus)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	select {
	case <-msgCh:
		t.Fatal("expected no NATS message for unsupported event type")
	case <-time.After(200 * time.Millisecond):
		// no message received — correct behaviour
	}
}

// TestWebhook_ClosedAction_PublishesToPRClosed verifies that a "closed" action
// publishes to WebhookPRClosed and does NOT publish to WebhookPRChanged.
func TestWebhook_ClosedAction_PublishesToPRClosed(t *testing.T) {
	bus := setupBusWithStream(t)
	closedCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRClosed)
	changedCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)

	cfg := config.WebhookConfig{
		Secret: testSecret,
		AllowedRepos: []config.GitRepoFilter{
			{Owner: testOwner, Repo: testRepo},
		},
	}

	payload := prPayload(t, "closed", testOwner, testRepo)
	sig := signPayload(testSecret, payload)
	req := buildRequest(t, "pull_request", payload, sig)
	rr := httptest.NewRecorder()

	handler := newWebhookHandler(cfg, testutil.NoopLogger(), bus)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	select {
	case msg := <-closedCh:
		assert.NotEmpty(t, msg, "expected a non-empty NATS message on PRClosed subject")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for NATS message on PRClosed subject")
	}

	select {
	case <-changedCh:
		t.Fatal("expected no NATS message on PRChanged subject for closed action")
	case <-time.After(200 * time.Millisecond):
		// no message received — correct behaviour
	}
}

// TestWebhook_ReopenedAction_PublishesToPRChanged verifies that a "reopened"
// action publishes to WebhookPRChanged.
func TestWebhook_ReopenedAction_PublishesToPRChanged(t *testing.T) {
	bus := setupBusWithStream(t)
	msgCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)

	cfg := config.WebhookConfig{
		Secret: testSecret,
		AllowedRepos: []config.GitRepoFilter{
			{Owner: testOwner, Repo: testRepo},
		},
	}

	payload := prPayload(t, "reopened", testOwner, testRepo)
	sig := signPayload(testSecret, payload)
	req := buildRequest(t, "pull_request", payload, sig)
	rr := httptest.NewRecorder()

	handler := newWebhookHandler(cfg, testutil.NoopLogger(), bus)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	select {
	case msg := <-msgCh:
		assert.NotEmpty(t, msg, "expected a non-empty NATS message payload")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for NATS message")
	}
}

// TestWebhook_SynchronizeAction_PublishesToPRChanged verifies that a
// "synchronize" action publishes to WebhookPRChanged.
func TestWebhook_SynchronizeAction_PublishesToPRChanged(t *testing.T) {
	bus := setupBusWithStream(t)
	msgCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)

	cfg := config.WebhookConfig{
		Secret: testSecret,
		AllowedRepos: []config.GitRepoFilter{
			{Owner: testOwner, Repo: testRepo},
		},
	}

	payload := prPayload(t, "synchronize", testOwner, testRepo)
	sig := signPayload(testSecret, payload)
	req := buildRequest(t, "pull_request", payload, sig)
	rr := httptest.NewRecorder()

	handler := newWebhookHandler(cfg, testutil.NoopLogger(), bus)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	select {
	case msg := <-msgCh:
		assert.NotEmpty(t, msg, "expected a non-empty NATS message payload")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for NATS message")
	}
}

// TestWebhook_UnknownAction_NoMessagePublished verifies that an unrecognized
// action (e.g. "edited") does not publish to any NATS subject.
func TestWebhook_UnknownAction_NoMessagePublished(t *testing.T) {
	bus := setupBusWithStream(t)
	changedCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRChanged)
	closedCh := testutil.SubscribeOnceBody(t, bus, subjects.WebhookPRClosed)

	cfg := config.WebhookConfig{
		Secret: testSecret,
		AllowedRepos: []config.GitRepoFilter{
			{Owner: testOwner, Repo: testRepo},
		},
	}

	payload := prPayload(t, "edited", testOwner, testRepo)
	sig := signPayload(testSecret, payload)
	req := buildRequest(t, "pull_request", payload, sig)
	rr := httptest.NewRecorder()

	handler := newWebhookHandler(cfg, testutil.NoopLogger(), bus)
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	select {
	case <-changedCh:
		t.Fatal("expected no NATS message on PRChanged subject for unknown action")
	case <-time.After(200 * time.Millisecond):
		// no message received — correct behaviour
	}

	select {
	case <-closedCh:
		t.Fatal("expected no NATS message on PRClosed subject for unknown action")
	case <-time.After(200 * time.Millisecond):
		// no message received — correct behaviour
	}
}
