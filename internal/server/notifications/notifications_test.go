package notifications

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

// newTestServer creates a httptest.Server backed by a ServeMux that routes
// WebSocket upgrade requests through the NotificationServer. Using a real mux
// with the same pattern as production ensures r.PathValue("id") is populated.
func newTestServer(t *testing.T, ns *NotificationServer) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /ws/updates/{id...}", ns)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// dialWS opens a WebSocket connection to srv at the given path and returns the
// connection. The connection is automatically closed when the test ends.
func dialWS(t *testing.T, srv *httptest.Server, path string) *websocket.Conn {
	t.Helper()
	url := "ws" + srv.URL[len("http"):] + path
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	require.NoError(t, err, "failed to dial WebSocket at %s", url)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// readTextMessage reads a single text frame from conn with a short deadline.
func readTextMessage(t *testing.T, conn *websocket.Conn, timeout time.Duration) []byte {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	_, msg, err := conn.ReadMessage()
	require.NoError(t, err, "failed to read WebSocket message")
	return msg
}

// TestWebSocket_Connect verifies that a client can establish a WebSocket
// connection to the notifications server without error.
func TestWebSocket_Connect(t *testing.T) {
	ns := NewNotificationServer(testutil.NoopLogger())
	srv := newTestServer(t, ns)

	conn := dialWS(t, srv, "/ws/updates/pr-42")

	// Connection was established; close it gracefully.
	err := conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	assert.NoError(t, err)
}

// TestWebSocket_Notify_BroadcastsToConnectedClients verifies that a Notify
// call sends the message to all clients subscribed to that ID.
func TestWebSocket_Notify_BroadcastsToConnectedClients(t *testing.T) {
	ns := NewNotificationServer(testutil.NoopLogger())
	srv := newTestServer(t, ns)

	const prID = "pr-123"

	conn1 := dialWS(t, srv, "/ws/updates/"+prID)
	conn2 := dialWS(t, srv, "/ws/updates/"+prID)

	// Give the server goroutines a moment to register both subscribers.
	require.Eventually(t, func() bool {
		ns.mu.RLock()
		defer ns.mu.RUnlock()
		return len(ns.subscribers[prID]) == 2
	}, 2*time.Second, 20*time.Millisecond, "timed out waiting for both subscribers to register")

	payload := map[string]string{"status": "complete", "pr": prID}
	ns.Notify(prID, payload)

	// Both connections should receive the broadcast.
	for i, conn := range []*websocket.Conn{conn1, conn2} {
		raw := readTextMessage(t, conn, 2*time.Second)

		var got map[string]string
		err := json.Unmarshal(raw, &got)
		require.NoError(t, err, "client %d: failed to unmarshal message", i+1)
		assert.Equal(t, "complete", got["status"], "client %d: unexpected status", i+1)
		assert.Equal(t, prID, got["pr"], "client %d: unexpected pr", i+1)
	}
}

// TestWebSocket_Notify_DifferentIDs verifies that Notify only delivers to
// subscribers of the matching ID, not other IDs.
func TestWebSocket_Notify_DifferentIDs(t *testing.T) {
	ns := NewNotificationServer(testutil.NoopLogger())
	srv := newTestServer(t, ns)

	connA := dialWS(t, srv, "/ws/updates/pr-A")
	connB := dialWS(t, srv, "/ws/updates/pr-B")

	// Wait for both subscribers to be registered before notifying.
	require.Eventually(t, func() bool {
		ns.mu.RLock()
		defer ns.mu.RUnlock()
		return len(ns.subscribers["pr-A"]) == 1 && len(ns.subscribers["pr-B"]) == 1
	}, 2*time.Second, 20*time.Millisecond, "timed out waiting for subscribers to register")

	// Notify only pr-A.
	ns.Notify("pr-A", map[string]string{"target": "A"})

	// connA should receive the message.
	raw := readTextMessage(t, connA, 2*time.Second)
	var gotA map[string]string
	require.NoError(t, json.Unmarshal(raw, &gotA))
	assert.Equal(t, "A", gotA["target"])

	// Verify connB was NOT delivered the pr-A message by sending it a pr-B
	// notification immediately after. If routing is broken and connB wrongly
	// received the pr-A message, it arrives first and gotB["target"] == "A",
	// failing the assertion. This is deterministic: no timing-dependent window.
	ns.Notify("pr-B", map[string]string{"target": "B"})
	raw = readTextMessage(t, connB, 2*time.Second)
	var gotB map[string]string
	require.NoError(t, json.Unmarshal(raw, &gotB))
	assert.Equal(t, "B", gotB["target"], "connB should only receive its own notification, not pr-A's")
}

// TestWebSocket_Notify_NoSubscribers verifies that calling Notify with no
// connected clients is a no-op and does not panic.
func TestWebSocket_Notify_NoSubscribers(t *testing.T) {
	ns := NewNotificationServer(testutil.NoopLogger())

	assert.NotPanics(t, func() {
		ns.Notify("pr-999", map[string]string{"status": "done"})
	})
}

// TestWebSocket_Disconnect_ClientRemovedCleanly verifies that once a client
// closes its connection, the server removes it from the subscriber list and
// subsequent Notify calls do not cause errors.
func TestWebSocket_Disconnect_ClientRemovedCleanly(t *testing.T) {
	ns := NewNotificationServer(testutil.NoopLogger())
	srv := newTestServer(t, ns)

	const prID = "pr-disconnect"
	conn := dialWS(t, srv, "/ws/updates/"+prID)

	// Wait for the subscriber to be registered before reading state.
	require.Eventually(t, func() bool {
		ns.mu.RLock()
		defer ns.mu.RUnlock()
		return len(ns.subscribers[prID]) == 1
	}, 2*time.Second, 20*time.Millisecond, "timed out waiting for subscriber to register")

	ns.mu.RLock()
	count := len(ns.subscribers[prID])
	ns.mu.RUnlock()
	assert.Equal(t, 1, count, "expected one subscriber before disconnect")

	// Close the connection from the client side.
	conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	conn.Close()

	// Wait for the server-side handleConnection goroutine to run unsubscribe.
	require.Eventually(t, func() bool {
		ns.mu.RLock()
		defer ns.mu.RUnlock()
		return len(ns.subscribers[prID]) == 0
	}, 2*time.Second, 20*time.Millisecond, "timed out waiting for subscriber to be removed")

	// Calling Notify after disconnect must not panic.
	assert.NotPanics(t, func() {
		ns.Notify(prID, map[string]string{"status": "after-disconnect"})
	})
}

// TestWebSocket_MultipleConnections_IndependentSubscriptions verifies that
// connections on different IDs are tracked independently.
func TestWebSocket_MultipleConnections_IndependentSubscriptions(t *testing.T) {
	ns := NewNotificationServer(testutil.NoopLogger())
	srv := newTestServer(t, ns)

	_ = dialWS(t, srv, "/ws/updates/pr-1")
	_ = dialWS(t, srv, "/ws/updates/pr-1")
	_ = dialWS(t, srv, "/ws/updates/pr-2")

	// Wait for all three subscribers to register.
	require.Eventually(t, func() bool {
		ns.mu.RLock()
		defer ns.mu.RUnlock()
		return len(ns.subscribers["pr-1"]) == 2 && len(ns.subscribers["pr-2"]) == 1
	}, 2*time.Second, 20*time.Millisecond, "timed out waiting for all subscribers to register")

	ns.mu.RLock()
	pr1Count := len(ns.subscribers["pr-1"])
	pr2Count := len(ns.subscribers["pr-2"])
	ns.mu.RUnlock()

	assert.Equal(t, 2, pr1Count, "expected 2 subscribers for pr-1")
	assert.Equal(t, 1, pr2Count, "expected 1 subscriber for pr-2")
}
