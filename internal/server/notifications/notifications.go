package notifications

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// NewRouteFunc returns a function that registers the webhook handler on the provided mux.
func NewRouteFunc(notifier *NotificationServer) func(*http.ServeMux, *slog.Logger) {
	return func(mux *http.ServeMux, log *slog.Logger) {
		mux.Handle("GET /ws/updates/{id...}", notifier)
	}
}

// NotificationServer handles WebSocket connections for PR report updates
type NotificationServer struct {
	log         *slog.Logger
	subscribers map[string][]*websocket.Conn // PR number -> connections
	mu          sync.RWMutex
	upgrader    websocket.Upgrader
}

// NewManager creates a new WebSocket manager
func NewNotificationServer(log *slog.Logger) *NotificationServer {
	return &NotificationServer{
		log:         log.With("module", "server", "handler", "notification"),
		subscribers: make(map[string][]*websocket.Conn),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				// Allow all origins for now - in production, you should validate origins
				return true
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
	}
}

func (m *NotificationServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		m.log.Error("failed to upgrade connection to WebSocket", "error", err)
		return
	}
	m.subscribe(id, conn)
	go m.handleConnection(conn, id)
}

// addConnection adds a new connection to the manager
func (m *NotificationServer) subscribe(id string, conn *websocket.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribers[id] = append(m.subscribers[id], conn)
}

func (m *NotificationServer) unsubscribe(id string, conn *websocket.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	subscribers := m.subscribers[id]
	for i, c := range subscribers {
		if c == conn {
			m.subscribers[id] = append(subscribers[:i], subscribers[i+1:]...)
			break
		}
	}

	if len(m.subscribers[id]) == 0 {
		delete(m.subscribers, id)
	}
}

func (m *NotificationServer) handleConnection(conn *websocket.Conn, id string) {
	defer func() {
		m.unsubscribe(id, conn)
		conn.Close()
	}()

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Start ping ticker
	ticker := time.NewTicker(54 * time.Second)
	defer ticker.Stop()

	// Handle ping/pong and connection management
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			select {
			case <-ticker.C:
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					m.log.Error("failed to send ping", "error", err)
					cancel()
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				m.log.Error("websocket connection error", "error", err)
			}
			break
		}
	}
}

func (m *NotificationServer) Notify(subscriber string, data any) {
	m.mu.RLock()
	subscribers := make([]*websocket.Conn, len(m.subscribers[subscriber]))
	copy(subscribers, m.subscribers[subscriber])
	m.mu.RUnlock()

	if len(subscribers) == 0 {
		return
	}

	encodedData, err := json.Marshal(data)
	if err != nil {
		m.log.Error("failed to marshal websocket message", "error", err)
		return
	}

	for _, conn := range subscribers {
		go func(c *websocket.Conn) {
			c.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.WriteMessage(websocket.TextMessage, encodedData); err != nil {
				m.log.Error("failed to send websocket message", "error", err, "id", subscriber)
			}
		}(conn)
	}
}
