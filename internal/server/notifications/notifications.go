package notifications

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait   = 10 * time.Second
	pongWait    = 60 * time.Second
	pingPeriod  = 54 * time.Second
	sendBufSize = 32
)

// NewRouteFunc returns a function that registers the webhook handler on the provided mux.
func NewRouteFunc(notifier *NotificationServer) func(*http.ServeMux, *slog.Logger) {
	return func(mux *http.ServeMux, log *slog.Logger) {
		mux.Handle("GET /ws/updates/{id...}", notifier)
	}
}

type client struct {
	conn *websocket.Conn
	send chan []byte
}

// NotificationServer handles WebSocket connections for PR report updates
type NotificationServer struct {
	log         *slog.Logger
	subscribers map[string][]*client
	mu          sync.RWMutex
	upgrader    websocket.Upgrader
}

// NewNotificationServer creates a new WebSocket manager
func NewNotificationServer(log *slog.Logger) *NotificationServer {
	return &NotificationServer{
		log:         log.With("module", "server", "handler", "notification"),
		subscribers: make(map[string][]*client),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
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
	c := &client{
		conn: conn,
		send: make(chan []byte, sendBufSize),
	}
	m.subscribe(id, c)
	go m.writePump(c, id)
	go m.readPump(c, id)
}

func (m *NotificationServer) subscribe(id string, c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribers[id] = append(m.subscribers[id], c)
}

func (m *NotificationServer) unsubscribe(id string, c *client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	subscribers := m.subscribers[id]
	for i, sub := range subscribers {
		if sub == c {
			m.subscribers[id] = append(subscribers[:i], subscribers[i+1:]...)
			break
		}
	}

	if len(m.subscribers[id]) == 0 {
		delete(m.subscribers, id)
	}
}

func (m *NotificationServer) readPump(c *client, id string) {
	defer func() {
		m.unsubscribe(id, c)
		c.conn.Close()
		close(c.send)
	}()

	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				m.log.Error("websocket connection error", "error", err)
			}
			return
		}
	}
}

func (m *NotificationServer) writePump(c *client, id string) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				c.conn.WriteControl(websocket.CloseMessage, nil, time.Now().Add(writeWait))
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				m.log.Error("failed to send websocket message", "error", err, "id", id)
				return
			}
		case <-ticker.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				m.log.Error("failed to send ping", "error", err)
				return
			}
		}
	}
}

func (m *NotificationServer) Notify(subscriber string, data any) {
	m.mu.RLock()
	subscribers := make([]*client, len(m.subscribers[subscriber]))
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

	for _, c := range subscribers {
		select {
		case c.send <- encodedData:
		default:
			m.log.Warn("dropping websocket message, client send buffer full", "id", subscriber)
		}
	}
}
