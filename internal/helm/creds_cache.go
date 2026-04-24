package helm

import (
	"sync"
)

type CredsCache struct {
	mu    sync.RWMutex
	creds map[string]struct{ Username, Password string }
}

func NewCredsCache() *CredsCache {
	return &CredsCache{
		creds: make(map[string]struct{ Username, Password string }),
	}
}

func (c *CredsCache) Cache(repoUrl, username, password string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creds[repoUrl] = struct{ Username, Password string }{Username: username, Password: password}
}

func (c *CredsCache) GetCreds(repoUrl string) struct{ Username, Password string } {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.creds[repoUrl]
}
