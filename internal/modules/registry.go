package modules

import (
	"fmt"
	"sync"
)

type Registry struct {
	mu       sync.RWMutex
	services map[string]any
}

func NewRegistry() *Registry {
	return &Registry{
		services: make(map[string]any),
	}
}

func (r *Registry) Set(key string, v any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.services[key] = v
}

func (r *Registry) Get(key string) any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if s, ok := r.services[key]; ok {
		return s
	}
	return nil
}

func Get[T any](r *Registry, key string) (T, error) {
	val := r.Get(key)
	if val == nil {
		var zero T
		return zero, fmt.Errorf("registry: %s not found", key)
	}
	t, ok := val.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("registry: %s wrong type", key)
	}
	return t, nil
}
