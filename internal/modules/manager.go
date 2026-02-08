package modules

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// Service is what each module's init function returns.
type Service interface {
	Run(ctx context.Context) error
}

// InitFunc creates a service. It's called only when the module is needed.
type InitFunc func(r *Registry) (Service, error)

type module struct {
	name string
	init InitFunc
	deps []string
}

type Manager struct {
	modules  map[string]*module
	targets  []string
	registry *Registry
}

func NewManager() *Manager {
	return &Manager{modules: make(map[string]*module)}
}

// Register adds a module with its init function and dependencies.
func (m *Manager) Register(name string, init InitFunc, deps ...string) {
	m.targets = append(m.targets, name)
	m.modules[name] = &module{name: name, init: init, deps: deps}
}

// Run resolves dependencies for the given targets, initializes them in
// topological order, and runs all resulting services concurrently.
func (m *Manager) Run(ctx context.Context) error {
	needed, err := m.resolve(m.targets)
	if err != nil {
		return err
	}

	var services []Service
	for _, name := range needed {
		mod := m.modules[name]
		svc, err := mod.init(m.registry)
		if err != nil {
			return fmt.Errorf("init %s: %w", name, err)
		}
		if svc != nil {
			services = append(services, svc)
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, svc := range services {
		g.Go(func() error { return svc.Run(gCtx) })
	}
	return g.Wait()
}

// resolve does a topological sort via DFS, returning init order.
func (m *Manager) resolve(targets []string) ([]string, error) {
	var order []string
	state := make(map[string]int) // 0=unvisited, 1=visiting, 2=done

	var visit func(name string) error
	visit = func(name string) error {
		if state[name] == 2 {
			return nil
		}
		if state[name] == 1 {
			return fmt.Errorf("circular dependency: %s", name)
		}

		mod, ok := m.modules[name]
		if !ok {
			return fmt.Errorf("unknown module: %s", name)
		}

		state[name] = 1
		for _, dep := range mod.deps {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[name] = 2
		order = append(order, name)
		return nil
	}

	for _, t := range targets {
		if err := visit(t); err != nil {
			return nil, err
		}
	}
	return order, nil
}
