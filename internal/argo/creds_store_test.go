package argo

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

func TestCredsStore_GetLoadsOnMissAndCaches(t *testing.T) {
	loaderCalls := 0
	s := NewCredsStore(func(ctx context.Context, repoURL, project string) (models.Creds, error) {
		loaderCalls++
		return models.Creds{Username: "user", Password: "pass"}, nil
	})

	ctx := context.Background()
	p := s.Scoped("default")
	c, err := p.Get(ctx, "https://charts.example.com")
	require.NoError(t, err)
	assert.Equal(t, models.Creds{Username: "user", Password: "pass"}, c)
	assert.Equal(t, 1, loaderCalls, "cache miss should hit the loader")

	c, err = p.Get(ctx, "https://charts.example.com")
	require.NoError(t, err)
	assert.Equal(t, models.Creds{Username: "user", Password: "pass"}, c)
	assert.Equal(t, 1, loaderCalls, "cache hit should not hit the loader again")
}

func TestCredsStore_RefreshForcesReload(t *testing.T) {
	password := "old"
	s := NewCredsStore(func(ctx context.Context, repoURL, project string) (models.Creds, error) {
		return models.Creds{Username: "user", Password: password}, nil
	})

	ctx := context.Background()
	p := s.Scoped("default")
	c, err := p.Get(ctx, "https://charts.example.com")
	require.NoError(t, err)
	assert.Equal(t, "old", c.Password)

	password = "new"
	c, err = p.Refresh(ctx, "https://charts.example.com")
	require.NoError(t, err)
	assert.Equal(t, "new", c.Password, "Refresh should bypass the cache and reload")

	c, err = p.Get(ctx, "https://charts.example.com")
	require.NoError(t, err)
	assert.Equal(t, "new", c.Password, "Refresh should update the cached entry")
}

func TestCredsStore_LoaderErrorPropagates(t *testing.T) {
	loaderErr := errors.New("argocd api unavailable")
	s := NewCredsStore(func(ctx context.Context, repoURL, project string) (models.Creds, error) {
		return models.Creds{}, loaderErr
	})

	_, err := s.Scoped("default").Get(context.Background(), "https://charts.example.com")
	assert.ErrorIs(t, err, loaderErr, "loader errors must surface, not degrade to anonymous")
}

func TestCredsStore_NilLoaderReturnsEmptyCreds(t *testing.T) {
	s := NewCredsStore(nil)
	c, err := s.Scoped("default").Get(context.Background(), "https://charts.example.com")
	require.NoError(t, err)
	assert.Equal(t, models.Creds{}, c)
}

func TestCredsStore_KeyNormalization(t *testing.T) {
	loaderCalls := 0
	s := NewCredsStore(func(ctx context.Context, repoURL, project string) (models.Creds, error) {
		loaderCalls++
		return models.Creds{Username: "user", Password: "pass"}, nil
	})

	ctx := context.Background()
	p := s.Scoped("default")
	_, err := p.Get(ctx, "oci://registry.example.com/charts")
	require.NoError(t, err)

	// Schemeless (as stored in ArgoCD repo secrets) and trailing-slash
	// variants must resolve to the same cached entry.
	for _, variant := range []string{
		"registry.example.com/charts",
		"registry.example.com/charts/",
		" oci://registry.example.com/charts ",
	} {
		_, err := p.Get(ctx, variant)
		require.NoError(t, err)
	}
	assert.Equal(t, 1, loaderCalls, "all URL variants should share one cache entry")
}

func TestCredsStore_ProjectsAreIsolated(t *testing.T) {
	s := NewCredsStore(func(ctx context.Context, repoURL, project string) (models.Creds, error) {
		return models.Creds{Username: project, Password: "pass"}, nil
	})

	ctx := context.Background()
	a, err := s.Scoped("team-a").Get(ctx, "https://charts.example.com")
	require.NoError(t, err)
	b, err := s.Scoped("team-b").Get(ctx, "https://charts.example.com")
	require.NoError(t, err)

	assert.Equal(t, "team-a", a.Username)
	assert.Equal(t, "team-b", b.Username, "same repo URL in different projects must not collide")
}
