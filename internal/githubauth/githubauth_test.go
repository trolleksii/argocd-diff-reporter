package githubauth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/testutil"
)

func TestNew_PAT_TokenSource(t *testing.T) {
	m, err := New(context.Background(), config.GithubAppConfig{Token: "ghp_test"}, testutil.NoopLogger())
	require.NoError(t, err)

	token, err := m.GetTokenSource().Token()
	require.NoError(t, err)
	assert.Equal(t, "ghp_test", token.AccessToken)
}

func TestNew_PAT_BasicAuth(t *testing.T) {
	m, err := New(context.Background(), config.GithubAppConfig{Token: "ghp_test"}, testutil.NoopLogger())
	require.NoError(t, err)

	ba, err := m.GetBasicHTTPAuth()
	require.NoError(t, err)
	assert.Equal(t, "x-access-token", ba.Username)
	assert.Equal(t, "ghp_test", ba.Password)
}
