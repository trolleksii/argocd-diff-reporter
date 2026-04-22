package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeConfig writes yaml content to a temp file and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// ---------------------------------------------------------------------------
// Load
// ---------------------------------------------------------------------------

func TestLoad_ValidMinimalConfig(t *testing.T) {
	yaml := `
github:
  appId: 42
  installationId: 7
  privateKey: "test-key"
`
	cfg, err := Load(writeConfig(t, yaml))
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Required fields populated
	assert.Equal(t, int64(42), cfg.Github.AppID)
	assert.Equal(t, int64(7), cfg.Github.InstallationID)
	assert.Equal(t, "test-key", cfg.Github.PrivateKey)

	// Defaults applied for unspecified fields
	assert.Equal(t, "all", cfg.Target)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "text", cfg.Log.Format)
	assert.Equal(t, "0.0.0.0:8000", cfg.Server.Addr)
	assert.Equal(t, "repositories", cfg.Workers.GitWorker.CloneBaseDir)
	assert.Equal(t, "snapshots", cfg.Workers.GitWorker.SnapshotBaseDir)
	assert.Equal(t, "charts", cfg.Workers.HelmWorker.ChartCacheDir)
	assert.Equal(t, "argocd-diff-reporter", cfg.Tracing.Service)
}

func TestLoad_YAMLOverridesDefaults(t *testing.T) {
	yaml := `
targetModule: "git"
log:
  level: "debug"
  format: "json"
server:
  addr: "127.0.0.1:9090"
workers:
  gitWorker:
    cloneBaseDir: "my-repos"
    snapshotBaseDir: "my-snaps"
  helmWorker:
    chartCacheDir: "my-charts"
tracing:
  service: "custom-service"
github:
  appId: 1
  installationId: 2
  privateKey: "key"
`
	cfg, err := Load(writeConfig(t, yaml))
	require.NoError(t, err)

	assert.Equal(t, "git", cfg.Target)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
	assert.Equal(t, "127.0.0.1:9090", cfg.Server.Addr)
	assert.Equal(t, "my-repos", cfg.Workers.GitWorker.CloneBaseDir)
	assert.Equal(t, "my-snaps", cfg.Workers.GitWorker.SnapshotBaseDir)
	assert.Equal(t, "my-charts", cfg.Workers.HelmWorker.ChartCacheDir)
	assert.Equal(t, "custom-service", cfg.Tracing.Service)
}

func TestLoad_InvalidYAML(t *testing.T) {
	_, err := Load(writeConfig(t, "not: valid: yaml: ["))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse config")
}

func TestLoad_MissingAppID(t *testing.T) {
	yaml := `
github:
  installationId: 7
  privateKey: "test-key"
`
	_, err := Load(writeConfig(t, yaml))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Application ID")
}

func TestLoad_MissingInstallationID(t *testing.T) {
	yaml := `
github:
  appId: 42
  privateKey: "test-key"
`
	_, err := Load(writeConfig(t, yaml))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Installation ID")
}

func TestLoad_MissingPrivateKey(t *testing.T) {
	yaml := `
github:
  appId: 42
  installationId: 7
`
	_, err := Load(writeConfig(t, yaml))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Private Key")
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read config")
}

// ---------------------------------------------------------------------------
// ApplyEnv
// ---------------------------------------------------------------------------

func TestApplyEnv_OverridesGithubFields(t *testing.T) {
	t.Setenv("GITHUB_APP_ID", "100")
	t.Setenv("GITHUB_INSTALLATION_ID", "200")
	t.Setenv("GITHUB_APP_PRIVATE_KEY", "env-key")

	cfg := &Config{}
	require.NoError(t, cfg.ApplyEnv())

	assert.Equal(t, int64(100), cfg.Github.AppID)
	assert.Equal(t, int64(200), cfg.Github.InstallationID)
	assert.Equal(t, "env-key", cfg.Github.PrivateKey)
}

func TestApplyEnv_OverridesTracingFields(t *testing.T) {
	t.Setenv("OTEL_ENDPOINT", "http://otel.example.com")
	t.Setenv("OTEL_SERVICE", "my-service")
	t.Setenv("OTEL_VERSION", "v1.2.3")

	cfg := &Config{}
	require.NoError(t, cfg.ApplyEnv())

	assert.Equal(t, "http://otel.example.com", cfg.Tracing.Endpoint)
	assert.Equal(t, "my-service", cfg.Tracing.Service)
	assert.Equal(t, "v1.2.3", cfg.Tracing.Version)
}

func TestApplyEnv_InvalidAppIDSilentlyIgnored(t *testing.T) {
	t.Setenv("GITHUB_APP_ID", "not-a-number")

	cfg := &Config{
		Github: GithubAppConfig{AppID: 99},
	}
	require.NoError(t, cfg.ApplyEnv())

	// Original value preserved; invalid env var silently ignored
	assert.Equal(t, int64(99), cfg.Github.AppID)
}

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

func TestValidate_ZeroAppID(t *testing.T) {
	cfg := &Config{
		Github: GithubAppConfig{AppID: 0, InstallationID: 1, PrivateKey: "key"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Application ID")
}

func TestValidate_ZeroInstallationID(t *testing.T) {
	cfg := &Config{
		Github: GithubAppConfig{AppID: 1, InstallationID: 0, PrivateKey: "key"},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Installation ID")
}

func TestValidate_EmptyPrivateKey(t *testing.T) {
	cfg := &Config{
		Github: GithubAppConfig{AppID: 1, InstallationID: 2, PrivateKey: ""},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Private Key")
}

func TestValidate_AllFieldsSet_NoError(t *testing.T) {
	cfg := &Config{
		Github: GithubAppConfig{AppID: 1, InstallationID: 2, PrivateKey: "key"},
	}
	assert.NoError(t, cfg.Validate())
}
