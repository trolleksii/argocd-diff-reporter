package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Global  GlobalConfig    `yaml:"global"`
	Nats    NatsConfig      `yaml:"nats"`
	Server  ServerConfig    `yaml:"server"`
	Github  GithubAppConfig `yaml:"github"`
	Log     LogConfig       `yaml:"log"`
	Webhook WebhookConfig   `yaml:"webhook"`
	Workers WorkersConfig   `yaml:"workers"`
	ArgoCD  ArgoCDConfig    `yaml:"argocd"`
	Tracing TracingConfig   `yaml:"tracing"`
}

type GlobalConfig struct {
	StorageDir string `yaml:"storageDir"`
}

type ArgoCDConfig struct {
	Namespace            string `yaml:"namespace"`
	RepoServerAddr       string `yaml:"repoServerAddr"`
	RepoServerTimeoutSec int    `yaml:"repoServerTimeoutSec"`
}

type ServerConfig struct {
	Addr string `yaml:"addr"`
}

type WebhookConfig struct {
	Secret       string          `yaml:"secret"`
	AllowedRepos []GitRepoFilter `yaml:"repositories"`
}

type GitRepoFilter struct {
	Owner string `yaml:"owner"`
	Repo  string `yaml:"repo"`
}

type WorkersConfig struct {
	GitWorker    GitWorkerConfig    `yaml:"gitWorker"`
	HelmWorker   HelmWorkerConfig   `yaml:"helmWorker"`
	Coordinator  CoordinatorConfig  `yaml:"coordinator"`
	GithubChecks GithubChecksConfig `yaml:"githubChecks"`
}

type GitWorkerConfig struct {
	CloneBaseDir    string   `yaml:"cloneBaseDir"`
	SnapshotBaseDir string   `yaml:"snapshotBaseDir"`
	FileGlobs       []string `yaml:"fileGlobs"`
}

type HelmWorkerConfig struct {
	ChartCacheDir string `yaml:"chartCacheDir"`
}

type CoordinatorConfig struct {
	IndexCapacity int `yaml:"indexCapacity"`
}

type GithubChecksConfig struct {
	UIBaseURL string `yaml:"uiBaseUrl"`
}

type NatsConfig struct {
	Domain     string `yaml:"domain"`
	ServerName string `yaml:"serverName"`
	StoreDir   string `yaml:"storeDir"`
}

type GithubAppConfig struct {
	AppID          int64  `yaml:"appId"`
	InstallationID int64  `yaml:"installationId"`
	PrivateKey     string `yaml:"privateKey"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type TracingConfig struct {
	Endpoint string `yaml:"endpoint"`
	Service  string `yaml:"service"`
	Version  string `yaml:"version"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{
		Global: GlobalConfig{StorageDir: "."},
		Log:    LogConfig{Level: "info", Format: "text"},
		Server: ServerConfig{Addr: "0.0.0.0:8000"},
		Workers: WorkersConfig{
			Coordinator: CoordinatorConfig{
				IndexCapacity: 10,
			},
		},
		Tracing: TracingConfig{
			Service: "argocd-diff-reporter",
		},
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.ApplyEnv(); err != nil {
		return nil, fmt.Errorf("invalid environment variable: %w", err)
	}
	cfg.Expand()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func (c *Config) Expand() {
	if c.Nats.StoreDir == "" {
		c.Nats.StoreDir = filepath.Join(c.Global.StorageDir, "nats")
	}
	if c.Workers.HelmWorker.ChartCacheDir == "" {
		c.Workers.HelmWorker.ChartCacheDir = filepath.Join(c.Global.StorageDir, "helm")
	}
	if c.Workers.GitWorker.CloneBaseDir == "" {
		c.Workers.GitWorker.CloneBaseDir = filepath.Join(c.Global.StorageDir, "repositories")
	}
	if c.Workers.GitWorker.SnapshotBaseDir == "" {
		c.Workers.GitWorker.SnapshotBaseDir = filepath.Join(c.Global.StorageDir, "snapshots")
	}
}

func (c *Config) ApplyEnv() error {
	if v := os.Getenv("GITHUB_APP_ID"); v != "" {
		if i64v, err := strconv.ParseInt(v, 10, 0); err == nil {
			c.Github.AppID = i64v
		}
	}
	if v := os.Getenv("GITHUB_INSTALLATION_ID"); v != "" {
		if i64v, err := strconv.ParseInt(v, 10, 0); err == nil {
			c.Github.InstallationID = i64v
		}
	}
	if v := os.Getenv("GITHUB_APP_PRIVATE_KEY"); v != "" {
		c.Github.PrivateKey = v
	}
	if v := os.Getenv("GITHUB_WEBHOOK_SECRET"); v != "" {
		c.Webhook.Secret = v
	}
	if v := os.Getenv("OTEL_ENDPOINT"); v != "" {
		c.Tracing.Endpoint = v
	}
	if v := os.Getenv("OTEL_SERVICE"); v != "" {
		c.Tracing.Service = v
	}
	if v := os.Getenv("OTEL_VERSION"); v != "" {
		c.Tracing.Version = v
	}
	return nil
}

func (c *Config) Validate() error {
	if c.Github.AppID == 0 {
		return fmt.Errorf("Github Application ID is required")
	}
	if c.Github.InstallationID == 0 {
		return fmt.Errorf("Github Installation ID is required")
	}
	if c.Github.PrivateKey == "" {
		return fmt.Errorf("Github Application Private Key is required")
	}
	return nil
}
