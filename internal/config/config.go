package config

import (
	"fmt"
	"os"
	"strconv"

	"sigs.k8s.io/yaml"
)

type Config struct {
	Target  string          `yaml:"targetModule"`
	Nats    NatsConfig      `yaml:"nats"`
	Server  ServerConfig    `yaml:"server"`
	Github  GithubAppConfig `yaml:"github"`
	Log     LogConfig       `yaml:"log"`
	Webhook WebhookConfig   `yaml:"webhook"`
	Workers WorkersConfig   `yaml:"workers"`
	ArgoCD  ArgoCDConfig    `yaml:"argocd"`
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
	GitWorker  GitWorkerConfig  `yaml:"gitWorker"`
	HelmWorker HelmWorkerConfig `yaml:"helmWorker"`
}

type GitWorkerConfig struct {
	CloneBaseDir    string   `yaml:"cloneBaseDir"`
	SnapshotBaseDir string   `yaml:"snapshotBaseDir"`
	FileGlobs       []string `yaml:"fileGlobs"`
}

type HelmWorkerConfig struct {
	ChartCacheDir string `yaml:"chartCacheDir"`
}

type NatsConfig struct {
	Addr string `yaml:"addr"`
	// or
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

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	cfg := &Config{
		Target: "all",
		Log:    LogConfig{Level: "info", Format: "text"},
		Server: ServerConfig{Addr: "0.0.0.0:8000"},
		Workers: WorkersConfig{
			GitWorker: GitWorkerConfig{
				CloneBaseDir:    "repositories",
				SnapshotBaseDir: "snapshots",
			},
			HelmWorker: HelmWorkerConfig{
				ChartCacheDir: "charts",
			},
		},
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := cfg.ApplyEnv(); err != nil {
		return nil, fmt.Errorf("invalid environment variable: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
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

	if c.Target != "all" && c.Nats.Addr == "" {
		return fmt.Errorf("NATS address is required")
	}
	return nil
}
