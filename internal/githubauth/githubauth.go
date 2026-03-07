package githubauth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/google/go-github/v74/github"
	"github.com/jferrl/go-githubauth"
	"golang.org/x/oauth2"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
)

type GithubCredManager struct {
	tokenSource oauth2.TokenSource
	basicAuth   *githttp.BasicAuth
	log         *slog.Logger
	mu          sync.RWMutex
}

func New(ctx context.Context, log *slog.Logger, cfg config.GithubAppConfig) (*GithubCredManager, error) {
	appTokenSource, err := githubauth.NewApplicationTokenSource(cfg.AppID, []byte(cfg.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("failed to create application token source: %w", err)
	}

	installationTokenSource := githubauth.NewInstallationTokenSource(cfg.InstallationID, appTokenSource)
	m := &GithubCredManager{
		tokenSource: installationTokenSource,
		log:         log.With("module", "githubauth"),
	}
	initialDelay, err := m.refreshToken()
	if err != nil {
		return nil, err
	}
	go m.refreshLoop(ctx, initialDelay)
	return m, nil
}

// NewAuthenticatedClient returns a ready to used authenticated Github API client
func NewAuthenticatedClient(ctx context.Context, auth *GithubCredManager) *github.Client {
	httpClient := oauth2.NewClient(ctx, auth.tokenSource)
	return github.NewClient(httpClient)
}

func (a *GithubCredManager) GetBasicHTTPAuth() (*githttp.BasicAuth, error) {
	a.mu.RLock()
	ba := a.basicAuth
	a.mu.RUnlock()
	if ba == nil {
		return ba, errors.New("failed to fetch a valid credentials")
	}
	return ba, nil
}

// refreshLoop runs in background and refreshes tokens proactively
func (a *GithubCredManager) refreshLoop(ctx context.Context, interval time.Duration) {
	var err error
	for {
		if interval <= 0 {
			interval = 10 * time.Second
		}

		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
		interval, err = a.refreshToken()
		if err != nil {
			a.log.Error("Failed to refresh a token", "error", err)
		}
		a.log.Debug("fetched fresh token")
	}
}

// getRefreshInterval calculates a refresh interval of a OAuth2 token
func getRefreshInterval(t *oauth2.Token) time.Duration {
	timeUntilExpiry := time.Until(t.Expiry)
	refreshAfterDuration := timeUntilExpiry * 3 / 4
	return refreshAfterDuration
}

// refreshToken proactively refreshes the token
func (a *GithubCredManager) refreshToken() (time.Duration, error) {
	token, err := a.tokenSource.Token()
	if err != nil {
		a.mu.Lock()
		a.basicAuth = nil
		a.mu.Unlock()
		return 0, err
	}
	interval := getRefreshInterval(token)

	a.mu.Lock()
	a.basicAuth = &githttp.BasicAuth{
		Username: "x-access-token",
		Password: token.AccessToken,
	}
	a.mu.Unlock()
	return interval, nil
}
