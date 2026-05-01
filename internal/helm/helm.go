package helm

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/client-go/discovery"

	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/cli/values"
	"helm.sh/helm/v3/pkg/downloader"
	"helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/registry"
	"helm.sh/helm/v3/pkg/strvals"
)

const fallbackKubeVersion = "v1.34.2"

type SetParam struct {
	Name        string
	Value       string
	ForceString bool
}

type RenderValues struct {
	Values     map[string]any
	ValueFiles []string
	SetParams  []SetParam
}

func initActionConfig(settings *cli.EnvSettings) (*action.Configuration, error) {
	if settings == nil {
		return nil, fmt.Errorf("CLI settings cannot be nil")
	}
	// Create logger with structured output
	logger := slog.Default()

	actionConfig := new(action.Configuration)
	namespace := settings.Namespace()
	if namespace == "" {
		namespace = "default"
	}

	// Use memory driver for dry-run operations to avoid cluster state pollution
	if err := actionConfig.Init(
		settings.RESTClientGetter(),
		namespace,
		"memory",
		logger.Debug,
	); err != nil {
		return nil, fmt.Errorf("failed to initialize action configuration for namespace %q: %w", namespace, err)
	}

	return actionConfig, nil
}

func createRegistryClient(settings *cli.EnvSettings, logger *slog.Logger, installClient *action.Install) (*registry.Client, error) {
	if settings == nil {
		return nil, fmt.Errorf("CLI settings cannot be nil")
	}
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}
	if installClient == nil {
		return nil, fmt.Errorf("install client cannot be nil")
	}

	// Determine if TLS configuration is needed
	hasTLSConfig := (installClient.CertFile != "" && installClient.KeyFile != "") ||
		installClient.CaFile != "" ||
		installClient.InsecureSkipTLSverify

	if hasTLSConfig {
		return createRegistryClientWithTLS(settings, installClient)
	}

	return createBasicRegistryClient(settings, installClient.PlainHTTP)
}

func createBasicRegistryClient(settings *cli.EnvSettings, plainHTTP bool) (*registry.Client, error) {
	opts := []registry.ClientOption{
		registry.ClientOptDebug(settings.Debug),
		registry.ClientOptEnableCache(true),
		registry.ClientOptWriter(os.Stderr),
	}

	if settings.RegistryConfig != "" {
		opts = append(opts, registry.ClientOptCredentialsFile(settings.RegistryConfig))
	}

	if plainHTTP {
		opts = append(opts, registry.ClientOptPlainHTTP())
	}

	registryClient, err := registry.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create registry client: %w", err)
	}

	return registryClient, nil
}

// createRegistryClientWithTLS creates a registry client with TLS configuration.
func createRegistryClientWithTLS(settings *cli.EnvSettings, installClient *action.Install) (*registry.Client, error) {
	registryClient, err := registry.NewRegistryClientWithTLS(
		io.Discard,
		installClient.CertFile,
		installClient.KeyFile,
		installClient.CaFile,
		installClient.InsecureSkipTLSverify,
		settings.RegistryConfig,
		settings.Debug,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create TLS registry client: %w", err)
	}

	return registryClient, nil
}

func loadAndValidateChart(chartPath string) (*chart.Chart, error) {
	chart, err := loader.Load(chartPath)
	if err != nil {
		return nil, err
	}

	if chart == nil {
		return nil, fmt.Errorf("loaded chart is nil")
	}

	if chart.Metadata == nil {
		return nil, fmt.Errorf("chart metadata is missing")
	}

	if chart.Metadata.Name == "" {
		return nil, fmt.Errorf("chart name is required")
	}

	return chart, nil
}

func resolveDependencies(ctx context.Context, chart *chart.Chart, chartPath string, installClient *action.Install, settings *cli.EnvSettings) error {
	if chart.Metadata.Dependencies == nil {
		return nil
	}

	if err := action.CheckDependencies(chart, chart.Metadata.Dependencies); err != nil {
		slog.Info("Chart dependencies need to be updated", "reason", err)

		manager := &downloader.Manager{
			Out:              io.Discard,
			ChartPath:        chartPath,
			Keyring:          installClient.ChartPathOptions.Keyring,
			SkipUpdate:       false,
			Getters:          getter.All(settings),
			RepositoryConfig: settings.RepositoryConfig,
			RepositoryCache:  settings.RepositoryCache,
			Debug:            settings.Debug,
			RegistryClient:   installClient.GetRegistryClient(),
		}

		if err := updateDependenciesWithContext(ctx, manager); err != nil {
			return fmt.Errorf("failed to update chart dependencies: %w", err)
		}
	}

	return nil
}

func updateDependenciesWithContext(ctx context.Context, manager *downloader.Manager) error {
	// Create a channel to handle the dependency update
	done := make(chan error, 1)

	go func() {
		done <- manager.Update()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type CredsProvider interface {
	GetCreds(string) struct{ Username, Password string }
}

func setupInstallClient(settings *cli.EnvSettings, chartVersion string) *action.Install {
	logger := slog.Default()
	installClient := &action.Install{}
	installClient.Version = chartVersion
	registryClient, _ := createRegistryClient(settings, logger, installClient)
	installClient.SetRegistryClient(registryClient)
	installClient.ChartPathOptions.Version = chartVersion
	return installClient
}

func FetchChartHTTPS(chartRef, chartVersion string, credsProvider CredsProvider, cache *HelmChartCache) (string, error) {
	settings := cli.New()
	installClient := setupInstallClient(settings, chartVersion)
	cacheKey := GenerateCacheKey(chartRef, chartVersion)

	return cache.GetOrFetch(cacheKey, func() (string, error) {
		slog.Debug("helm cache miss", "ref", chartRef)
		u, err := url.Parse(chartRef)
		if err != nil {
			return "", fmt.Errorf("invalid chart URL %q: %w", chartRef, err)
		}

		pathParts := strings.Split(strings.Trim(u.Path, "/"), "/")
		chartName := pathParts[len(pathParts)-1]
		if chartName == "" {
			return "", fmt.Errorf("chart reference %q is missing a chart name", chartRef)
		}

		repoURL := fmt.Sprintf("%s://%s", u.Scheme, u.Host)
		if repoPath := strings.Join(pathParts[:len(pathParts)-1], "/"); repoPath != "" {
			repoURL = fmt.Sprintf("%s/%s", repoURL, repoPath)
		}
		installClient.ChartPathOptions.RepoURL = repoURL

		creds := credsProvider.GetCreds(repoURL)
		installClient.ChartPathOptions.Username = creds.Username
		installClient.ChartPathOptions.Password = creds.Password

		chartPath, err := installClient.ChartPathOptions.LocateChart(chartName, settings)
		if err != nil {
			return "", fmt.Errorf("failed to download HTTP chart: %w", err)
		}

		finalChartPath, err := cacheHelmChart(chartPath, cacheKey, cache)
		if err != nil {
			slog.Error("Failed to cache HTTP chart", "error", err)
			return chartPath, nil
		}
		return finalChartPath, nil
	})
}

func FetchChartOCI(chartRef, chartVersion string, credsProvider CredsProvider, cache *HelmChartCache) (string, error) {
	settings := cli.New()
	installClient := setupInstallClient(settings, chartVersion)
	cacheKey := GenerateCacheKey(chartRef, chartVersion)

	return cache.GetOrFetch(cacheKey, func() (string, error) {
		slog.Debug("helm cache miss", "ref", chartRef)
		if !strings.HasPrefix(chartRef, "oci://") {
			chartRef = "oci://" + chartRef
		}
		u, err := url.Parse(chartRef)
		if err != nil {
			return "", fmt.Errorf("invalid chart URL %q: %w", chartRef, err)
		}

		creds := credsProvider.GetCreds(u.Host)
		installClient.ChartPathOptions.Username = creds.Username
		installClient.ChartPathOptions.Password = creds.Password

		registryClient := installClient.GetRegistryClient()
		if registryClient != nil {
			err := registryClient.Login(chartRef, registry.LoginOptBasicAuth(creds.Username, creds.Password))
			if err != nil {
				slog.Error("OCI registry login failed", "error", err)
			}
		}

		// Use Helm's built-in OCI support to pull the chart
		installClient.ChartPathOptions.Version = chartVersion
		chartPath, err := installClient.ChartPathOptions.LocateChart(chartRef, settings)
		if err != nil {
			return "", fmt.Errorf("failed to pull OCI chart: %w", err)
		}

		finalChartPath, err := cacheHelmChart(chartPath, cacheKey, cache)
		if err != nil {
			slog.Error("Failed to cache OCI chart", "error", err)
			return chartPath, nil
		}
		return finalChartPath, nil
	})
}

func cacheHelmChart(chartPath, cacheKey string, chartCache *HelmChartCache) (string, error) {
	// Check if the chart path is a .tgz file or directory
	stat, err := os.Stat(chartPath)
	if err != nil {
		return "", fmt.Errorf("failed to stat chart path: %w", err)
	}

	if stat.IsDir() {
		// It's already a directory, cache it directly
		if err := chartCache.Set(cacheKey, chartPath); err != nil {
			return "", fmt.Errorf("failed to cache chart directory: %w", err)
		}
		slog.Info("Cached chart directory", "key", cacheKey)
		return chartPath, nil
	}

	// It's a file (likely .tgz), extract it first
	if strings.HasSuffix(chartPath, ".tgz") || strings.HasSuffix(chartPath, ".tar.gz") {
		// Create temporary directory for extraction
		tempDir, err := os.MkdirTemp("", "helm-chart-*")
		if err != nil {
			return "", fmt.Errorf("failed to create temp directory: %w", err)
		}

		// Extract the archive
		if err := extractChartArchive(chartPath, tempDir); err != nil {
			os.RemoveAll(tempDir)
			return "", fmt.Errorf("failed to extract chart archive: %w", err)
		}

		// Find the actual chart directory (usually the first subdirectory)
		entries, err := os.ReadDir(tempDir)
		if err != nil {
			os.RemoveAll(tempDir)
			return "", fmt.Errorf("failed to read extracted directory: %w", err)
		}

		var chartDir string
		for _, entry := range entries {
			if entry.IsDir() {
				chartDir = filepath.Join(tempDir, entry.Name())
				break
			}
		}

		if chartDir == "" {
			os.RemoveAll(tempDir)
			return "", fmt.Errorf("no chart directory found in extracted archive")
		}

		// Cache the extracted directory
		if err := chartCache.Set(cacheKey, chartDir); err != nil {
			os.RemoveAll(tempDir)
			return "", fmt.Errorf("failed to cache extracted chart: %w", err)
		}

		// Get the cached path
		chartLocation, found := chartCache.Get(cacheKey)
		if !found {
			os.RemoveAll(tempDir)
			return "", fmt.Errorf("failed to retrieve cached chart path")
		}

		// Clean up the temporary directory since we've cached it
		os.RemoveAll(tempDir)

		slog.Debug("Extracted and cached chart", "key", cacheKey)
		return chartLocation, nil
	}

	// For other file types, just cache as-is
	if err := chartCache.Set(cacheKey, chartPath); err != nil {
		return "", fmt.Errorf("failed to cache chart file: %w", err)
	}
	slog.Info("Cached chart file", "key", cacheKey)
	return chartPath, nil
}

func extractChartArchive(archivePath, destDir string) error {
	file, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer file.Close()

	// Handle gzip compression
	var reader io.Reader = file
	if strings.HasSuffix(archivePath, ".gz") || strings.HasSuffix(archivePath, ".tgz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Extract tar archive
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// Construct the full path
		destPath := filepath.Join(destDir, header.Name)

		// Security check: prevent path traversal
		if !strings.HasPrefix(destPath, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("invalid file path in archive: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(destPath, 0755); err != nil {
				return fmt.Errorf("failed to create directory: %w", err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory: %w", err)
			}

			outFile, err := os.Create(destPath)
			if err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("failed to write file: %w", err)
			}
			outFile.Close()
		}
	}

	return nil
}

func detectKubernetesVersion(settings *cli.EnvSettings) (*chartutil.KubeVersion, error) {
	restConfig, err := settings.RESTClientGetter().ToRESTConfig()
	if err != nil || restConfig == nil {
		slog.Default().Debug("helm: falling back to default kube version; unable to build REST config",
			"error", err, "version", fallbackKubeVersion)
		return chartutil.ParseKubeVersion(fallbackKubeVersion)
	}

	dc, err := discovery.NewDiscoveryClientForConfig(restConfig)
	if err != nil {
		slog.Default().Debug("helm: falling back to default kube version; unable to create discovery client",
			"error", err, "version", fallbackKubeVersion)
		return chartutil.ParseKubeVersion(fallbackKubeVersion)
	}

	sv, err := dc.ServerVersion()
	if err != nil {
		slog.Default().Debug("helm: falling back to default kube version; unable to query server version",
			"error", err, "version", fallbackKubeVersion)
		return chartutil.ParseKubeVersion(fallbackKubeVersion)
	}

	return chartutil.ParseKubeVersion(sv.GitVersion)
}

// RenderChart renders a Helm chart to Kubernetes manifests without installing it.
func RenderChart(ctx context.Context, namespace, releaseName, chartPath, chartVersion string, rv RenderValues) (string, error) {
	if namespace == "" {
		namespace = "default"
	}
	if releaseName == "" {
		return "", fmt.Errorf("release name cannot be empty")
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	settings := cli.New()
	settings.SetNamespace(namespace)

	actionConfig, err := initActionConfig(settings)
	if err != nil {
		return "", fmt.Errorf("failed to initialize Helm action configuration: %w", err)
	}

	kubeVersion, err := detectKubernetesVersion(settings)
	if err != nil {
		return "", fmt.Errorf("failed to parse Kubernetes version: %w", err)
	}

	installClient := action.NewInstall(actionConfig)
	installClient.DryRunOption = "true"
	installClient.ReleaseName = releaseName
	installClient.Namespace = settings.Namespace()
	installClient.Version = chartVersion
	installClient.ClientOnly = true
	installClient.DryRun = true
	installClient.Replace = true
	installClient.KubeVersion = kubeVersion
	installClient.DisableHooks = true
	installClient.IncludeCRDs = true

	chart, err := loadAndValidateChart(chartPath)
	if err != nil {
		return "", fmt.Errorf("failed to load chart from %q: %w", chartPath, err)
	}

	if err := resolveDependencies(ctx, chart, chartPath, installClient, settings); err != nil {
		return "", fmt.Errorf("failed to resolve chart dependencies: %w", err)
	}

	if chart.Metadata.Dependencies != nil {
		if reloadedChart, err := loader.Load(chartPath); err != nil {
			return "", fmt.Errorf("failed to reload chart after dependency resolution: %w", err)
		} else {
			chart = reloadedChart
		}
	}

	releaseValues, err := mergeHelmValues(rv, settings)
	if err != nil {
		return "", fmt.Errorf("failed to merge helm values: %w", err)
	}

	release, err := installClient.RunWithContext(ctx, chart, releaseValues)
	if err != nil {
		return "", fmt.Errorf("failed to render chart templates for %q: %w", releaseName, err)
	}

	if release == nil || release.Manifest == "" {
		return "", fmt.Errorf("chart rendering produced empty manifest")
	}

	return release.Manifest, nil
}

// mergeHelmValues merges values from all sources in ArgoCD precedence order:
// ValueFiles (lowest) → Values/ValuesObject → Parameters (highest).
func mergeHelmValues(rv RenderValues, settings *cli.EnvSettings) (map[string]any, error) {
	valOpts := values.Options{ValueFiles: rv.ValueFiles}
	merged, err := valOpts.MergeValues(getter.All(settings))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve value files: %w", err)
	}

	if rv.Values != nil {
		chartutil.CoalesceTables(rv.Values, merged)
		merged = rv.Values
	}

	for _, p := range rv.SetParams {
		if p.ForceString {
			if err := strvals.ParseIntoString(p.Name+"="+p.Value, merged); err != nil {
				return nil, fmt.Errorf("failed to apply string parameter %q: %w", p.Name, err)
			}
		} else {
			if err := strvals.ParseInto(p.Name+"="+p.Value, merged); err != nil {
				return nil, fmt.Errorf("failed to apply parameter %q: %w", p.Name, err)
			}
		}
	}

	return merged, nil
}
