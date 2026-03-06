package helm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/singleflight"
)

// HelmChartCache implements ChartCache using the filesystem
type HelmChartCache struct {
	cacheDir string
	inflight singleflight.Group
}

// NewChartDiskCache creates a new file-based chart cache
func NewChartDiskCache(cacheDir string) (*HelmChartCache, error) {
	cacheDir = filepath.Join(cacheDir, "snapshots")
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory %s: %w", cacheDir, err)
	}

	return &HelmChartCache{cacheDir: cacheDir}, nil
}

// GenerateCacheKey creates a unique cache key for a chart reference and version
func GenerateCacheKey(chartRef, version string) string {
	return fmt.Sprintf("%x", xxhash.Sum64String(chartRef+version))
}

// GetOrFetch returns a cached chart path if it exists, otherwise calls fetch to populate the cache.
// Concurrent calls with the same key are deduplicated — only one fetch runs, the rest wait and share the result.
func (c *HelmChartCache) GetOrFetch(key string, fetch func() (string, error)) (string, error) {
	if path, ok := c.Get(key); ok {
		return path, nil
	}
	result, err, _ := c.inflight.Do(key, func() (any, error) {
		if path, ok := c.Get(key); ok {
			return path, nil
		}
		return fetch()
	})
	if err != nil {
		return "", err
	}
	return result.(string), nil
}

// Get retrieves a cached chart path if it exists
func (c *HelmChartCache) Get(key string) (string, bool) {
	cachePath := filepath.Join(c.cacheDir, key)
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		return "", false
	}

	entries, err := os.ReadDir(cachePath)
	if err != nil || len(entries) == 0 {
		return "", false
	}
	return cachePath, true
}

// Set stores a chart in the cache
func (c *HelmChartCache) Set(key, chartPath string) error {
	cachePath := filepath.Join(c.cacheDir, key)

	if err := os.MkdirAll(cachePath, 0755); err != nil {
		return fmt.Errorf("failed to create cache directory %s: %w", cachePath, err)
	}

	return copyDir(chartPath, cachePath)
}

func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return fmt.Errorf("failed to read source directory %s: %w", src, err)
	}

	if err := os.MkdirAll(dst, 0755); err != nil {
		return fmt.Errorf("failed to create destination directory %s: %w", dst, err)
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", src, err)
	}
	defer srcFile.Close()

	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination file %s: %w", dst, err)
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("failed to copy file from %s to %s: %w", src, dst, err)
	}

	return nil
}
