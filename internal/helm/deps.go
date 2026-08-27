package helm

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/Masterminds/semver/v3"
	"go.yaml.in/yaml/v3"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/registry"
	"helm.sh/helm/v3/pkg/repo"
)

type indexEntry struct {
	Version string   `yaml:"version"`
	URLs    []string `yaml:"urls"`
}

// fetchMissingDependencies downloads into charts/ every Chart.yaml dependency
// that is not already there. It replaces helm's downloader.Manager, which
// fully parses the index.yaml of every repo it knows about several times per
// call (~0.8GB of allocations per parse of bitnami's 27MB index).
func fetchMissingDependencies(ctx context.Context, ch *chart.Chart, chartPath string) error {
	present := map[string]bool{}
	for _, d := range ch.Dependencies() {
		present[d.Name()] = true
	}
	byRepo := map[string][]*chart.Dependency{}
	for _, d := range ch.Metadata.Dependencies {
		if present[d.Name] {
			continue
		}
		if !strings.HasPrefix(d.Repository, "http://") && !strings.HasPrefix(d.Repository, "https://") {
			return fmt.Errorf("dependency %q: unsupported repository %q (only http(s) chart repos)", d.Name, d.Repository)
		}
		byRepo[d.Repository] = append(byRepo[d.Repository], d)
	}
	if len(byRepo) == 0 {
		return nil
	}
	chartsDir := filepath.Join(chartPath, "charts")
	if err := os.MkdirAll(chartsDir, 0o755); err != nil {
		return err
	}
	for repoURL, deps := range byRepo {
		names := make([]string, len(deps))
		for i, d := range deps {
			names[i] = d.Name
		}
		body, err := httpGet(ctx, strings.TrimSuffix(repoURL, "/")+"/index.yaml")
		if err != nil {
			return err
		}
		entries, err := scanIndexEntries(body, names)
		body.Close()
		if err != nil {
			return fmt.Errorf("read %s/index.yaml: %w", repoURL, err)
		}
		for _, d := range deps {
			if entries[d.Name] == nil {
				return fmt.Errorf("dependency %q: chart not found in %s/index.yaml", d.Name, repoURL)
			}
			url, version, err := pickVersion(entries[d.Name], d)
			if err != nil {
				return err
			}
			dst := filepath.Join(chartsDir, fmt.Sprintf("%s-%s.tgz", d.Name, version))
			if strings.HasPrefix(url, "oci://") { // bitnami and friends now publish tgzs to OCI registries
				err = pullOCI(url, dst)
			} else if url, err = repo.ResolveReferenceURL(repoURL, url); err == nil {
				err = downloadFile(ctx, url, dst)
			}
			if err != nil {
				return fmt.Errorf("dependency %q: %w", d.Name, err)
			}
		}
	}
	return nil
}

// scanIndexEntries streams an index.yaml and parses only the entries.<name>
// sections of the requested charts. Relies on the block layout every
// helm-generated index has: 2-space indent, one `  <name>:` key per chart.
func scanIndexEntries(r io.Reader, names []string) (map[string][]indexEntry, error) {
	want := map[string]*bytes.Buffer{}
	for _, n := range names {
		want[n] = nil
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(nil, 4<<20)
	var cur *bytes.Buffer
	inEntries := false
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
		case line == "entries:":
			inEntries = true
		case !inEntries:
		case line[0] != ' ': // next top-level key
			inEntries = false
		case len(line) > 2 && line[1] == ' ' && line[2] != ' ' && line[2] != '-': // chart name
			cur = nil
			if name := strings.TrimSuffix(line[2:], ":"); want[name] == nil {
				if _, ok := want[name]; ok {
					cur = &bytes.Buffer{}
					want[name] = cur
				}
			}
		case cur != nil:
			cur.WriteString(line)
			cur.WriteByte('\n')
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	out := map[string][]indexEntry{}
	for name, buf := range want {
		if buf == nil {
			continue
		}
		var es []indexEntry
		if err := yaml.Unmarshal(buf.Bytes(), &es); err != nil {
			return nil, fmt.Errorf("entries.%s: %w", name, err)
		}
		out[name] = es
	}
	return out, nil
}

// pickVersion returns the url and version of the highest entry satisfying the
// dependency's version constraint.
func pickVersion(entries []indexEntry, d *chart.Dependency) (string, string, error) {
	c, err := semver.NewConstraint(d.Version)
	if err != nil {
		return "", "", fmt.Errorf("dependency %q: bad version constraint %q: %w", d.Name, d.Version, err)
	}
	var best *semver.Version
	var url string
	for _, e := range entries {
		v, err := semver.NewVersion(e.Version)
		if err != nil || len(e.URLs) == 0 || !c.Check(v) {
			continue
		}
		if best == nil || v.GreaterThan(best) {
			best, url = v, e.URLs[0]
		}
	}
	if best == nil {
		return "", "", fmt.Errorf("dependency %q: no version matching %q in %s", d.Name, d.Version, d.Repository)
	}
	return url, best.Original(), nil
}

// pullOCI pulls the chart layer of an anonymous OCI ref into dst.
func pullOCI(ref, dst string) error {
	rc, err := registry.NewClient()
	if err != nil {
		return err
	}
	res, err := rc.Pull(strings.TrimPrefix(ref, "oci://"))
	if err != nil {
		return err
	}
	return os.WriteFile(dst, res.Chart.Data, 0o644)
}

func httpGet(ctx context.Context, url string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return resp.Body, nil
}

// downloadFile writes url to dst atomically, so concurrent renders of the
// same cached chart never observe a half-written tgz.
func downloadFile(ctx context.Context, url, dst string) error {
	body, err := httpGet(ctx, url)
	if err != nil {
		return err
	}
	defer body.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".dl-*")
	if err != nil {
		return err
	}
	_, err = io.Copy(tmp, body)
	if err = errors.Join(err, tmp.Close()); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), dst)
}
