package helm

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
)

func TestFetchMissingDependencies(t *testing.T) {
	// serve a helm-style index with decoy charts around the wanted one
	dir := t.TempDir()
	child := &chart.Chart{Metadata: &chart.Metadata{APIVersion: "v2", Name: "childchart", Version: "0.1.1"}}
	if _, err := chartutil.Save(child, dir); err != nil {
		t.Fatal(err)
	}
	index := `apiVersion: v1
entries:
  aaa:
  - name: aaa
    version: 9.9.9
    urls:
    - aaa-9.9.9.tgz
  childchart:
  - name: childchart
    version: 0.1.1
    urls:
    - childchart-0.1.1.tgz
  - name: childchart
    version: 0.1.0
    urls:
    - childchart-0.1.0.tgz
  zzz:
  - name: zzz
    version: 1.0.0
    urls:
    - zzz-1.0.0.tgz
generated: "2026-01-01T00:00:00Z"
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/index.yaml" {
			w.Write([]byte(index))
			return
		}
		http.ServeFile(w, r, filepath.Join(dir, filepath.Base(r.URL.Path)))
	}))
	defer srv.Close()

	parent := func(depVersion string) string {
		p := t.TempDir()
		y := "apiVersion: v2\nname: parentchart\nversion: 0.1.0\ndependencies:\n  - name: childchart\n    version: " + depVersion + "\n    repository: " + srv.URL + "\n"
		os.WriteFile(filepath.Join(p, "Chart.yaml"), []byte(y), 0o644)
		return p
	}

	p := parent("~0.1.0")
	ch, _ := loader.Load(p)
	if err := fetchMissingDependencies(context.Background(), ch, p); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(p, "charts", "childchart-0.1.1.tgz")); err != nil {
		t.Fatal(err)
	}
	ch, _ = loader.Load(p)
	if len(ch.Dependencies()) != 1 || ch.Dependencies()[0].Metadata.Version != "0.1.1" {
		t.Fatalf("dependency not loaded: %+v", ch.Dependencies())
	}
	// second call: already present, nothing to do
	if err := fetchMissingDependencies(context.Background(), ch, p); err != nil {
		t.Fatal(err)
	}

	p = parent("2.0.0")
	ch, _ = loader.Load(p)
	err := fetchMissingDependencies(context.Background(), ch, p)
	if err == nil || !strings.Contains(err.Error(), `no version matching "2.0.0"`) {
		t.Fatalf("expected version-not-found error, got %v", err)
	}
}
