package directoryworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/kustomize/kyaml/resid"
	"sigs.k8s.io/yaml"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/directoryworker")

type DirectoryWorker struct {
	log   *slog.Logger
	bus   *nats.Bus
	store *nats.Store
}

func New(log *slog.Logger, b *nats.Bus, s *nats.Store) *DirectoryWorker {
	return &DirectoryWorker{
		log:   log.With("worker", "directory"),
		bus:   b,
		store: s,
	}
}

func (w *DirectoryWorker) Run(ctx context.Context) error {
	w.log.Info("starting directory worker...")
	err := w.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "directoryworker",
		MaxDeliver:  3,
		AckWait:     3 * time.Second,
		Concurrency: 8,
		Routes: []nats.Route{
			{Subjects: []string{subjects.GitDirectoryFetched}, Handler: w.handleDirectoryRender},
		},
	})
	if err != nil {
		return fmt.Errorf("directory worker failed to consume: %w", err)
	}
	return nil
}

func (w *DirectoryWorker) handleDirectoryRender(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleDirectoryRender",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	number := headers["pr.number"]
	sha := headers["sha.active"]
	origin := headers["app.origin"]
	snapshotDir := headers["chart.location"]

	spec, err := nats.Unmarshal[models.AppSpec](data)
	if err != nil {
		w.log.Error("failed to unmarshal pr object", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", number),
		attribute.String("sha.active", sha),
		attribute.String("app.name", spec.AppName),
		attribute.String("app.origin", origin),
	)
	w.log.Debug("new git.directory.fetched event",
		"app", spec.AppName,
		"path", spec.Source.Path,
		"revision", spec.Source.Revision)

	sourcePath := filepath.Join(snapshotDir, spec.Source.Path)

	if spec.SourceType == models.SourceTypeUndefined {
		chartYAML := filepath.Join(sourcePath, "Chart.yaml")
		if _, statErr := os.Stat(chartYAML); statErr == nil {
			msg := "unexpected Helm source routed to directory worker: Chart.yaml found at path"
			headers["error.msg"] = msg
			headers["error.origin.file"] = origin
			headers["error.origin.app"] = spec.AppName
			w.log.Error(msg, "app", spec.AppName, "path", sourcePath)
			span.SetStatus(codes.Error, msg)
			w.bus.Publish(ctx, subjects.DirectoryManifestRenderFailed, headers, nil)
			ack()
			return
		}
		kustomizationFilenames := []string{"kustomization.yaml", "kustomization.yml", "Kustomization"}
		spec.SourceType = models.SourceTypeDirectory
		for _, name := range kustomizationFilenames {
			if _, statErr := os.Stat(filepath.Join(sourcePath, name)); statErr == nil {
				spec.SourceType = models.SourceTypeKustomize
				break
			}
		}
	}

	manifestLocation := fmt.Sprintf("%s.%s.%s.%s.%s.%s", owner, repo, number, sha, origin, spec.AppName)
	switch spec.SourceType {
	case models.SourceTypeKustomize:
		err = w.renderKustomize(ctx, spec, sourcePath, manifestLocation)
	case models.SourceTypeDirectory:
		err = w.renderDirectory(ctx, spec, sourcePath, manifestLocation)
	}
	if err != nil {
		headers["error.msg"] = err.Error()
		headers["error.origin.file"] = origin
		headers["error.origin.app"] = spec.AppName
		w.log.Error("failed to render manifest", "error", err)
		w.bus.Publish(ctx, subjects.DirectoryManifestRenderFailed, headers, nil)
		ack()
		return
	}
	headers["manifest.location"] = manifestLocation
	headers["app.name"] = spec.AppName
	w.bus.Publish(ctx, subjects.DirectoryManifestRendered, headers, nil)
	ack()
}

// isKustomizeSpecUnset returns true when all fields of the KustomizeSpec are zero values.
func isKustomizeSpecUnset(k models.KustomizeSpec) bool {
	return k.NamePrefix == "" && k.NameSuffix == "" && k.Namespace == "" &&
		len(k.CommonLabels) == 0 && len(k.CommonAnnotations) == 0 &&
		!k.ForceCommonLabels && !k.ForceCommonAnnotations &&
		len(k.Images) == 0 && len(k.Replicas) == 0 &&
		len(k.Patches) == 0 && len(k.Components) == 0
}


func (w *DirectoryWorker) renderKustomize(ctx context.Context, spec models.AppSpec, kustomizationDir, key string) error {
	runDir := kustomizationDir
	var tempDir string
	var err error
	if !isKustomizeSpecUnset(spec.Kustomize) {
		tempDir, err = buildKustomizeOverlay(kustomizationDir, spec.Kustomize)
		if err != nil {
			return err 
		}
		defer os.RemoveAll(tempDir)
		runDir = tempDir
	}
	kustomizer := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
	fSys := filesys.MakeFsOnDisk()
	resMap, err := kustomizer.Run(fSys, runDir)
	if err != nil {
		return err
	}
	renderedYAML, err := resMap.AsYaml()
	if err != nil {
		return err
	}
	if err := w.store.StoreObject(ctx, key, string(renderedYAML)); err != nil {
		return err
	}
	return nil
}

func (w *DirectoryWorker) renderDirectory(ctx context.Context, spec models.AppSpec, sourcePath, key string) error {
	var yamlFiles []string
	if spec.Directory.Recurse {
		err := filepath.WalkDir(sourcePath, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && isYAMLFile(d.Name()) {
				yamlFiles = append(yamlFiles, path)
			}
			return nil
		})
		if err != nil {
			return err
		}
	} else {
		entries, err := os.ReadDir(sourcePath)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.IsDir() && isYAMLFile(entry.Name()) {
				yamlFiles = append(yamlFiles, filepath.Join(sourcePath, entry.Name()))
			}
		}
	}

	if len(yamlFiles) == 0 {
		return errors.New("no YAML files found in directory")
	}

	sort.Strings(yamlFiles)

	var parts []string
	for _, f := range yamlFiles {
		content, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		parts = append(parts, string(content))
	}
	concatenated := strings.Join(parts, "---\n")

	if err := w.store.StoreObject(ctx, key, concatenated); err != nil {
		return err
	}
	return nil
}

func isYAMLFile(name string) bool {
	lower := strings.ToLower(name)
	return strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml")
}

// parseKustomizeImageString converts an image string in kustomize format
// ([old_name=]name[:tag][@digest]) to a types.Image struct.
func parseKustomizeImageString(img string) types.Image {
	var oldName, rest string
	if idx := strings.Index(img, "="); idx >= 0 {
		oldName = img[:idx]
		rest = img[idx+1:]
	} else {
		rest = img
	}
	var name, tag, digest string
	if idx := strings.Index(rest, "@"); idx >= 0 {
		digest = rest[idx+1:]
		rest = rest[:idx]
	}
	if idx := strings.LastIndex(rest, ":"); idx >= 0 {
		name = rest[:idx]
		tag = rest[idx+1:]
	} else {
		name = rest
	}
	if oldName == "" {
		oldName = name
	}
	return types.Image{
		Name:    oldName,
		NewName: name,
		NewTag:  tag,
		Digest:  digest,
	}
}

// buildKustomizeOverlay creates a temporary kustomization overlay directory that
// references baseDir and applies the given KustomizeSpec parameters.
// The caller is responsible for removing the returned temp directory.
func buildKustomizeOverlay(baseDir string, spec models.KustomizeSpec) (string, error) {
	overlay := types.Kustomization{
		NamePrefix:        spec.NamePrefix,
		NameSuffix:        spec.NameSuffix,
		Namespace:         spec.Namespace,
		CommonLabels:      spec.CommonLabels,
		CommonAnnotations: spec.CommonAnnotations,
		Components:        spec.Components,
	}
	if spec.ForceCommonLabels && len(spec.CommonLabels) > 0 {
		overlay.Labels = []types.Label{{
			Pairs:            spec.CommonLabels,
			IncludeSelectors: true,
		}}
	}
	// ForceCommonAnnotations has no direct equivalent in the krusty Go API;
	// CommonAnnotations are always applied to all objects, so the flag is a no-op here.
	for _, img := range spec.Images {
		overlay.Images = append(overlay.Images, parseKustomizeImageString(img))
	}
	for _, r := range spec.Replicas {
		overlay.Replicas = append(overlay.Replicas, types.Replica{
			Name:  r.Name,
			Count: r.Count,
		})
	}
	for _, p := range spec.Patches {
		patch := types.Patch{Path: p.Path, Patch: p.Patch}
		if p.Target != nil {
			patch.Target = &types.Selector{
				ResId: resid.ResId{
					Gvk: resid.Gvk{
						Group:   p.Target.Group,
						Version: p.Target.Version,
						Kind:    p.Target.Kind,
					},
					Name:      p.Target.Name,
					Namespace: p.Target.Namespace,
				},
				LabelSelector:      p.Target.LabelSelector,
				AnnotationSelector: p.Target.AnnotationSelector,
			}
		}
		overlay.Patches = append(overlay.Patches, patch)
	}

	// Create the overlay dir as a sibling of baseDir so we can use a relative path.
	// krusty does not allow absolute paths in Resources.
	parentDir := filepath.Dir(baseDir)
	tempDir, err := os.MkdirTemp(parentDir, "kustomize-overlay-*")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir for kustomize overlay: %w", err)
	}

	relPath, err := filepath.Rel(tempDir, baseDir)
	if err != nil {
		os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to compute relative path for kustomize overlay: %w", err)
	}
	overlay.Resources = []string{relPath}

	overlayYAML, err := yaml.Marshal(overlay)
	if err != nil {
		os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to marshal kustomize overlay: %w", err)
	}

	kustFile := filepath.Join(tempDir, "kustomization.yaml")
	if err := os.WriteFile(kustFile, overlayYAML, 0o644); err != nil {
		os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to write kustomize overlay file: %w", err)
	}
	return tempDir, nil
}
