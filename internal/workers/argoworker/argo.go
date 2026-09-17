package argoworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"sigs.k8s.io/yaml"

	appv1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"

	"github.com/trolleksii/argocd-diff-reporter/internal/argo"
	"github.com/trolleksii/argocd-diff-reporter/internal/keys"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
	"github.com/trolleksii/argocd-diff-reporter/internal/tracing"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/argoworker")

type ArgoWorker struct {
	log          *slog.Logger
	bus          *nats.Bus
	rendererFunc argo.AppSetRenderer
}

// New creates the worker. rendererFunc is the live template engine from
// argo.New in production, or a stub in tests.
func New(log *slog.Logger, b *nats.Bus, rendererFunc argo.AppSetRenderer) *ArgoWorker {
	return &ArgoWorker{
		log:          log.With("worker", "argo"),
		bus:          b,
		rendererFunc: rendererFunc,
	}
}

func (w *ArgoWorker) Run(ctx context.Context) error {
	w.log.InfoContext(ctx, "starting argo worker...")
	err := w.bus.Consume(ctx, nats.ConsumerConfig{
		Name:       "argotemplateengine",
		MaxDeliver: 3,
		// AppSet generators may hit git/SCM APIs; must outlast a slow render or the
		// message is redelivered while the first attempt is still running.
		AckWait:     time.Minute,
		Concurrency: 4,
		Routes:      []nats.Route{{Subjects: []string{subjects.GitFilesSnapshotted}, Handler: w.parseForArgoResources}},
	})
	if err != nil {
		return fmt.Errorf("argotemplateengine: consume: %w", err)
	}
	return nil
}

func (w *ArgoWorker) parseForArgoResources(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"parseForArgoResources",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	num := headers.Get("pr.number")
	owner := headers.Get("pr.owner")
	repo := headers.Get("pr.repo")
	sha := headers.Get("sha.active")
	runId := headers.Get("RunId")
	s := headers.Get("pr.files.snapshot")
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", num),
		attribute.String("sha.active", sha),
	)
	w.log.DebugContext(ctx, "new git.files.snapshotted event",
		"prNum", num,
		"owner", owner,
		"repo", repo,
		"sha", sha)

	files, err := nats.Unmarshal[[]string](data)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to unmarshal files", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}

	side := make([]models.FileParsingResult, 0, len(files))

	// renderDispatch is a an AppSpec with routing information
	type renderDispatch struct {
		file, subject string
		spec          models.ArgoAppSpec
	}
	var pending []renderDispatch
	for _, f := range files {
		_, parseSpan := tracing.StartDetail(ctx, tracer, "parseFileResources")
		parseSpan.SetAttributes(attribute.String("file", f))
		appSets, apps, err := parseFileResources(filepath.Join(s, f))
		parseSpan.End()
		if err != nil {
			w.log.ErrorContext(ctx, "failed to load file", "error", err, "file", f)
			side = append(side, models.FileParsingResult{File: f, Error: err.Error()})
			continue
		}
		// Render all ApplicationSets in parallel; error reporting and app
		// routing stay sequential because they mutate the shared headers map.
		appsFromAppsets := make([][]appv1alpha1.Application, len(appSets))
		appsetErrs := make([]error, len(appSets))
		var wg sync.WaitGroup
		for i, appSet := range appSets {
			wg.Go(func() {
				appsFromAppsets[i], appsetErrs[i] = w.rendererFunc(appSet)
			})
		}
		_, renderSpan := tracing.StartDetail(ctx, tracer, "waitAppsetRender")
		wg.Wait()
		renderSpan.End()
		if err := errors.Join(appsetErrs...); err != nil {
			w.log.ErrorContext(ctx, "failed to render appsets", "error", err, "file", f)
			side = append(side, models.FileParsingResult{File: f, Error: err.Error()})
			continue
		}
		for i := range appSets {
			apps = append(apps, appsFromAppsets[i]...)
		}

		_, routeSpan := tracing.StartDetail(ctx, tracer, "routeApps")
		routeSpan.SetAttributes(
			attribute.String("file", f),
			attribute.Int("apps.count", len(apps)),
		)

		var fa []models.AppParsingResult
		for _, app := range apps {
			sub, spec, err := w.buildAppSpec(ctx, app)
			if err != nil {
				fa = append(fa, models.AppParsingResult{Name: app.Name, Error: err.Error()})
				continue
			}
			pending = append(pending, renderDispatch{file: f, subject: sub, spec: spec})
			fa = append(fa, models.AppParsingResult{Name: app.Name})
		}
		routeSpan.End()
		side = append(side, models.FileParsingResult{File: f, Apps: fa})
	}
	data, err = nats.Marshal(side)
	if err != nil {
		w.log.ErrorContext(ctx, "failed to marshal side result", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}
	headers.Set(keys.MsgIDHeader, keys.MsgIDSide(owner, repo, num, runId, sha))
	w.bus.Publish(ctx, subjects.ArgoSideParsed, headers, data)
	for _, d := range pending {
		headers.Set("app.origin", d.file)
		headers.Set("app.name", d.spec.AppName)
		data, err := nats.Marshal(d.spec)
		if err != nil {
			w.log.ErrorContext(ctx, "failed to marshal application", "error", err)
			nak()
			return
		}
		headers.Set(keys.MsgIDHeader, keys.MsgIDApp(owner, repo, num, runId, sha, d.file, d.spec.AppName))
		w.bus.Publish(ctx, d.subject, headers, data)
	}
	ack()
}

func (w *ArgoWorker) buildAppSpec(ctx context.Context, app appv1alpha1.Application) (string, models.ArgoAppSpec, error) {
	sourceType, err := app.Spec.Source.ExplicitType()
	if err != nil {
		w.log.ErrorContext(ctx, "multiple explicit source types set", "error", err)
		return "", models.ArgoAppSpec{}, fmt.Errorf("multiple explicit source types set: %w", err)
	}

	appSpec := models.ArgoAppSpec{
		AppName:   app.Name,
		Namespace: app.Spec.Destination.Namespace,
		Project:   app.Spec.Project,
		Source: models.ArgoAppSource{
			RepoURL:   app.Spec.Source.RepoURL,
			Revision:  app.Spec.Source.TargetRevision,
			Path:      app.Spec.Source.Path,
			ChartName: app.Spec.Source.Chart,
		},
	}

	var subject string
	switch {
	case sourceType == nil:
		// No explicit source type — auto-detection is deferred to the DirectoryWorker.
		subject = subjects.ArgoDirectoryGitParsed

	case *sourceType == appv1alpha1.ApplicationSourceTypeDirectory:
		subject = subjects.ArgoDirectoryGitParsed
		appSpec.SourceType = models.SourceTypeDirectory
		if app.Spec.Source.Directory != nil {
			appSpec.Directory.Recurse = app.Spec.Source.Directory.Recurse
		}

	case *sourceType == appv1alpha1.ApplicationSourceTypeKustomize:
		subject = subjects.ArgoDirectoryGitParsed
		appSpec.SourceType = models.SourceTypeKustomize
		if k := app.Spec.Source.Kustomize; k != nil {
			appSpec.Kustomize = models.KustomizeSpec{
				NamePrefix:             k.NamePrefix,
				NameSuffix:             k.NameSuffix,
				Namespace:              k.Namespace,
				CommonLabels:           k.CommonLabels,
				CommonAnnotations:      k.CommonAnnotations,
				ForceCommonLabels:      k.ForceCommonLabels,
				ForceCommonAnnotations: k.ForceCommonAnnotations,
				Images:                 kustomizeImagesToStrings(k.Images),
				Components:             k.Components,
			}
			for _, r := range k.Replicas {
				count, _ := r.GetIntCount()
				appSpec.Kustomize.Replicas = append(appSpec.Kustomize.Replicas, models.KustomizeReplica{
					Name:  r.Name,
					Count: int64(count),
				})
			}
			for _, p := range k.Patches {
				patch := models.KustomizePatch{
					Path:  p.Path,
					Patch: p.Patch,
				}
				if p.Target != nil {
					patch.Target = &models.KustomizePatchTarget{
						Group:              p.Target.Group,
						Version:            p.Target.Version,
						Kind:               p.Target.Kind,
						Name:               p.Target.Name,
						Namespace:          p.Target.Namespace,
						LabelSelector:      p.Target.LabelSelector,
						AnnotationSelector: p.Target.AnnotationSelector,
					}
				}
				appSpec.Kustomize.Patches = append(appSpec.Kustomize.Patches, patch)
			}
		}

	case *sourceType == appv1alpha1.ApplicationSourceTypeHelm:
		h := app.Spec.Source.Helm
		appSpec.SourceType = models.SourceTypeHelm
		appSpec.Helm = models.HelmSpec{
			ReleaseName: h.ReleaseName,
			ValueFiles:  h.ValueFiles,
		}
		if !h.ValuesIsEmpty() {
			if err := yaml.Unmarshal(h.ValuesYAML(), &appSpec.Helm.Values); err != nil {
				return "", models.ArgoAppSpec{}, fmt.Errorf("unmarshal helm values: %w", err)
			}
		}
		for _, p := range h.Parameters {
			appSpec.Helm.Parameters = append(appSpec.Helm.Parameters, models.HelmParameter{
				Name:        p.Name,
				Value:       p.Value,
				ForceString: p.ForceString,
			})
		}
		switch {
		// Spec with git reference will have non empty path
		case app.Spec.Source.Path != "":
			subject = subjects.ArgoHelmGitParsed
		case strings.HasPrefix(app.Spec.Source.RepoURL, "http://") || strings.HasPrefix(app.Spec.Source.RepoURL, "https://"):
			subject = subjects.ArgoHelmHTTPParsed
		default:
			subject = subjects.ArgoHelmOCIParsed
		}

	default:
		return "", models.ArgoAppSpec{}, fmt.Errorf("unsupported source type: %s", *sourceType)
	}

	return subject, appSpec, nil
}

func parseFileResources(filePath string) ([]appv1alpha1.ApplicationSet, []appv1alpha1.Application, error) {
	appSetBytes, err := os.ReadFile(filePath)
	var appSets []appv1alpha1.ApplicationSet
	var apps []appv1alpha1.Application
	if err != nil {
		return appSets, apps, err
	}

	documents := strings.SplitSeq(string(appSetBytes), "---")
	for doc := range documents {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}

		var meta struct {
			Kind string `yaml:"kind"`
		}
		if err := yaml.Unmarshal([]byte(doc), &meta); err != nil {
			return appSets, apps, err
		}

		switch meta.Kind {
		case "ApplicationSet":
			var appSet appv1alpha1.ApplicationSet
			if err := yaml.Unmarshal([]byte(doc), &appSet); err != nil {
				return appSets, apps, err
			}
			appSets = append(appSets, appSet)
		case "Application":
			var app appv1alpha1.Application
			if err := yaml.Unmarshal([]byte(doc), &app); err != nil {
				return appSets, apps, err
			}
			apps = append(apps, app)
		}
	}
	return appSets, apps, nil
}

// kustomizeImagesToStrings converts ArgoCD KustomizeImages ([]KustomizeImage, each is a string)
// to a plain []string for use in models.KustomizeSpec.
func kustomizeImagesToStrings(images appv1alpha1.KustomizeImages) []string {
	if len(images) == 0 {
		return nil
	}
	result := make([]string, len(images))
	for i, img := range images {
		result[i] = string(img)
	}
	return result
}
