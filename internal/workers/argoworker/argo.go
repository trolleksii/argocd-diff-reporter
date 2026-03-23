package argoworker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	logrus "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/argoproj/argo-cd/v3/applicationset/controllers/template"
	"github.com/argoproj/argo-cd/v3/applicationset/generators"
	"github.com/argoproj/argo-cd/v3/applicationset/services"
	"github.com/argoproj/argo-cd/v3/applicationset/utils"
	appv1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/reposerver/apiclient"
	"github.com/argoproj/argo-cd/v3/util/db"
	"github.com/argoproj/argo-cd/v3/util/github_app"
	argosettings "github.com/argoproj/argo-cd/v3/util/settings"

	"github.com/trolleksii/argocd-diff-reporter/internal/config"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
	"github.com/trolleksii/argocd-diff-reporter/internal/subjects"
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/argoworker")

// AppSetRendererFunc renders an ApplicationSet into a list of Applications.
// Inject a custom implementation in tests; pass nil in production to use the
// live ArgoCD template engine.
type AppSetRendererFunc func(appset appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error)

type ArgoWorker struct {
	cfg          config.ArgoCDConfig
	log          *slog.Logger
	bus          *nats.Bus
	rendererFunc AppSetRendererFunc
}

func New(cfg config.ArgoCDConfig, log *slog.Logger, b *nats.Bus, customRendererFunc AppSetRendererFunc) *ArgoWorker {
	return &ArgoWorker{
		cfg:          cfg,
		log:          log.With("worker", "argo"),
		bus:          b,
		rendererFunc: customRendererFunc,
	}
}

func (w *ArgoWorker) Run(ctx context.Context) error {
	w.log.Info("starting argo worker...")
	if w.rendererFunc == nil {
		fn, err := newDefaultRendererFunc(ctx, w.cfg)
		if err != nil {
			return err
		}
		w.rendererFunc = fn
	}
	err := w.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "argotemplateengine",
		MaxDeliver:  3,
		AckWait:     3 * time.Second,
		Concurrency: 4,
		Handlers: map[string]nats.Handler{
			subjects.GitFilesSnapshotted: w.handleSnapshottedFiles,
		},
	})
	if err != nil {
		return fmt.Errorf("argotemplateengine: consume: %w", err)
	}
	return nil
}

func (w *ArgoWorker) reportError(ctx context.Context, headers nats.Headers, origin string, e error) {
	headers["error.origin"] = origin
	headers["error.msg"] = e.Error()
	w.log.Error("failed to load file", "error", e)
	w.bus.Publish(ctx, subjects.ArgoAppGenerationFailed, headers, nil)
	delete(headers, "error.origin")
	delete(headers, "error.msg")
}

func (w *ArgoWorker) handleSnapshottedFiles(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
	ctx, span := tracer.Start(
		otel.GetTextMapPropagator().Extract(ctx, headers),
		"handleSnapshottedFiles",
	)
	otel.GetTextMapPropagator().Inject(ctx, headers)
	defer span.End()

	num := headers["pr.number"]
	owner := headers["pr.owner"]
	repo := headers["pr.repo"]
	sha := headers["sha.active"]
	s := headers["pr.files.snapshot"]
	span.SetAttributes(
		attribute.String("pr.owner", owner),
		attribute.String("pr.repo", repo),
		attribute.String("pr.number", num),
		attribute.String("sha.active", sha),
	)
	w.log.Debug("new git.files.snapshotted event",
		"prNum", num,
		"owner", owner,
		"repo", repo,
		"sha", sha)

	specs, err := nats.Unmarshal[[]models.FileProcessingSpec](data)
	if err != nil {
		w.log.Error("failed to unmarshal files", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}

	totalApps := 0
	for _, f := range specs {
		appSets, apps, err := parseFileResources(filepath.Join(s, f.FileName))
		if err != nil {
			w.reportError(ctx, headers, f.ArtifactName, err)
			continue
		}
		for _, appSet := range appSets {
			renderedApps, err := w.rendererFunc(appSet)
			if err != nil {
				w.reportError(ctx, headers, f.ArtifactName, err)
				continue
			}
			for _, a := range renderedApps {
				apps = append(apps, a)
			}
		}
		if f.ArtifactName != "" {
			headers["app.origin"] = f.ArtifactName
		} else {
			headers["app.origin"] = f.FileName
		}

		totalApps += len(apps)
		for _, app := range apps {
			if err := w.routeApp(ctx, app, headers, f.HasNoCounterpart); err != nil {
				span.SetStatus(codes.Error, err.Error())
				nak()
				return
			}
		}
	}
	headers["app.total"] = strconv.Itoa(totalApps)
	w.bus.Publish(ctx, subjects.ArgoTotalUpdated, headers, nil)
	ack()
}

func (w *ArgoWorker) routeApp(ctx context.Context, app appv1alpha1.Application, headers nats.Headers, noCounterpart bool) error {
	sourceType, err := app.Spec.Source.ExplicitType()
	if err != nil {
		w.reportError(ctx, headers, app.Name, fmt.Errorf("multiple explicit source types set: %w", err))
		return nil
	}

	appSpec := models.AppSpec{
		AppName:   app.Name,
		Namespace: app.Spec.Destination.Namespace,
		Source: models.AppSource{
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
		// TODO: check values and valueFiles and add support if necessary
		var values map[string]any
		if err := json.Unmarshal(app.Spec.Source.Helm.ValuesObject.Raw, &values); err != nil {
			w.log.Error("failed to unmarshal helm values", "error", err)
		}
		appSpec.SourceType = models.SourceTypeHelm
		appSpec.Helm = models.HelmSpec{
			ReleaseName: app.Spec.Source.Helm.ReleaseName,
			Values:      values,
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
		w.log.Debug("skipping unsupported source type")
		return nil
	}

	data, err := nats.Marshal(appSpec)
	if err != nil {
		w.log.Error("failed to marshal application", "error", err)
		return err
	}
	w.bus.Publish(ctx, subject, headers, data)
	if noCounterpart {
		headers["sha.active"], headers["sha.complementary"] = headers["sha.complementary"], headers["sha.active"]
		headers["app.name"] = app.Name
		w.bus.Publish(ctx, subjects.ArgoEmptyParsed, headers, nil)
	}
	return nil
}

func newDefaultRendererFunc(ctx context.Context, c config.ArgoCDConfig) (AppSetRendererFunc, error) {
	scheme := runtime.NewScheme()
	clientgoscheme.AddToScheme(scheme)
	appv1alpha1.AddToScheme(scheme)

	cfg := ctrl.GetConfigOrDie()
	if err := appv1alpha1.SetK8SConfigDefaults(cfg); err != nil {
		return nil, fmt.Errorf("failed to apply k8s config defaults: %w", err)
	}

	k8sClientSet, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}
	ctrlClient, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create controller-runtime client: %w", err)
	}

	argoSettingsMgr := argosettings.NewSettingsManager(ctx, k8sClientSet, c.Namespace)
	argoCDDB := db.NewDB(c.Namespace, argoSettingsMgr, k8sClientSet)
	scmConfig := generators.NewSCMConfig("", nil, true, false, github_app.NewAuthCredentials(argoCDDB.(db.RepoCredsDB)), false)
	tlsConfig := apiclient.TLSConfiguration{DisableTLS: true}
	repoClientset := apiclient.NewRepoServerClientset(c.RepoServerAddr, c.RepoServerTimeoutSec, tlsConfig)
	argoCDService := services.NewArgoCDService(argoCDDB, true, repoClientset, false)

	gen := generators.GetGenerators(
		ctx, ctrlClient, k8sClientSet, c.Namespace,
		argoCDService, dynamicClient, scmConfig,
	)
	logctx := logrus.NewEntry(logrus.StandardLogger())
	logrus.SetOutput(io.Discard)
	return func(appset appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
		apps, _, err := template.GenerateApplications(logctx, appset, gen, &utils.Render{}, ctrlClient)
		return apps, err
	}, nil
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
