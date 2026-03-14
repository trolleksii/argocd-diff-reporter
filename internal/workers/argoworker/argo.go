package argoworker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	logrus "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
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
)

var tracer = otel.Tracer("argocd-diff-reporter/internal/workers/argoworker")

type ArgoWorker struct {
	cfg      config.ArgoCDConfig
	log      *slog.Logger
	bus      *nats.Bus
	generate templateFunc
}

type templateFunc func(appset appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error)

func New(cfg config.ArgoCDConfig, log *slog.Logger, b *nats.Bus) *ArgoWorker {
	return &ArgoWorker{
		cfg: cfg,
		log: log.With("worker", "argo"),
		bus: b,
	}
}

func (m *ArgoWorker) Run(ctx context.Context) error {
	m.log.Info("starting argo worker...")
	fn, err := getTemplateFunc(ctx, m.cfg)
	if err != nil {
		return err
	}
	m.generate = fn
	err = m.bus.Consume(ctx, nats.ConsumerConfig{
		Name:        "argotemplateengine",
		MaxDeliver:  3,
		AckWait:     3 * time.Second,
		Concurrency: 4,
		Handlers: map[string]nats.Handler{
			"git.files.snapshotted": m.handleSnapshottedFiles,
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
	w.bus.Publish(ctx, "argo.app.generation.failed", headers, nil)
	delete(headers, "error.origin")
	delete(headers, "error.msg")
}

func (m *ArgoWorker) handleSnapshottedFiles(ctx context.Context, headers nats.Headers, data []byte, ack, nak func() error) {
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
	m.log.Debug("new git.files.snapshotted event",
		"prNum", num,
		"owner", owner,
		"repo", repo,
		"sha", sha)

	specs, err := nats.Unmarshal[[]models.FileProcessingSpec](data)
	if err != nil {
		m.log.Error("failed to unmarshal files", "error", err)
		span.SetStatus(codes.Error, err.Error())
		nak()
		return
	}

	for _, f := range specs {
		appSets, apps, err := parseFileResources(filepath.Join(s, f.FileName))
		if err != nil {
			m.reportError(ctx, headers, f.ArtifactName, err)
			continue
		}
		for _, appSet := range appSets {
			renderedApps, err := m.generate(appSet)
			if err != nil {
				m.reportError(ctx, headers, f.ArtifactName, err)
				continue
			}
			for _, a := range renderedApps {
				apps = append(apps, a)
			}
		}
		headers["app.origin"] = f.ArtifactName
		for _, app := range apps {
			if app.Spec.Source.Helm == nil {
				m.log.Debug("skipping non helm application")
				// TODO: add support for kustomize/dir
				continue
			}
			// TODO: check values and valueFiles and add support if necessay
			var values map[string]any
			if err := json.Unmarshal(app.Spec.Source.Helm.ValuesObject.Raw, &values); err != nil {
				m.log.Error("failed to unmarshal helm values", "error", err)
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
				Helm: models.HelmSpec{
					ReleaseName: app.Spec.Source.Helm.ReleaseName,
					Values:      values,
				},
			}

			var subject string = "argo.helm.oci.parsed"
			switch {
			// Spec with git reference will have non empty path
			case app.Spec.Source.Path != "":
				subject = "argo.helm.git.parsed"
			case strings.HasPrefix(app.Spec.Source.RepoURL, "http://") || strings.HasPrefix(app.Spec.Source.RepoURL, "https://"):
				subject = "argo.helm.http.parsed"
			}
			data, err := nats.Marshal(appSpec)
			if err != nil {
				m.log.Error("failed to marshal application", "error", err)
				span.SetStatus(codes.Error, err.Error())
				nak()
				return
			}
			m.bus.Publish(ctx, subject, headers, data)
			if f.EmptyCounterpart {
				headers["sha.active"], headers["sha.complementary"] = headers["sha.complementary"], headers["sha.active"]
				headers["app.name"] = app.Name
				m.bus.Publish(ctx, "argo.helm.empty.parsed", headers, nil)
			}
		}
	}
	ack()
}

func getTemplateFunc(ctx context.Context, c config.ArgoCDConfig) (func(appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error), error) {
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
