package argoworker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	logrus "github.com/sirupsen/logrus"
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
	"github.com/trolleksii/argocd-diff-reporter/internal/nats"
)

type ArgoWorker struct {
	cfg      config.ArgoCDConfig
	log      *slog.Logger
	bus      *nats.Bus
	generate templateFunc
}

type templateFunc func(appset appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, appv1alpha1.ApplicationSetReasonType, error)

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
		Name:       "argotemplateengine",
		MaxDeliver: 3,
		AckWait:    3 * time.Second,
		Handlers: map[string]nats.Handler{
			"git.files.snapshotted": m.process,
		},
	})
	if err != nil {
		return fmt.Errorf("argotemplateengine: consume: %w", err)
	}
	return nil
}

func (m *ArgoWorker) process(ctx context.Context, headers map[string]string, data []byte, ack, nak func() error) {
	m.log.Info("argo worker got an event")
	files, err := nats.Unmarshal[[]string](data)
	if err != nil {
		m.log.Error("failed to unmarshal files", "error", err)
		nak()
		return
	}
	s := headers["snapshotDir"]
	for _, f := range files {
		headers["fileName"] = f
		appSets, apps, err := parseFileResources(filepath.Join(s, f))
		if err != nil {
			headers["error"] = err.Error()
			m.log.Error("failed to load file", "error", err)
			m.bus.Publish(ctx, "argo.file.parsing.failed", headers, []byte{})
			delete(headers, "error")
			continue
		}
		for _, appSet := range appSets {
			headers["appset"] = appSet.Name
			renderedApps, reason, err := m.generate(appSet)
			if err != nil {
				headers["error"] = err.Error()
				m.log.Error("failed to generate applications", "reason", reason, "error", err)
				m.bus.Publish(ctx, "argo.app.generation.failed", headers, nil)
				delete(headers, "error")
				continue
			}
			for _, a := range renderedApps {
				apps = append(apps, a)
			}
		}
		for _, app := range apps {
			headers["application"] = app.Name
			data, err := nats.Marshal(app)
			if err != nil {
				m.log.Error("failed to marshal application", "error", err)
				nak()
				return
			}

			if app.Spec.Source.Helm == nil {
				m.log.Debug("skipping non helm application")
				// TODO: add support for kustomize/dir
				continue
			}
			headers["chartRepo"] = app.Spec.Source.RepoURL 
			headers["chartRevision"] = app.Spec.Source.TargetRevision
			switch {
			// Spec with git reference will have non empty path
			case app.Spec.Source.Path != "":
				headers["chartPath"] = app.Spec.Source.Path
				m.bus.Publish(ctx, "argo.helm.git.parsed", headers, data)
			case strings.HasPrefix(app.Spec.Source.RepoURL, "http://") || strings.HasPrefix(app.Spec.Source.RepoURL, "https://"):
				headers["chartName"] = app.Spec.Source.Chart
				m.bus.Publish(ctx, "argo.helm.http.parsed", headers, data)
			default:
				headers["chartName"] = app.Spec.Source.Chart
				m.bus.Publish(ctx, "argo.helm.oci.parsed", headers, data)
			}
		}
	}
	ack()
}

func getTemplateFunc(ctx context.Context, c config.ArgoCDConfig) (func(appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, appv1alpha1.ApplicationSetReasonType, error), error) {
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
	return func(appset appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, appv1alpha1.ApplicationSetReasonType, error) {
		return template.GenerateApplications(logctx, appset, gen, &utils.Render{}, ctrlClient)
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
