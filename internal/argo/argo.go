package argo

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
	"github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	appv1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/reposerver/apiclient"
	"github.com/argoproj/argo-cd/v3/util/db"
	"github.com/argoproj/argo-cd/v3/util/github_app"
	argosettings "github.com/argoproj/argo-cd/v3/util/settings"

	"github.com/trolleksii/argocd-diff-reporter/internal/bus"
	"github.com/trolleksii/argocd-diff-reporter/internal/config"
)

type TemplateEngine struct {
	cfg      config.ArgoCDConfig
	log      *slog.Logger
	bus      *bus.Bus
	generate templateFunc
}

type templateFunc func(appset v1alpha1.ApplicationSet) ([]v1alpha1.Application, v1alpha1.ApplicationSetReasonType, error)

func NewTemplateEngine(cfg config.ArgoCDConfig, b *bus.Bus, log *slog.Logger) *TemplateEngine {
	return &TemplateEngine{
		cfg: cfg,
		log: log.With("component", "argotemplateengine"),
		bus: b,
	}
}

func (m *TemplateEngine) Run(ctx context.Context) error {
	fn, err := getTemplateFunc(ctx, m.cfg)
	if err != nil {
		return err
	}
	m.generate = fn
	err = m.bus.Consume(ctx, bus.ConsumerConfig{
		Name:       "argotemplateengine",
		Subjects:   []string{"repo.snapshot.created"},
		MaxDeliver: 3,
		AckWait:    3 * time.Second,
		Handle:     m.process,
	})
	if err != nil {
		return fmt.Errorf("gitrepomanager: consume: %w", err)
	}
	return nil
}

func try(log *slog.Logger, msg string, fn func() error) {
	if err := fn(); err != nil {
		log.Error(msg, "error", err)
	}
}

// process handles one incoming snapshot request.
func (m *TemplateEngine) process(ctx context.Context, subject string, headers map[string]string, data []byte, ack, nak func() error) {
	switch subject {
	case "repo.snapshot.created":
		files, err := bus.Unmarshal[[]string](data)
		if err != nil {
			m.log.Error("failed to unmarshal files", "error", err)
			try(m.log, "failed to nak the message", nak)
			return
		}
		s := headers["snapshotDir"]
		for _, f := range files {
			appSets, err := loadApplicationSetsFromFile(filepath.Join(s, f))
			if err != nil {
				if os.IsNotExist(err) {
					return
				}
				// publish error event
			}
			for _, appset := range appSets {
				apps, reason, err := m.generate(appset)
				if err != nil {
					m.log.Error("failed to generate applications", "reason", reason, "error", err)
				}
				for _, a := range apps {
					m.log.Info(a.Name)
				}
			}
		}
	}
	try(m.log, "failed to ack message", ack)
}

func getTemplateFunc(ctx context.Context, c config.ArgoCDConfig) (func(v1alpha1.ApplicationSet) ([]v1alpha1.Application, v1alpha1.ApplicationSetReasonType, error), error) {
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
	return func(appset v1alpha1.ApplicationSet) ([]v1alpha1.Application, v1alpha1.ApplicationSetReasonType, error) {
		return template.GenerateApplications(logctx, appset, gen, &utils.Render{}, ctrlClient)
	}, nil
}

func loadApplicationSetsFromFile(filePath string) ([]v1alpha1.ApplicationSet, error) {
	appSetBytes, err := os.ReadFile(filePath)
	var appSets []v1alpha1.ApplicationSet
	if err != nil {
		return appSets, err
	}

	documents := strings.SplitSeq(string(appSetBytes), "---")
	for doc := range documents {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}

		var appSet v1alpha1.ApplicationSet
		if err := yaml.Unmarshal([]byte(doc), &appSet); err != nil {
			return appSets, err
		}

		appSets = append(appSets, appSet)
	}
	return appSets, nil
}
