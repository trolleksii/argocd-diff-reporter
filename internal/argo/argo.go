package argo

import (
	"context"
	"fmt"
	"io"

	logrus "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

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
)

// AppSetRenderer renders an ApplicationSet into a list of Applications.
type AppSetRenderer func(appset appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error)

// Argo bundles the ArgoCD integration built from a single set of cluster
// clients: the ApplicationSet template engine and the repository credentials
// store, both backed by the same ArgoDB.
type Argo struct {
	renderer AppSetRenderer
	creds    *CredsStore
}

// New connects to the cluster and builds the shared ArgoCD dependencies.
func New(ctx context.Context, cfg config.ArgoCDConfig) (*Argo, error) {
	scheme := runtime.NewScheme()
	clientgoscheme.AddToScheme(scheme)
	appv1alpha1.AddToScheme(scheme)

	restCfg, err := ctrl.GetConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubernetes config: %w", err)
	}
	if err := appv1alpha1.SetK8SConfigDefaults(restCfg); err != nil {
		return nil, fmt.Errorf("failed to apply k8s config defaults: %w", err)
	}

	k8sClientSet, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}
	ctrlClient, err := ctrlclient.New(restCfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("failed to create controller-runtime client: %w", err)
	}

	settingsMgr := argosettings.NewSettingsManager(ctx, k8sClientSet, cfg.Namespace)
	argoDB := db.NewDB(cfg.Namespace, settingsMgr, k8sClientSet)

	scmConfig := generators.NewSCMConfig("", nil, true, false, github_app.NewAuthCredentials(argoDB.(db.RepoCredsDB)), false)
	repoClientset := apiclient.NewRepoServerClientset(cfg.RepoServerAddr, cfg.RepoServerTimeoutSec, apiclient.TLSConfiguration{DisableTLS: true})
	argoCDService := services.NewArgoCDService(argoDB, true, repoClientset, false)

	gen := generators.GetGenerators(
		ctx, ctrlClient, k8sClientSet, cfg.Namespace,
		argoCDService, dynamicClient, scmConfig,
	)
	logctx := logrus.NewEntry(logrus.StandardLogger())
	logrus.SetOutput(io.Discard)

	return &Argo{
		renderer: func(appset appv1alpha1.ApplicationSet) ([]appv1alpha1.Application, error) {
			apps, _, err := template.GenerateApplications(logctx, appset, gen, &utils.Render{}, ctrlClient)
			return apps, err
		},
		creds: NewCredsStore(func(ctx context.Context, repoURL, project string) (models.Creds, error) {
			r, err := argoDB.GetRepository(ctx, repoURL, project)
			if err != nil {
				return models.Creds{}, fmt.Errorf("failed to get argocd repository %q: %w", repoURL, err)
			}
			return models.Creds{Username: r.Username, Password: r.Password}, nil
		}),
	}, nil
}

// Renderer returns the live ApplicationSet template engine.
func (a *Argo) Renderer() AppSetRenderer {
	return a.renderer
}

// CredsStore returns the repository credentials store.
func (a *Argo) CredsStore() *CredsStore {
	return a.creds
}
