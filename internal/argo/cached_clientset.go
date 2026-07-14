package argo

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// cachedSecretsClientset wraps a kubernetes.Interface so that Secret LISTs in
// the given namespace are served from the informer cache instead of the API
// server. The ApplicationSet cluster generator lists cluster secrets through
// the plain clientset (utils.ListClusters) on every render; this reroutes
// those reads to the same watch-fed cache the controller-runtime client uses.
// Everything else falls through to the real clientset.
type cachedSecretsClientset struct {
	kubernetes.Interface
	reader    ctrlclient.Reader
	namespace string
}

func (c *cachedSecretsClientset) CoreV1() corev1client.CoreV1Interface {
	return &cachedCoreV1{
		CoreV1Interface: c.Interface.CoreV1(),
		reader:          c.reader,
		namespace:       c.namespace,
	}
}

type cachedCoreV1 struct {
	corev1client.CoreV1Interface
	reader    ctrlclient.Reader
	namespace string
}

func (c *cachedCoreV1) Secrets(namespace string) corev1client.SecretInterface {
	real := c.CoreV1Interface.Secrets(namespace)
	if namespace != c.namespace {
		return real
	}
	return &cachedSecrets{SecretInterface: real, reader: c.reader, namespace: namespace}
}

type cachedSecrets struct {
	corev1client.SecretInterface
	reader    ctrlclient.Reader
	namespace string
}

func (c *cachedSecrets) List(ctx context.Context, opts metav1.ListOptions) (*corev1.SecretList, error) {
	// Only plain (optionally label-selected) lists are served from cache;
	// anything more exotic falls through to the API server.
	if opts.FieldSelector != "" || opts.Watch || opts.Limit != 0 || opts.Continue != "" {
		return c.SecretInterface.List(ctx, opts)
	}
	sel := labels.Everything()
	if opts.LabelSelector != "" {
		var err error
		if sel, err = labels.Parse(opts.LabelSelector); err != nil {
			return c.SecretInterface.List(ctx, opts)
		}
	}
	var list corev1.SecretList
	if err := c.reader.List(ctx, &list,
		ctrlclient.InNamespace(c.namespace),
		ctrlclient.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return c.SecretInterface.List(ctx, opts)
	}
	return &list, nil
}
