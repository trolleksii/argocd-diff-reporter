package argo

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	fakectrl "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCachedSecretsList(t *testing.T) {
	cachedSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cluster-a",
			Namespace: "argocd",
			Labels:    map[string]string{"argocd.argoproj.io/secret-type": "cluster"},
		},
	}
	// The reader (informer cache stand-in) holds the cluster secret; the real
	// clientset holds a differently-named one so we can tell which answered.
	reader := fakectrl.NewClientBuilder().WithObjects(cachedSecret).Build()
	apiSecret := cachedSecret.DeepCopy()
	apiSecret.Name = "from-api-server"
	clientset := fake.NewClientset(apiSecret)

	wrapped := &cachedSecretsClientset{Interface: clientset, reader: reader, namespace: "argocd"}
	ctx := context.Background()

	list, err := wrapped.CoreV1().Secrets("argocd").List(ctx, metav1.ListOptions{
		LabelSelector: "argocd.argoproj.io/secret-type=cluster",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != "cluster-a" {
		t.Fatalf("expected cached secret cluster-a, got %+v", list.Items)
	}

	// Label selector that matches nothing must not fall back to the API server.
	list, err = wrapped.CoreV1().Secrets("argocd").List(ctx, metav1.ListOptions{
		LabelSelector: "no=match",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 0 {
		t.Fatalf("expected empty list, got %+v", list.Items)
	}

	// Other namespaces bypass the cache entirely.
	otherNs := apiSecret.DeepCopy()
	otherNs.Namespace = "other"
	clientset = fake.NewClientset(otherNs)
	wrapped = &cachedSecretsClientset{Interface: clientset, reader: reader, namespace: "argocd"}
	list, err = wrapped.CoreV1().Secrets("other").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != "from-api-server" {
		t.Fatalf("expected API-server secret, got %+v", list.Items)
	}

	// Exotic options (field selector) fall through to the API server.
	clientset = fake.NewClientset(apiSecret)
	wrapped = &cachedSecretsClientset{Interface: clientset, reader: reader, namespace: "argocd"}
	list, err = wrapped.CoreV1().Secrets("argocd").List(ctx, metav1.ListOptions{
		FieldSelector: "metadata.name=from-api-server",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Name != "from-api-server" {
		t.Fatalf("expected fallthrough to API server, got %+v", list.Items)
	}
}
