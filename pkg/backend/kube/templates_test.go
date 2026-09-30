package kube

import (
	"context"
	"testing"

	v1alpha1 "github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestReadTemplate(t *testing.T) {
	data := "version: \"0.1\""
	b := newAttributesTestBackend(t, &v1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
		Spec:       v1alpha1.TemplateSpec{Data: &data},
	})

	tpl, err := b.ReadTemplate(context.Background(), "debian", "default")
	if err != nil {
		t.Fatalf("ReadTemplate() error = %v", err)
	}
	if tpl.Spec.Data == nil || *tpl.Spec.Data != data {
		t.Fatalf("ReadTemplate() Spec.Data = %v, want %q", tpl.Spec.Data, data)
	}

	if _, err := b.ReadTemplate(context.Background(), "missing", "default"); !kerrors.IsNotFound(err) {
		t.Fatalf("ReadTemplate() for a missing Template: error = %v, want a NotFound error", err)
	}
}
