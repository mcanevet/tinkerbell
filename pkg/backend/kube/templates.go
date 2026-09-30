package kube

import (
	"context"
	"fmt"

	v1alpha1 "github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"go.opentelemetry.io/otel"
	"k8s.io/apimachinery/pkg/types"
)

// ReadTemplate looks up a Template object by name and namespace using a direct Get.
func (b *Backend) ReadTemplate(ctx context.Context, name, namespace string) (*v1alpha1.Template, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "backend.kube.ReadTemplate")
	defer span.End()

	tpl := &v1alpha1.Template{}
	if err := b.cluster.GetClient().Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, tpl); err != nil {
		return nil, fmt.Errorf("failed to get template %s/%s: %w", namespace, name, err)
	}

	return tpl, nil
}
