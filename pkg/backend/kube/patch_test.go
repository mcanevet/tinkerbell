package kube

import (
	"strings"
	"testing"

	v1alpha1 "github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"github.com/tinkerbell/tinkerbell/pkg/data"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPatchFromOptsOptimisticLock(t *testing.T) {
	original := &v1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf1", Namespace: "default", ResourceVersion: "5"},
	}
	modified := original.DeepCopy()
	modified.Status.State = v1alpha1.WorkflowStatePending

	t.Run("without OptimisticLock, no resourceVersion precondition in the patch body", func(t *testing.T) {
		p, err := patchFromOpts(data.UpdateOptions{PatchFrom: original})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		body, err := p.Data(modified)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if strings.Contains(string(body), "resourceVersion") {
			t.Fatalf("expected no resourceVersion precondition in the patch, got %s", body)
		}
	})

	t.Run("with OptimisticLock, resourceVersion precondition included in the patch body", func(t *testing.T) {
		p, err := patchFromOpts(data.UpdateOptions{PatchFrom: original, OptimisticLock: true})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		body, err := p.Data(modified)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(string(body), `"resourceVersion":"5"`) {
			t.Fatalf("expected a resourceVersion=5 precondition in the patch, got %s", body)
		}
	})

	t.Run("with OptimisticLock but no resourceVersion on the original, a clear error rather than a silent no-op lock", func(t *testing.T) {
		noVersion := &v1alpha1.Workflow{ObjectMeta: metav1.ObjectMeta{Name: "wf1", Namespace: "default"}}
		p, err := patchFromOpts(data.UpdateOptions{PatchFrom: noVersion, OptimisticLock: true})
		if err != nil {
			t.Fatalf("unexpected error building the patch: %v", err)
		}
		if _, err := p.Data(noVersion.DeepCopy()); err == nil {
			t.Fatal("expected an error computing patch data without a resourceVersion to lock against, got nil")
		}
	})
}
