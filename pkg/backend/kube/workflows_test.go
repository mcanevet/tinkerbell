package kube

import (
	"context"
	"net/http"
	"testing"

	"github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"github.com/tinkerbell/tinkerbell/pkg/data"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
)

// TestUpdateWorkflowOptimisticLock verifies that two callers patching the same Workflow
// from the same snapshot don't silently last-write-wins when OptimisticLock is set: the
// second patch, built against a resourceVersion the object no longer has, must fail
// rather than overwrite the first caller's write.
func TestUpdateWorkflowOptimisticLock(t *testing.T) {
	seed := tinkerbell.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf1", Namespace: "default"},
		Status:     tinkerbell.WorkflowStatus{State: tinkerbell.WorkflowStatePending},
	}

	rs := runtime.NewScheme()
	if err := scheme.AddToScheme(rs); err != nil {
		t.Fatal(err)
	}
	if err := tinkerbell.AddToScheme(rs); err != nil {
		t.Fatal(err)
	}

	cl := fake.NewClientBuilder().
		WithScheme(rs).
		WithRuntimeObjects(&tinkerbell.WorkflowList{}).
		WithStatusSubresource(&tinkerbell.Workflow{}).
		WithLists(&tinkerbell.WorkflowList{Items: []tinkerbell.Workflow{seed}}).
		Build()

	fn := func(o *cluster.Options) {
		o.NewClient = func(*rest.Config, client.Options) (client.Client, error) {
			return cl, nil
		}
		o.MapperProvider = func(*rest.Config, *http.Client) (meta.RESTMapper, error) {
			return cl.RESTMapper(), nil
		}
		o.NewCache = func(*rest.Config, cache.Options) (cache.Cache, error) {
			return &informertest.FakeInformers{Scheme: cl.Scheme()}, nil
		}
	}
	rc := new(rest.Config)
	b, err := NewBackend(Backend{ClientConfig: rc}, fn)
	if err != nil {
		t.Fatal(err)
	}
	go b.Start(context.Background())

	ctx := context.Background()

	// Two callers both read the Workflow at the same (stale-to-be) resourceVersion.
	callerA, err := b.ReadWorkflow(ctx, "wf1", "default")
	if err != nil {
		t.Fatal(err)
	}
	callerB, err := b.ReadWorkflow(ctx, "wf1", "default")
	if err != nil {
		t.Fatal(err)
	}

	// Caller A wins the race: patches first, bumping the stored resourceVersion.
	aOriginal := callerA.DeepCopy()
	callerA.Status.State = tinkerbell.WorkflowStateRunning
	if err := b.UpdateWorkflow(ctx, callerA, data.UpdateOptions{StatusOnly: true, PatchFrom: aOriginal, OptimisticLock: true}); err != nil {
		t.Fatalf("caller A: unexpected error: %v", err)
	}

	// Caller B's patch is still built against the resourceVersion from before caller A's
	// write - with OptimisticLock this must fail as a conflict, not silently overwrite
	// caller A's already-persisted state.
	bOriginal := callerB.DeepCopy()
	callerB.Status.State = tinkerbell.WorkflowStateFailed
	err = b.UpdateWorkflow(ctx, callerB, data.UpdateOptions{StatusOnly: true, PatchFrom: bOriginal, OptimisticLock: true})
	if err == nil {
		t.Fatal("caller B: expected a conflict error, got nil")
	}

	stored, err := b.ReadWorkflow(ctx, "wf1", "default")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status.State != tinkerbell.WorkflowStateRunning {
		t.Fatalf("expected caller A's write to survive (State=%q), got %q", tinkerbell.WorkflowStateRunning, stored.Status.State)
	}
}
