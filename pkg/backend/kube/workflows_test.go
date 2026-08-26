package kube

import (
	"context"
	"net/http"
	"sort"
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

// TestListWorkflowsByAgentID exercises the fix for a Workflow awaiting its target
// Agent's first check-in: before it has ever rendered, it has no Status.AgentID (the
// WorkflowAgentIDIndex is empty), so ByAgentID must also match via
// WorkflowHardwareMapIndex (Spec.HardwareMap) or WorkflowHardwareRefIndex
// (Spec.HardwareRef, resolved to the Agent's own Hardware name first) - the ordinary
// single-machine shape (HardwareRef only, no HardwareMap) is otherwise never findable.
func TestListWorkflowsByAgentID(t *testing.T) {
	rendered := tinkerbell.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "rendered", Namespace: "default"},
		Spec: tinkerbell.WorkflowSpec{
			HardwareMap: map[string]string{"device_1": "agent1"},
		},
		Status: tinkerbell.WorkflowStatus{
			State:   tinkerbell.WorkflowStateRunning,
			AgentID: "agent1",
		},
	}
	awaitingCheckIn := tinkerbell.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "awaiting-check-in", Namespace: "default"},
		Spec: tinkerbell.WorkflowSpec{
			HardwareMap: map[string]string{"device_1": "agent2"},
		},
		Status: tinkerbell.WorkflowStatus{
			State: tinkerbell.WorkflowStateAwaitingCheckIn,
			// AgentID intentionally empty: this Workflow has never rendered.
		},
	}
	unrelated := tinkerbell.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "default"},
		Spec: tinkerbell.WorkflowSpec{
			HardwareMap: map[string]string{"device_1": "agent3"},
		},
		Status: tinkerbell.WorkflowStatus{State: tinkerbell.WorkflowStateAwaitingCheckIn},
	}
	// The ordinary single-machine shape: addressed via Spec.HardwareRef (a Hardware
	// object's name), not Spec.HardwareMap - this is the configuration
	// WorkflowHardwareMapIndex alone can never find, since it only ever looks at
	// HardwareMap.
	awaitingCheckInByRef := tinkerbell.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "awaiting-check-in-by-ref", Namespace: "default"},
		Spec:       tinkerbell.WorkflowSpec{HardwareRef: "hw4"},
		Status:     tinkerbell.WorkflowStatus{State: tinkerbell.WorkflowStateAwaitingCheckIn},
	}
	hw4 := tinkerbell.Hardware{
		ObjectMeta: metav1.ObjectMeta{Name: "hw4", Namespace: "default"},
		Spec:       tinkerbell.HardwareSpec{AgentID: "agent4"},
	}

	tests := map[string]struct {
		byAgentID string
		wantNames []string
	}{
		"already-rendered Workflow found by status.agentID": {
			byAgentID: "agent1",
			wantNames: []string{"rendered"},
		},
		"never-rendered Workflow found by spec.hardwareMap": {
			byAgentID: "agent2",
			wantNames: []string{"awaiting-check-in"},
		},
		"never-rendered Workflow found by spec.hardwareRef": {
			byAgentID: "agent4",
			wantNames: []string{"awaiting-check-in-by-ref"},
		},
		"no match": {
			byAgentID: "agent-does-not-exist",
			wantNames: []string{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rs := runtime.NewScheme()
			if err := scheme.AddToScheme(rs); err != nil {
				t.Fatal(err)
			}
			if err := tinkerbell.AddToScheme(rs); err != nil {
				t.Fatal(err)
			}

			cl := fake.NewClientBuilder().
				WithScheme(rs).
				WithRuntimeObjects(&tinkerbell.WorkflowList{}, &tinkerbell.HardwareList{}).
				WithIndex(&tinkerbell.Workflow{}, WorkflowAgentIDIndex, WorkflowAgentID).
				WithIndex(&tinkerbell.Workflow{}, WorkflowHardwareMapIndex, WorkflowHardwareMapAgentIDs).
				WithIndex(&tinkerbell.Workflow{}, WorkflowHardwareRefIndex, WorkflowHardwareRefs).
				WithIndex(&tinkerbell.Hardware{}, HardwareAgentIDIndex, HardwareAgentID).
				WithLists(
					&tinkerbell.WorkflowList{Items: []tinkerbell.Workflow{rendered, awaitingCheckIn, unrelated, awaitingCheckInByRef}},
					&tinkerbell.HardwareList{Items: []tinkerbell.Hardware{hw4}},
				).
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

			got, err := b.ListWorkflows(context.Background(), data.WorkflowFilter{ByAgentID: tc.byAgentID})
			if err != nil {
				t.Fatal(err)
			}
			gotNames := make([]string, 0, len(got))
			for _, wf := range got {
				gotNames = append(gotNames, wf.Name)
			}
			sort.Strings(gotNames)
			want := append([]string{}, tc.wantNames...)
			sort.Strings(want)
			if len(gotNames) != len(want) {
				t.Fatalf("ListWorkflows(ByAgentID=%q) names = %v, want %v", tc.byAgentID, gotNames, want)
			}
			for i := range want {
				if gotNames[i] != want[i] {
					t.Errorf("ListWorkflows(ByAgentID=%q) names = %v, want %v", tc.byAgentID, gotNames, want)
					break
				}
			}
		})
	}
}

// TestUpdateWorkflowOptimisticLock exercises the fix for renderOnCheckIn's concurrent
// check-in race: two callers reading the same Workflow and racing to patch its status
// must not silently last-write-wins when OptimisticLock is set - the second patch, built
// against a resourceVersion the object no longer has, must fail rather than overwrite
// the first caller's write.
func TestUpdateWorkflowOptimisticLock(t *testing.T) {
	seed := tinkerbell.Workflow{
		ObjectMeta: metav1.ObjectMeta{Name: "wf1", Namespace: "default"},
		Status:     tinkerbell.WorkflowStatus{State: tinkerbell.WorkflowStateAwaitingCheckIn},
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
	callerA.Status.State = tinkerbell.WorkflowStatePending
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
	if stored.Status.State != tinkerbell.WorkflowStatePending {
		t.Fatalf("expected caller A's write to survive (State=%q), got %q", tinkerbell.WorkflowStatePending, stored.Status.State)
	}
}
