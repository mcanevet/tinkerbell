package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/go-logr/logr"
	"github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"github.com/tinkerbell/tinkerbell/pkg/constant"
	"github.com/tinkerbell/tinkerbell/pkg/data"
	"github.com/tinkerbell/tinkerbell/pkg/proto"
	"github.com/tinkerbell/tinkerbell/tink/internal/render"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type fakeDynamicClient struct {
	result map[string]interface{}
	err    error

	calls int // counts calls to DynamicRead, to catch redundant re-resolution
}

func (f *fakeDynamicClient) DynamicRead(_ context.Context, _ schema.GroupVersionResource, _, _ string) (map[string]interface{}, error) {
	f.calls++
	return f.result, f.err
}

const checkInTemplateData = `{{- $attrs := index .hardware.metadata.annotations "tinkerbell.org/agent-attributes" | fromJson -}}
version: "0.1"
name: debian
global_timeout: 600
tasks:
  - name: "provision"
    worker: "{{ .hardware.spec.agentID }}"
    volumes:
      - /dev:/dev
    actions:
      - name: "stream"
        image: quay.io/tinkerbell-actions/image2disk:v1.0.0
        timeout: 300
        environment:
          DEST_DISK: "/dev/{{ (index $attrs.blockDevices 0).name }}"
`

const checkInHardwareMapTemplateData = `version: "0.1"
name: debian
global_timeout: 600
tasks:
  - name: "provision"
    worker: "{{ .device_1 }}"
    volumes:
      - /dev:/dev
    actions:
      - name: "stream"
        image: quay.io/tinkerbell-actions/image2disk:v1.0.0
        timeout: 300
`

const checkInReferenceTemplateData = `version: "0.1"
name: debian
global_timeout: 600
tasks:
  - name: "provision"
    worker: "{{ .hardware.spec.agentID }}"
    volumes:
      - /dev:/dev
    actions:
      - name: "stream"
        image: quay.io/tinkerbell-actions/image2disk:v1.0.0
        timeout: 300
        environment:
          DEST_DISK: "{{ .references.hw.diskPath }}"
`

func awaitingCheckInWorkflow() *tinkerbell.Workflow {
	return &tinkerbell.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wf1",
			Namespace: "default",
		},
		Spec: tinkerbell.WorkflowSpec{
			TemplateRef: "debian",
			HardwareRef: "my-hw",
		},
		Status: tinkerbell.WorkflowStatus{
			State: tinkerbell.WorkflowStateAwaitingCheckIn,
		},
	}
}

func TestGetActionRenderOnCheckIn(t *testing.T) {
	t.Run("renders and serves the first action once attributes arrive", func(t *testing.T) {
		mock := &mockBackendReadWriter{
			workflow: awaitingCheckInWorkflow(),
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
			},
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		resp, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId: toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{
				Block: []*proto.Block{
					{Name: toPtr("sda"), SizeBytes: toPtr(int64(1_000_000_000_000))},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetName() != "stream" {
			t.Fatalf("expected first action %q to be served, got %q", "stream", resp.GetName())
		}
		wantEnv := "DEST_DISK=/dev/sda"
		found := false
		for _, e := range resp.GetEnvironment() {
			if e == wantEnv {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected environment to contain %q, got %v", wantEnv, resp.GetEnvironment())
		}
		if mock.updatedHardware == nil {
			t.Fatal("expected the attributes annotation to be force-refreshed on Hardware")
		}
		var gotAttrs data.AgentAttributes
		if err := json.Unmarshal([]byte(mock.updatedHardware.Annotations[constant.AttributesAnnotation]), &gotAttrs); err != nil {
			t.Fatalf("annotation is not valid JSON: %v", err)
		}
		if len(gotAttrs.BlockDevices) != 1 || gotAttrs.BlockDevices[0].Name == nil || *gotAttrs.BlockDevices[0].Name != "sda" {
			t.Fatalf("expected the refreshed annotation to contain this check-in's block devices, got %+v", gotAttrs.BlockDevices)
		}
	})

	t.Run("preserves Status.BootOptions/Conditions recorded during Preparing", func(t *testing.T) {
		wf := awaitingCheckInWorkflow()
		wf.Status.BootOptions.AllowNetboot.ToggledTrue = true
		wf.Status.SetCondition(tinkerbell.WorkflowCondition{
			Type:   tinkerbell.ToggleAllowNetbootTrue,
			Status: metav1.ConditionTrue,
			Reason: "Complete",
		})
		mock := &mockBackendReadWriter{
			workflow: wf,
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
			},
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		// A Workflow with BootOptions goes ""->Preparing (recording
		// Status.BootOptions/Conditions there) ->AwaitingCheckIn before ever reaching
		// check-in - renderOnCheckIn must not wipe that record when it renders.
		_, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId: toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{
				Block: []*proto.Block{
					{Name: toPtr("sda"), SizeBytes: toPtr(int64(1_000_000_000_000))},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !mock.updatedWorkflow.Status.BootOptions.AllowNetboot.ToggledTrue {
			t.Fatal("expected Status.BootOptions.AllowNetboot.ToggledTrue to survive the check-in render, got wiped")
		}
		found := false
		for _, c := range mock.updatedWorkflow.Status.Conditions {
			if c.Type == tinkerbell.ToggleAllowNetbootTrue {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected the ToggleAllowNetbootTrue condition to survive the check-in render, got %+v", mock.updatedWorkflow.Status.Conditions)
		}
	})

	t.Run("workflow stays waiting when no attributes are on this check-in", func(t *testing.T) {
		mock := &mockBackendReadWriter{
			workflow: awaitingCheckInWorkflow(),
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		_, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId: toPtr("machine-mac-1"),
		})
		if err == nil {
			t.Fatal("expected an error (no rendered Tasks yet), got nil")
		}
		if mock.updatedHardware != nil {
			t.Fatalf("expected no writes at all, got Hardware update")
		}
	})

	t.Run("a template referencing an unpopulated field fails predictably", func(t *testing.T) {
		mock := &mockBackendReadWriter{
			workflow: awaitingCheckInWorkflow(),
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
			},
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		// No Block devices reported at all - $attrs.blockDevices resolves against an empty
		// annotation map, so this must fail rather than silently rendering an empty disk
		// target.
		_, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId:         toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{},
		})
		if err == nil {
			t.Fatal("expected an error from a missing map key, got nil")
		}
		// The attributes annotation refresh happens before rendering and succeeds on its
		// own terms regardless of what the Template does with it afterwards - only the
		// Workflow's own status reflects the render failure.
		if mock.updatedHardware == nil {
			t.Fatal("expected the attributes annotation to still be refreshed even though rendering failed")
		}
	})

	t.Run("renders a Template addressing its worker via Spec.HardwareMap", func(t *testing.T) {
		wf := awaitingCheckInWorkflow()
		wf.Spec.HardwareMap = map[string]string{"device_1": "machine-mac-1"}
		mock := &mockBackendReadWriter{
			workflow: wf,
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInHardwareMapTemplateData)},
			},
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		// worker: "{{ .device_1 }}" only resolves if render.Input.HardwareMap is populated
		// from wf.Spec.HardwareMap - previously omitted on this check-in-time render path.
		resp, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId:         toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetName() != "stream" {
			t.Fatalf("expected first action %q to be served, got %q", "stream", resp.GetName())
		}
	})

	t.Run("a broken sibling Workflow does not block a servable one for the same Agent", func(t *testing.T) {
		broken := awaitingCheckInWorkflow()
		broken.Name = "wf-broken"
		broken.Spec.TemplateRef = "broken-template"

		good := awaitingCheckInWorkflow()
		good.Name = "wf-good"
		good.Spec.TemplateRef = "debian"

		mock := &mockBackendReadWriter{
			// broken listed first: proves doGetAction tries wf-good instead of aborting
			// the whole request on wf-broken's render failure.
			workflows: []tinkerbell.Workflow{*broken, *good},
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			templates: map[string]*tinkerbell.Template{
				"broken-template": {
					ObjectMeta: metav1.ObjectMeta{Name: "broken-template", Namespace: "default"},
					Spec:       tinkerbell.TemplateSpec{Data: toPtr("not: valid: yaml: [")},
				},
				"debian": {
					ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
					Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
				},
			},
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		resp, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId: toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{
				Block: []*proto.Block{
					{Name: toPtr("sda"), SizeBytes: toPtr(int64(1_000_000_000_000))},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v (a broken sibling Workflow should not abort a servable one)", err)
		}
		if resp.GetWorkflowId() != "default/wf-good" {
			t.Fatalf("expected the action to come from wf-good, got workflow %q", resp.GetWorkflowId())
		}
	})

	t.Run("resolves Hardware.Spec.References the same way the workflow controller does", func(t *testing.T) {
		mock := &mockBackendReadWriter{
			workflow: awaitingCheckInWorkflow(),
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec: tinkerbell.HardwareSpec{
					AgentID: "machine-mac-1",
					References: map[string]tinkerbell.Reference{
						"hw": {Name: "my-hw", Namespace: "default", Group: "tinkerbell.org", Version: "v1alpha1", Resource: "hardware"},
					},
				},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInReferenceTemplateData)},
			},
		}
		server := &Handler{
			Logger:        logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:       mock,
			NowFunc:       func() time.Time { return time.Time{} },
			RetryOptions:  []backoff.RetryOption{backoff.WithMaxTries(1)},
			DynamicClient: &fakeDynamicClient{result: map[string]interface{}{"diskPath": "/dev/nvme0n1"}},
			ReferenceRules: render.ReferenceRules{
				Allowlist: []string{`{"reference":{"resource":["hardware"]}}`},
			},
		}

		resp, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId:         toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		wantEnv := "DEST_DISK=/dev/nvme0n1"
		found := false
		for _, e := range resp.GetEnvironment() {
			if e == wantEnv {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected environment to contain %q (from a resolved reference), got %v", wantEnv, resp.GetEnvironment())
		}
	})

	t.Run("renders a Template with no Spec.HardwareRef at all", func(t *testing.T) {
		wf := awaitingCheckInWorkflow()
		wf.Spec.HardwareRef = ""
		wf.Spec.HardwareMap = map[string]string{"device_1": "machine-mac-1"}
		mock := &mockBackendReadWriter{
			workflow: wf,
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInHardwareMapTemplateData)},
			},
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		// No HardwareRef set - and no hardware on the mock either, so a lookup attempt
		// would fail with "hardware not found". Rendering must still succeed, addressing
		// the worker via Spec.HardwareMap alone.
		resp, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId:         toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetName() != "stream" {
			t.Fatalf("expected first action %q to be served, got %q", "stream", resp.GetName())
		}
	})
}

// TestGetActionRenderOnCheckInFailureHandling covers doGetAction's handling of sibling
// Workflows, caching across them, and renderOnCheckIn failure classification (permanent
// vs transient vs a conflict worth aborting the whole request for) - split out from
// TestGetActionRenderOnCheckIn purely to stay under the cognitive-complexity lint limit
// as this grew across several rounds of review.
func TestGetActionRenderOnCheckInFailureHandling(t *testing.T) {
	t.Run("a terminal sibling does not block a fresh AwaitingCheckIn Workflow for the same Agent", func(t *testing.T) {
		terminal := tinkerbell.Workflow{
			ObjectMeta: metav1.ObjectMeta{Name: "wf-terminal", Namespace: "default"},
			Spec:       tinkerbell.WorkflowSpec{TemplateRef: "debian", HardwareRef: "my-hw"},
			Status: tinkerbell.WorkflowStatus{
				State:   tinkerbell.WorkflowStateSuccess,
				AgentID: "machine-mac-1",
				Tasks: []tinkerbell.Task{
					{
						Name:    "provision",
						AgentID: "machine-mac-1",
						Actions: []tinkerbell.Action{
							{Name: "stream", State: tinkerbell.WorkflowStateSuccess},
						},
					},
				},
			},
		}
		fresh := awaitingCheckInWorkflow()
		fresh.Name = "wf-fresh"

		mock := &mockBackendReadWriter{
			// terminal listed first - matching ListWorkflows' real merge order (already-
			// rendered Workflows before not-yet-rendered ones) - proves doGetAction skips
			// past it instead of hard-aborting the whole request before wf-fresh is ever
			// considered. Workflows are never garbage collected, so any Agent that ever
			// completed a prior Workflow would otherwise never be served a new one.
			workflows: []tinkerbell.Workflow{terminal, *fresh},
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
			},
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		resp, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId: toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{
				Block: []*proto.Block{
					{Name: toPtr("sda"), SizeBytes: toPtr(int64(1_000_000_000_000))},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v (a terminal sibling should not block wf-fresh)", err)
		}
		if resp.GetWorkflowId() != "default/wf-fresh" {
			t.Fatalf("expected the action to come from wf-fresh, got workflow %q", resp.GetWorkflowId())
		}
	})

	t.Run("siblings sharing a HardwareRef reuse the same read+annotated Hardware", func(t *testing.T) {
		broken := awaitingCheckInWorkflow()
		broken.Name = "wf-broken"
		broken.Spec.TemplateRef = "broken-template"

		good := awaitingCheckInWorkflow()
		good.Name = "wf-good"
		good.Spec.TemplateRef = "debian"

		mock := &mockBackendReadWriter{
			// broken listed first, sharing wf-good's HardwareRef ("my-hw") - proves
			// renderOnCheckIn's second call (for wf-good) reuses the Hardware object
			// broken's failed render already read and annotated, instead of redundantly
			// reading and re-patching the same object a second time for this check-in.
			workflows: []tinkerbell.Workflow{*broken, *good},
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			templates: map[string]*tinkerbell.Template{
				"broken-template": {
					ObjectMeta: metav1.ObjectMeta{Name: "broken-template", Namespace: "default"},
					Spec:       tinkerbell.TemplateSpec{Data: toPtr("not: valid: yaml: [")},
				},
				"debian": {
					ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
					Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
				},
			},
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		resp, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId: toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{
				Block: []*proto.Block{
					{Name: toPtr("sda"), SizeBytes: toPtr(int64(1_000_000_000_000))},
				},
			},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetWorkflowId() != "default/wf-good" {
			t.Fatalf("expected the action to come from wf-good, got workflow %q", resp.GetWorkflowId())
		}
		if mock.readHardwareCalls != 1 {
			t.Fatalf("expected exactly 1 ReadHardware call across both siblings, got %d", mock.readHardwareCalls)
		}
		if mock.updateHardwareCalls != 1 {
			t.Fatalf("expected exactly 1 UpdateHardware call across both siblings, got %d", mock.updateHardwareCalls)
		}
	})

	t.Run("siblings sharing a HardwareRef reuse the same resolved References", func(t *testing.T) {
		broken := awaitingCheckInWorkflow()
		broken.Name = "wf-broken"
		broken.Spec.TemplateRef = "broken-template"

		good := awaitingCheckInWorkflow()
		good.Name = "wf-good"
		good.Spec.TemplateRef = "debian"

		fakeDC := &fakeDynamicClient{result: map[string]interface{}{"diskPath": "/dev/nvme0n1"}}
		mock := &mockBackendReadWriter{
			// broken listed first, sharing wf-good's HardwareRef ("my-hw") - proves
			// wf-good's render reuses the References already resolved while rendering
			// (and failing) wf-broken, instead of re-resolving them from scratch.
			workflows: []tinkerbell.Workflow{*broken, *good},
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec: tinkerbell.HardwareSpec{
					AgentID: "machine-mac-1",
					References: map[string]tinkerbell.Reference{
						"hw": {Name: "my-hw", Namespace: "default", Group: "tinkerbell.org", Version: "v1alpha1", Resource: "hardware"},
					},
				},
			},
			templates: map[string]*tinkerbell.Template{
				"broken-template": {
					ObjectMeta: metav1.ObjectMeta{Name: "broken-template", Namespace: "default"},
					Spec:       tinkerbell.TemplateSpec{Data: toPtr("not: valid: yaml: [")},
				},
				"debian": {
					ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
					Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInReferenceTemplateData)},
				},
			},
		}
		server := &Handler{
			Logger:        logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:       mock,
			NowFunc:       func() time.Time { return time.Time{} },
			RetryOptions:  []backoff.RetryOption{backoff.WithMaxTries(1)},
			DynamicClient: fakeDC,
			ReferenceRules: render.ReferenceRules{
				Allowlist: []string{`{"reference":{"resource":["hardware"]}}`},
			},
		}

		resp, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId:         toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetWorkflowId() != "default/wf-good" {
			t.Fatalf("expected the action to come from wf-good, got workflow %q", resp.GetWorkflowId())
		}
		wantEnv := "DEST_DISK=/dev/nvme0n1"
		found := false
		for _, e := range resp.GetEnvironment() {
			if e == wantEnv {
				found = true
			}
		}
		if !found {
			t.Fatalf("expected environment to contain %q, got %v", wantEnv, resp.GetEnvironment())
		}
		if fakeDC.calls != 1 {
			t.Fatalf("expected exactly 1 DynamicRead call across both siblings, got %d", fakeDC.calls)
		}
	})

	t.Run("hardware genuinely not found marks the Workflow Failed", func(t *testing.T) {
		mock := &mockBackendReadWriter{
			workflow:    awaitingCheckInWorkflow(),
			hardwareErr: kerrors.NewNotFound(schema.GroupResource{Group: "tinkerbell.org", Resource: "hardware"}, "my-hw"),
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		// ReadHardware fails with a genuine not-found before rendering ever starts - this
		// is permanent (won't change on retry) and must leave the Workflow in a visible
		// terminal state, not just an error returned to this one caller with no persisted
		// trace.
		_, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId:         toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{},
		})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if mock.updatedWorkflow == nil {
			t.Fatal("expected the Workflow status to be persisted even on a pre-render failure")
		}
		if mock.updatedWorkflow.Status.State != tinkerbell.WorkflowStateFailed {
			t.Fatalf("expected Workflow to be marked %q, got %q", tinkerbell.WorkflowStateFailed, mock.updatedWorkflow.Status.State)
		}
	})

	t.Run("a transient hardware read error does not mark the Workflow Failed", func(t *testing.T) {
		mock := &mockBackendReadWriter{
			workflow:    awaitingCheckInWorkflow(),
			hardwareErr: errors.New("boom: hardware backend unavailable"),
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		// A plain (non-NotFound) error is treated as transient - it must be returned
		// as-is without persisting any Workflow status change, so the Workflow stays
		// AwaitingCheckIn (and indexed) for the Agent's next check-in to retry.
		_, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId:         toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{},
		})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if mock.updatedWorkflow != nil {
			t.Fatalf("expected no Workflow status persisted for a transient error, got state %q", mock.updatedWorkflow.Status.State)
		}
	})

	t.Run("still returns the read+annotated Hardware when the final persist fails", func(t *testing.T) {
		mock := &mockBackendReadWriter{
			workflow: awaitingCheckInWorkflow(),
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
			},
			writeErr: errors.New("boom: backend unavailable persisting the render"),
		}
		server := &Handler{
			Logger:  logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend: mock,
			NowFunc: func() time.Time { return time.Time{} },
		}

		// A successful render whose final UpdateWorkflow fails must still return the
		// Hardware it already read and annotated - doGetAction's hwCache relies on this to
		// avoid re-reading the same object for a sibling Workflow sharing this HardwareRef.
		attrs := convert(&proto.AgentAttributes{
			Block: []*proto.Block{{Name: toPtr("sda"), SizeBytes: toPtr(int64(1_000_000_000_000))}},
		})
		_, cache, err := server.renderOnCheckIn(context.Background(), awaitingCheckInWorkflow(), attrs, nil)
		if err == nil {
			t.Fatal("expected an error persisting the render, got nil")
		}
		if cache == nil || cache.hw == nil {
			t.Fatal("expected the read+annotated Hardware to still be returned, got nil")
		}
	})

	t.Run("an oversized attributes annotation marks the Workflow Failed, not transient", func(t *testing.T) {
		mock := &mockBackendReadWriter{
			workflow: awaitingCheckInWorkflow(),
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
			},
		}
		server := &Handler{
			Logger:  logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend: mock,
			NowFunc: func() time.Time { return time.Time{} },
		}

		// Exceeding maxAnnotationSize (64KB) is deterministic given the same attrs - it
		// won't resolve on retry, so it must mark the Workflow Failed like a genuine
		// not-found, not be treated as a transient error left to silently re-fail on
		// every future check-in forever.
		attrs := &data.AgentAttributes{
			BlockDevices: []*data.Block{{Name: toPtr(strings.Repeat("x", 100_000))}},
		}
		_, cache, err := server.renderOnCheckIn(context.Background(), awaitingCheckInWorkflow(), attrs, nil)
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if cache != nil {
			t.Fatalf("expected no cache to be returned for a permanent failure, got %+v", cache)
		}
		if mock.updatedWorkflow == nil {
			t.Fatal("expected the Workflow status to be persisted")
		}
		if mock.updatedWorkflow.Status.State != tinkerbell.WorkflowStateFailed {
			t.Fatalf("expected Workflow to be marked %q, got %q", tinkerbell.WorkflowStateFailed, mock.updatedWorkflow.Status.State)
		}
	})

	t.Run("an annotation-refresh failure is not cached for a sibling to inherit", func(t *testing.T) {
		bad := awaitingCheckInWorkflow()
		bad.Name = "wf-bad-attrs"

		good := awaitingCheckInWorkflow()
		good.Name = "wf-good"

		mock := &mockBackendReadWriter{
			// Both share HardwareRef "my-hw" and this same check-in's (oversized) attrs.
			// wf-bad-attrs's annotation-refresh failure must not be cached, so wf-good
			// still gets its own independent ReadHardware attempt rather than silently
			// inheriting a partial (read-but-not-annotated, References never attempted)
			// cache entry - it would fail identically here too (same oversized attrs),
			// but that's incidental to this specific request, not something a future,
			// differently-sized check-in should be permanently denied by way of a stale
			// cached entry.
			workflows: []tinkerbell.Workflow{*bad, *good},
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
			},
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		_, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId: toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{
				Block: []*proto.Block{{Name: toPtr(strings.Repeat("x", 100_000))}},
			},
		})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if mock.readHardwareCalls != 2 {
			t.Fatalf("expected wf-good to independently re-read Hardware rather than reuse a poisoned cache entry, got %d ReadHardware calls", mock.readHardwareCalls)
		}
	})

	t.Run("a resourceVersion conflict on the final persist aborts instead of silently serving a different sibling", func(t *testing.T) {
		fresh := awaitingCheckInWorkflow()
		fresh.Name = "wf-fresh"

		unrelated := tinkerbell.Workflow{
			ObjectMeta: metav1.ObjectMeta{Name: "wf-unrelated", Namespace: "default"},
			Spec:       tinkerbell.WorkflowSpec{TemplateRef: "debian", HardwareRef: "my-hw"},
			Status: tinkerbell.WorkflowStatus{
				State: tinkerbell.WorkflowStatePending,
				Tasks: []tinkerbell.Task{
					{
						Name:    "provision",
						AgentID: "machine-mac-1",
						Actions: []tinkerbell.Action{
							{Name: "other-action", State: tinkerbell.WorkflowStatePending},
						},
					},
				},
			},
		}

		mock := &mockBackendReadWriter{
			// wf-fresh renders successfully but loses the race persisting it; wf-unrelated
			// is already servable. doGetAction must abort with the conflict rather than
			// silently falling through and serving wf-unrelated instead this round.
			workflows: []tinkerbell.Workflow{*fresh, unrelated},
			hardware: &tinkerbell.Hardware{
				ObjectMeta: metav1.ObjectMeta{Name: "my-hw", Namespace: "default"},
				Spec:       tinkerbell.HardwareSpec{AgentID: "machine-mac-1"},
			},
			template: &tinkerbell.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "debian", Namespace: "default"},
				Spec:       tinkerbell.TemplateSpec{Data: toPtr(checkInTemplateData)},
			},
			writeErr: kerrors.NewConflict(schema.GroupResource{Group: "tinkerbell.org", Resource: "workflows"}, "wf-fresh", errors.New("resourceVersion mismatch")),
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		_, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId: toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{
				Block: []*proto.Block{{Name: toPtr("sda"), SizeBytes: toPtr(int64(1_000_000_000_000))}},
			},
		})
		if err == nil {
			t.Fatal("expected a conflict error to abort the request, got nil")
		}
		if status.Code(err) != codes.Aborted {
			t.Fatalf("expected codes.Aborted, got %v (%v)", status.Code(err), err)
		}
	})

	t.Run("a transient failure with no other candidate surfaces the real error, not a generic NotFound", func(t *testing.T) {
		mock := &mockBackendReadWriter{
			workflow:    awaitingCheckInWorkflow(),
			hardwareErr: errors.New("boom: hardware backend unavailable"),
		}
		server := &Handler{
			Logger:       logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil)),
			Backend:      mock,
			NowFunc:      func() time.Time { return time.Time{} },
			RetryOptions: []backoff.RetryOption{backoff.WithMaxTries(1)},
		}

		_, err := server.GetAction(context.Background(), &proto.ActionRequest{
			AgentId: toPtr("machine-mac-1"),
			AgentAttributes: &proto.AgentAttributes{
				Block: []*proto.Block{{Name: toPtr("sda"), SizeBytes: toPtr(int64(1_000_000_000_000))}},
			},
		})
		if err == nil {
			t.Fatal("expected an error, got nil")
		}
		if status.Code(err) == codes.NotFound {
			t.Fatalf("expected the real transient error to surface, not a generic NotFound: %v", err)
		}
		if !strings.Contains(err.Error(), "hardware backend unavailable") {
			t.Fatalf("expected the underlying transient error to be included, got %v", err)
		}
	})
}
