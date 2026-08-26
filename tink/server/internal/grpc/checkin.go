package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"github.com/tinkerbell/tinkerbell/pkg/data"
	"github.com/tinkerbell/tinkerbell/pkg/journal"
	"github.com/tinkerbell/tinkerbell/tink/internal/render"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// errPermanentCheckIn marks a renderOnCheckIn failure as permanent (not-found-like) even
// though it isn't a Kubernetes NotFound error - currently only the attributes-annotation
// refresh, whose failure modes (oversized/unmarshalable payload) are deterministic given
// the same input and won't resolve on retry, unlike a transient backend read error.
var errPermanentCheckIn = errors.New("permanent check-in render failure")

// renderOnCheckIn renders wf's Template now, using the Agent's just-received attrs. Only
// reached for Workflows whose Template opted into Spec.RequiresCheckIn - the workflow
// controller's reconciler (tink/controller/internal/workflow) leaves those, and only
// those, in WorkflowStateAwaitingCheckIn instead of rendering them itself at creation.
//
// Rendering is a pure function of (Template, Hardware, attrs), so a retried or duplicate
// first check-in re-renders identically - safe to call more than once for the same
// Workflow if doGetAction's caller retries.
//
// Hardware.Spec.References are resolved the same way the workflow controller does, via
// h.DynamicClient/h.ReferenceRules - see render.ResolveReferences. If h.DynamicClient is
// nil (not configured), References are simply left unresolved rather than failing the
// render outright.
//
// Returns the Hardware object read (and attributes-annotated) and its resolved
// References along the way, bundled into a checkInCache, so the caller can reuse them
// (as hwRef, and skip re-resolving References) instead of resolveAndAnnotateHardware
// reading and re-annotating Hardware a second time for the same check-in. Returned even
// on a failure that happens after Hardware was obtained (template/render errors), so a
// caller iterating several sibling Workflows can still cache and reuse it for the next
// one.
//
// cached, when non-nil, bundles a Hardware object and References already
// resolved by an earlier renderOnCheckIn call for this same check-in request (see
// doGetAction's hwCache) - reusing it skips redundant work when more than one
// AwaitingCheckIn sibling Workflow shares the same Spec.HardwareRef (e.g. one that fails
// to render before a later, servable sibling).
//
// Only a failure known to be permanent - a genuine not-found reading Hardware or
// Template, an attributes-annotation refresh failure (see errPermanentCheckIn - always
// deterministic given the same input), or the render itself failing (a deterministic
// function of inputs that won't change on retry) - marks wf Failed. Any other error (a
// transient API hiccup, say) is returned as-is without touching wf.Status, so wf stays
// AwaitingCheckIn and indexed - exactly like the immediate-render path this replaces,
// which never sets .Status.State on a read error and relies on the caller retrying.
func (h *Handler) renderOnCheckIn(ctx context.Context, wf *tinkerbell.Workflow, attrs *data.AgentAttributes, cached *checkInCache) (*tinkerbell.Workflow, *checkInCache, error) {
	original := wf.DeepCopy()

	hw, references, err := h.hardwareAndReferencesForCheckIn(ctx, wf, attrs, cached)
	if err != nil {
		if kerrors.IsNotFound(err) || errors.Is(err, errPermanentCheckIn) {
			return nil, nil, h.failRenderOnCheckIn(ctx, wf, original, err.Error())
		}
		return nil, &checkInCache{hw: hw}, err
	}

	tpl, err := h.Backend.ReadTemplate(ctx, wf.Spec.TemplateRef, wf.Namespace)
	if err != nil {
		cache := &checkInCache{hw: hw, references: references}
		if kerrors.IsNotFound(err) {
			return nil, cache, h.failRenderOnCheckIn(ctx, wf, original, fmt.Sprintf("template not found: %v", err))
		}
		return nil, cache, fmt.Errorf("reading template for deferred render: %w", err)
	}

	renderedStatus, renderErr := render.RenderWorkflow(render.NewInput(wf, tpl, *hw, references))
	if renderErr != nil {
		return nil, &checkInCache{hw: hw, references: references}, h.failRenderOnCheckIn(ctx, wf, original, fmt.Sprintf("rendering template on check-in: %v", renderErr))
	}

	// Only the fields render.YAMLToStatus actually produces are updated - a wholesale
	// `wf.Status = *renderedStatus` would silently wipe Status.BootOptions/Conditions that
	// the Preparing phase (see pre.go) may have already recorded for this same Workflow.
	wf.Status.GlobalTimeout = renderedStatus.GlobalTimeout
	wf.Status.Tasks = renderedStatus.Tasks
	wf.Status.AgentID = renderedStatus.AgentID
	wf.Status.TemplateRendering = tinkerbell.TemplateRenderingSuccessful
	wf.Status.State = tinkerbell.WorkflowStatePending
	wf.Status.SetCondition(tinkerbell.WorkflowCondition{
		Type:    tinkerbell.TemplateRenderedSuccess,
		Status:  metav1.ConditionTrue,
		Reason:  "Complete",
		Message: "template rendered successfully on Agent check-in",
		Time:    &metav1.Time{Time: metav1.Now().UTC()},
	})

	// OptimisticLock: two concurrent first check-ins for the same AwaitingCheckIn Workflow
	// (e.g. a client retrying on a slow response) must not silently last-write-wins - a
	// resourceVersion conflict here surfaces as an error, which doGetAction's caller
	// (backoff.Retry in GetAction) already retries by re-listing and re-rendering against
	// the now-current Workflow.
	if err := h.Backend.UpdateWorkflow(ctx, wf, data.UpdateOptions{StatusOnly: true, PatchFrom: original, OptimisticLock: true}); err != nil {
		return nil, &checkInCache{hw: hw, references: references}, fmt.Errorf("persisting deferred render: %w", err)
	}

	return wf, &checkInCache{hw: hw, references: references}, nil
}

// hardwareAndReferencesForCheckIn resolves the Hardware object and its References for
// wf's render, or reuses them from cached if already known for this check-in request.
// Returns hw non-nil on a transient (non-permanent) failure that happens after Hardware
// was obtained, so the caller can still cache it for a sibling; hw is read but not fully
// valid (not annotated, References not resolved) on a permanent failure - wrapped with
// errPermanentCheckIn - and the caller must not cache it in that case.
func (h *Handler) hardwareAndReferencesForCheckIn(ctx context.Context, wf *tinkerbell.Workflow, attrs *data.AgentAttributes, cached *checkInCache) (*tinkerbell.Hardware, map[string]interface{}, error) {
	if cached != nil {
		return cached.hw, cached.references, nil
	}

	hw, err := h.resolveHardware(ctx, nil, wf.Spec.HardwareRef, wf.Namespace)
	if err != nil {
		return nil, nil, fmt.Errorf("reading hardware for deferred render: %w", err)
	}
	if hw == nil {
		// Spec.HardwareRef is optional (a Workflow may render entirely from
		// Spec.HardwareMap): same zero-value fallback the legacy processWorkflow render
		// path uses, rather than treating "no Hardware object to look up" as an error.
		return &tinkerbell.Hardware{}, nil, nil
	}

	// Refresh the attributes annotation so it reflects *this* check-in before rendering,
	// so the Template can read it back via
	// {{ (index .hardware.metadata.annotations "tinkerbell.org/agent-attributes") | fromJson }}.
	// overwrite=true: this render needs THIS check-in's live values, not a stale
	// first-ever snapshot (see setHardwareAttributesAnnotation).
	if err := h.setHardwareAttributesAnnotation(ctx, hw, attrs, true); err != nil {
		return hw, nil, fmt.Errorf("refreshing attributes annotation for deferred render: %w: %w", errPermanentCheckIn, err)
	}

	var references map[string]interface{}
	if h.DynamicClient != nil {
		var refErr error
		references, refErr = render.ResolveReferences(ctx, h.DynamicClient, h.ReferenceRules, *hw)
		if refErr != nil {
			journal.Log(ctx, "error resolving one or more references on check-in", "error", refErr)
		}
	}
	return hw, references, nil
}

// checkInCache bundles a Hardware object and its resolved References, both obtained at
// most once per Spec.HardwareRef per check-in request (see doGetAction's hwCache) and
// reused across every AwaitingCheckIn sibling Workflow that shares it - References only
// depend on Hardware.Spec.References, which doesn't change across siblings sharing the
// same Hardware object within one request, so they're safe to resolve once and reuse.
type checkInCache struct {
	hw         *tinkerbell.Hardware
	references map[string]interface{}
}

// failRenderOnCheckIn marks wf Failed with reason as the condition message and
// persists it against original (best-effort - a persist failure is only logged, since
// the caller's own returned error already explains what went wrong). Used only for
// permanent renderOnCheckIn failures (Hardware/Template genuinely not found, or the
// render itself failing) - see renderOnCheckIn's own doc comment for why transient
// errors deliberately don't call this.
func (h *Handler) failRenderOnCheckIn(ctx context.Context, wf, original *tinkerbell.Workflow, reason string) error {
	wf.Status.TemplateRendering = tinkerbell.TemplateRenderingFailed
	wf.Status.State = tinkerbell.WorkflowStateFailed
	wf.Status.SetConditionIfDifferent(tinkerbell.WorkflowCondition{
		Type:    tinkerbell.TemplateRenderedSuccess,
		Status:  metav1.ConditionFalse,
		Reason:  "Error",
		Message: reason,
		Time:    &metav1.Time{Time: metav1.Now().UTC()},
	})
	if err := h.Backend.UpdateWorkflow(ctx, wf, data.UpdateOptions{StatusOnly: true, PatchFrom: original, OptimisticLock: true}); err != nil {
		journal.Log(ctx, "error persisting failed render-on-checkin state", "error", err)
	}
	return fmt.Errorf("%s", reason)
}
