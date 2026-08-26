// Package render implements Tinkerbell Workflow Template rendering: turning a
// Template's Go-template data field, plus Hardware context, into a Workflow's
// Status.Tasks. It lives under tink/internal (rather than as a workflow-controller
// private package) specifically so both the workflow controller (rendering at Workflow
// creation - the default, for any Template that doesn't set Spec.RequiresCheckIn) and
// the tink-server gRPC handler (rendering on the target Agent's first check-in - see
// checkin.go - for a Template that does) can call the same rendering core.
package render

import (
	"fmt"

	v1alpha1 "github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
)

const (
	// DataKeyReferences is the key used to access the Hardware references in the template data.
	// This is lowercase as it is new and follows the all lowercase convention used when referencing
	// fields in the reference object.
	DataKeyReferences = "references"
	// DataKeyHardware is the key used to access the Hardware data in the template data.
	DataKeyHardware = "hardware"
	// DataKeyHardwareLegacy is the key used to access the Hardware data in the template data.
	// This is Title cased as it was the original convention used in the template data and is
	// used for backwards compatibility.
	//
	// Deprecated: use DataKeyHardware instead. This key will be removed in a future release.
	DataKeyHardwareLegacy = "Hardware"
)

// Input is everything RenderWorkflow needs to render a Template against a specific
// Hardware instance.
type Input struct {
	// TemplateID is used only for error messages (identifying which Template failed to render).
	TemplateID string
	// TemplateData is the Template's Spec.Data - the Go-template source.
	TemplateData string
	// Hardware is the Hardware object the Workflow targets. Its Spec and Status (including
	// Annotations) are all exposed to the Template under DataKeyHardware. In particular, on
	// the check-in render path, callers are expected to have refreshed
	// Hardware.metadata.annotations["tinkerbell.org/agent-attributes"] with this check-in's
	// attributes before calling RenderWorkflow (see tink/server/internal/grpc/checkin.go) - a
	// Template can then read it back via
	// {{ (index .hardware.metadata.annotations "tinkerbell.org/agent-attributes") | fromJson }}.
	Hardware v1alpha1.Hardware
	// HardwareMap is Workflow.Spec.HardwareMap, merged into the template data root.
	HardwareMap map[string]string
	// References is pre-resolved reference data, keyed by the same names as
	// Hardware.Spec.References. Reference resolution requires a dynamic Kubernetes client,
	// which not every caller has configured - callers without one should pass nil or an
	// empty map, and any Template relying on {{ .references }} will render with an empty
	// references set rather than failing outright.
	References map[string]interface{}
}

// NewInput builds an Input from a Workflow, its Template, and pre-resolved references.
// Both of RenderWorkflow's callers (the workflow controller's create-time render, and
// tink-server's checkin.go check-in-time render) build their Input through this
// constructor rather than each assembling the struct literal by hand, so a field one
// caller needs (e.g. HardwareMap, once missing from the check-in path) can't silently
// end up populated on only one of the two render paths.
func NewInput(wf *v1alpha1.Workflow, tpl *v1alpha1.Template, hardware v1alpha1.Hardware, references map[string]interface{}) Input {
	return Input{
		TemplateID:   wf.Name,
		TemplateData: PointerToValue(tpl.Spec.Data),
		Hardware:     hardware,
		HardwareMap:  wf.Spec.HardwareMap,
		References:   references,
	}
}

// RenderWorkflow renders in.TemplateData against in.Hardware and returns the resulting
// WorkflowStatus (Tasks/AgentID/GlobalTimeout populated), or an error if rendering failed.
//
//nolint:revive // Workflow is already a type in this package; renaming to avoid the "stutter" would collide with it.
func RenderWorkflow(in Input) (*v1alpha1.WorkflowStatus, error) {
	tdata := make(map[string]interface{})
	for key, val := range in.HardwareMap {
		tdata[key] = val
	}

	// structToMap is used so that fields are accessible in Templates by their json struct tag names instead of
	// their Go struct field names and their case.
	// for example, {{ hardware.spec.metadata.instance.id }} instead of {{ hardware.Spec.Metadata.Instance.ID }}.
	//
	// A failure here is surfaced as a render error rather than silently falling back to an
	// empty map: render has no logger dependency by design, so a swallowed failure here
	// would leave a Template silently seeing empty hardware data with no signal anywhere
	// that anything went wrong.
	hwMap, err := structToMap(in.Hardware)
	if err != nil {
		return nil, fmt.Errorf("converting hardware to template data: %w", err)
	}
	tdata[DataKeyHardware] = hwMap
	tdata[DataKeyHardwareLegacy] = toTemplateHardwareData(in.Hardware)

	references := in.References
	if references == nil {
		references = map[string]interface{}{}
	}
	tdata[DataKeyReferences] = references

	tinkWf, err := renderTemplateHardware(in.TemplateID, in.TemplateData, tdata)
	if err != nil {
		return nil, err
	}

	return YAMLToStatus(tinkWf), nil
}
