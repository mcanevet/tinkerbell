package render

import (
	"testing"

	v1alpha1 "github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
)

// combinedFieldsTemplate exercises every template data source RenderWorkflow merges
// into one root: DataKeyHardwareLegacy (title-cased .Hardware.*), DataKeyHardware
// (lowercase .hardware.*), HardwareMap (top-level keys), and DataKeyReferences
// (.references.*) - together, since NewInput assembles all of them into a single Input
// and a regression could silently drop or collide one against another.
const combinedFieldsTemplate = `version: "0.1"
name: combined
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
        environment:
          AGENT_ID: "{{ .hardware.spec.agentID }}"
          LEGACY_DISK: "{{ index .Hardware.Disks 0 }}"
          REFERENCE_DISK_PATH: "{{ .references.hw.diskPath }}"
`

func TestRenderWorkflowCombinedFields(t *testing.T) {
	wf := &v1alpha1.Workflow{}
	wf.Name = "combined"
	wf.Spec.HardwareMap = map[string]string{"device_1": "machine-mac-1"}

	tpl := &v1alpha1.Template{}
	tpl.Spec.Data = valueToPointer(combinedFieldsTemplate)

	hw := v1alpha1.Hardware{
		Spec: v1alpha1.HardwareSpec{
			AgentID: "machine-mac-1",
			Disks:   []v1alpha1.Disk{{Device: "/dev/nvme0n1"}},
		},
	}

	references := map[string]interface{}{
		"hw": map[string]interface{}{"diskPath": "/dev/nvme0n1"},
	}

	status, err := RenderWorkflow(NewInput(wf, tpl, hw, references))
	if err != nil {
		t.Fatalf("RenderWorkflow() error = %v", err)
	}

	if len(status.Tasks) != 1 || len(status.Tasks[0].Actions) != 1 {
		t.Fatalf("expected exactly 1 Task with 1 Action, got %+v", status.Tasks)
	}
	if status.Tasks[0].AgentID != "machine-mac-1" {
		t.Errorf("expected the Task's worker to resolve from HardwareMap, got %q", status.Tasks[0].AgentID)
	}

	env := status.Tasks[0].Actions[0].Environment
	if got := env["AGENT_ID"]; got != "machine-mac-1" {
		t.Errorf("expected AGENT_ID from .hardware.spec.agentID, got %q", got)
	}
	if got := env["LEGACY_DISK"]; got != "/dev/nvme0n1" {
		t.Errorf("expected LEGACY_DISK from .Hardware.Disks, got %q", got)
	}
	if got := env["REFERENCE_DISK_PATH"]; got != "/dev/nvme0n1" {
		t.Errorf("expected REFERENCE_DISK_PATH from .references.hw.diskPath, got %q", got)
	}
}
