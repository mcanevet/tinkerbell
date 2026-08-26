package kube

import (
	"github.com/tinkerbell/tinkerbell/api/v1alpha1/bmc"
	"github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type IndexType string

const (
	IndexTypeMACAddr             IndexType = MACAddrIndex
	IndexTypeIPAddr              IndexType = IPAddrIndex
	IndexTypeHardwareName        IndexType = "hardware.metadata.name"
	IndexTypeMachineName         IndexType = "machine.metadata.name"
	IndexTypeWorkflowAgentID     IndexType = WorkflowAgentIDIndex
	IndexTypeWorkflowHardwareMap IndexType = WorkflowHardwareMapIndex
	IndexTypeWorkflowHardwareRef IndexType = WorkflowHardwareRefIndex
	IndexTypeHardwareAgentID     IndexType = HardwareAgentIDIndex
	IndexTypeInstanceID          IndexType = InstanceIDIndex

	// MACAddrIndex is an index used with a controller-runtime client to lookup hardware by MAC.
	MACAddrIndex = ".Spec.Interfaces.MAC"

	// IPAddrIndex is an index used with a controller-runtime client to lookup hardware by IP.
	IPAddrIndex = ".Spec.Interfaces.DHCP.IP"

	// NameIndex is an index used with a controller-runtime client to lookup objects by name.
	NameIndex = ".metadata.name"

	// WorkflowAgentIDIndex is an index used with a controller-runtime client to lookup workflows by their status agent id.
	WorkflowAgentIDIndex = ".status.agentID"

	// WorkflowHardwareMapIndex is an index used with a controller-runtime client to lookup
	// workflows by any Agent ID referenced in their spec hardware map.
	WorkflowHardwareMapIndex = ".spec.hardwareMap"

	// WorkflowHardwareRefIndex is an index used with a controller-runtime client to lookup
	// workflows by their spec hardwareRef (a Hardware object's name, not an Agent ID - see
	// WorkflowHardwareRefs).
	WorkflowHardwareRefIndex = ".spec.hardwareRef"

	// HardwareAgentIDIndex is an index used with a controller-runtime client to lookup hardware by their spec agent id.
	HardwareAgentIDIndex = ".spec.agentID"

	// InstanceIDIndex is an index used with a controller-runtime client to lookup hardware by its metadata instance id.
	InstanceIDIndex = ".Spec.Metadata.Instance.ID" // #nosec G101 - This is a field path, not a credential

)

// Indexes that are currently known.
var Indexes = map[IndexType]Index{
	IndexTypeMACAddr: {
		Obj:          &tinkerbell.Hardware{},
		Field:        MACAddrIndex,
		ExtractValue: MACAddrs,
	},
	IndexTypeIPAddr: {
		Obj:          &tinkerbell.Hardware{},
		Field:        IPAddrIndex,
		ExtractValue: IPAddrs,
	},
	IndexTypeHardwareName: {
		Obj:          &tinkerbell.Hardware{},
		Field:        NameIndex,
		ExtractValue: HardwareName,
	},
	IndexTypeMachineName: {
		Obj:          &bmc.Machine{},
		Field:        NameIndex,
		ExtractValue: MachineName,
	},
	IndexTypeWorkflowAgentID: {
		Obj:          &tinkerbell.Workflow{},
		Field:        WorkflowAgentIDIndex,
		ExtractValue: WorkflowAgentID,
	},
	IndexTypeWorkflowHardwareMap: {
		Obj:          &tinkerbell.Workflow{},
		Field:        WorkflowHardwareMapIndex,
		ExtractValue: WorkflowHardwareMapAgentIDs,
	},
	IndexTypeWorkflowHardwareRef: {
		Obj:          &tinkerbell.Workflow{},
		Field:        WorkflowHardwareRefIndex,
		ExtractValue: WorkflowHardwareRefs,
	},
	IndexTypeHardwareAgentID: {
		Obj:          &tinkerbell.Hardware{},
		Field:        HardwareAgentIDIndex,
		ExtractValue: HardwareAgentID,
	},
	IndexTypeInstanceID: {
		Obj:          &tinkerbell.Hardware{},
		Field:        InstanceIDIndex,
		ExtractValue: InstanceID,
	},
}

// MACAddrs returns a list of MAC addresses for a Hardware object.
func MACAddrs(obj client.Object) []string {
	hw, ok := obj.(*tinkerbell.Hardware)
	if !ok {
		return nil
	}
	return GetMACs(hw)
}

// GetMACs retrieves all MACs associated with h.
func GetMACs(h *tinkerbell.Hardware) []string {
	var macs []string
	for _, i := range h.Spec.Interfaces {
		if i.DHCP != nil && i.DHCP.MAC != "" {
			macs = append(macs, i.DHCP.MAC)
		}
	}

	return macs
}

// IPAddrs returns a list of IP addresses for a Hardware object.
func IPAddrs(obj client.Object) []string {
	hw, ok := obj.(*tinkerbell.Hardware)
	if !ok {
		return nil
	}
	return GetIPs(hw)
}

// GetIPs retrieves all IP addresses.
func GetIPs(h *tinkerbell.Hardware) []string {
	var ips []string
	for _, i := range h.Spec.Interfaces {
		if i.DHCP != nil && i.DHCP.IP != nil && i.DHCP.IP.Address != "" {
			ips = append(ips, i.DHCP.IP.Address)
		}
	}
	return ips
}

// HardwareName extracts the name of a Hardware object for field indexing.
func HardwareName(obj client.Object) []string {
	hw, ok := obj.(*tinkerbell.Hardware)
	if !ok {
		return nil
	}
	return []string{hw.Name}
}

// MachineName extracts the name of a BMC Machine object for field indexing.
func MachineName(obj client.Object) []string {
	m, ok := obj.(*bmc.Machine)
	if !ok {
		return nil
	}
	return []string{m.Name}
}

// WorkflowAgentID extracts the agent ID from a Workflow's status for field indexing.
func WorkflowAgentID(obj client.Object) []string {
	wf, ok := obj.(*tinkerbell.Workflow)
	if !ok {
		return nil
	}
	if wf.Status.AgentID == "" {
		return []string{}
	}
	return []string{wf.Status.AgentID}
}

// workflowAwaitingRender reports whether wf is in one of the pre-render states: "" (not
// yet reconciled at all, e.g. a permanently Spec.Disabled Workflow, which never
// progresses past this zero value), WorkflowStatePreparing (boot orchestration, which can
// run for a while and also never sets Status.AgentID), or WorkflowStateAwaitingCheckIn.
//
// Shared by every Workflow field index that needs to find a Workflow before it has ever
// rendered (and so before it has a status.agentID) - WorkflowHardwareMapAgentIDs and
// WorkflowHardwareRefs. Once a Workflow leaves all three states (rendered successfully,
// or otherwise), it's either found by WorkflowAgentID (status.agentID now set) or
// intentionally not re-discoverable by either Spec-based index at all - Workflows are
// never garbage collected, so an unscoped index would resurface every Success/Failed
// Workflow that ever referenced this Agent for the rest of that Workflow's lifetime, and
// doGetAction hard-errors the whole request on the first non-pending/running Workflow it
// sees rather than skipping it.
func workflowAwaitingRender(wf *tinkerbell.Workflow) bool {
	switch wf.Status.State {
	case "", tinkerbell.WorkflowStatePreparing, tinkerbell.WorkflowStateAwaitingCheckIn:
		return true
	default:
		return false
	}
}

// WorkflowHardwareMapAgentIDs extracts every Agent ID (MAC address) referenced by a
// Workflow's Spec.HardwareMap, for field indexing. Unlike WorkflowAgentID (status,
// populated only once a Workflow has rendered), this is known from Spec at creation
// time, so it's what a Workflow awaiting its target Agent's first check-in (before
// rendering has ever run, and Status.AgentID is therefore still empty) can be found by.
// See workflowAwaitingRender for the state scoping.
func WorkflowHardwareMapAgentIDs(obj client.Object) []string {
	wf, ok := obj.(*tinkerbell.Workflow)
	if !ok {
		return nil
	}
	if !workflowAwaitingRender(wf) || len(wf.Spec.HardwareMap) == 0 {
		return []string{}
	}
	ids := make([]string, 0, len(wf.Spec.HardwareMap))
	for _, agentID := range wf.Spec.HardwareMap {
		ids = append(ids, agentID)
	}
	return ids
}

// WorkflowHardwareRefs extracts a Workflow's Spec.HardwareRef - a Hardware object's
// name, not an Agent ID - for field indexing. This is what lets a Workflow addressed the
// ordinary single-machine way (HardwareRef only, no HardwareMap) be found before it has
// ever rendered: ListWorkflows resolves the target Agent ID to its own Hardware's name
// first (via HardwareAgentIDIndex), then queries this index by that name. See
// workflowAwaitingRender for the state scoping.
func WorkflowHardwareRefs(obj client.Object) []string {
	wf, ok := obj.(*tinkerbell.Workflow)
	if !ok {
		return nil
	}
	if !workflowAwaitingRender(wf) || wf.Spec.HardwareRef == "" {
		return []string{}
	}
	return []string{wf.Spec.HardwareRef}
}

// HardwareAgentID extracts the agent ID from a Hardware's spec for field indexing.
func HardwareAgentID(obj client.Object) []string {
	hw, ok := obj.(*tinkerbell.Hardware)
	if !ok {
		return nil
	}
	if hw.Spec.AgentID == "" {
		return []string{}
	}
	return []string{hw.Spec.AgentID}
}

// InstanceID extracts the instance ID from a Hardware's metadata for field indexing.
func InstanceID(obj client.Object) []string {
	hw, ok := obj.(*tinkerbell.Hardware)
	if !ok {
		return nil
	}
	if hw.Spec.Metadata == nil || hw.Spec.Metadata.Instance == nil || hw.Spec.Metadata.Instance.ID == "" {
		return []string{}
	}
	return []string{hw.Spec.Metadata.Instance.ID}
}
