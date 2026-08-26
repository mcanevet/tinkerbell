package data

// HardwareFilter holds selectors for finding Hardware objects.
// Multiple selectors can be set and are AND-ed.
// InNamespace optionally scopes any selector.
type HardwareFilter struct {
	InNamespace  string
	ByName       string
	ByAgentID    string
	ByMACAddress string
	ByIPAddress  string
	ByInstanceID string
}

// WorkflowFilter holds selectors for listing Workflows.
type WorkflowFilter struct {
	InNamespace string
	ByAgentID   string
}

// UpdateOptions holds all the parameters that can be used to update an object.
type UpdateOptions struct {
	StatusOnly bool

	// PatchFrom, when non-nil, signals that the backend should compute a merge-patch
	// between this original object and the modified object passed to the Update call.
	// The caller is expected to pass a DeepCopy taken before any mutations.
	// The concrete type must be compatible with the backend (e.g. client.Object for the kube backend).
	PatchFrom any

	// OptimisticLock, when true alongside PatchFrom, adds a resourceVersion precondition
	// to the generated patch (the kube backend's client.MergeFromWithOptimisticLock), so
	// the Update call fails with a conflict rather than silently overwriting if the object
	// changed since PatchFrom's snapshot was taken. Use for read-modify-write sequences
	// where two callers racing to persist the same first-time transition (and each
	// winning silently) would be a real correctness problem, not just a wasted write.
	OptimisticLock bool

	// RawPatch, when non-nil, signals that the backend should apply a raw patch.
	RawPatch []byte

	// RawPatchType specifies the patch strategy. Supported Kubernetes patch types:
	//   - "application/json-patch+json"            (JSON Patch, RFC 6902: array of {op, path, value} operations)
	//   - "application/merge-patch+json"           (JSON Merge Patch, RFC 7386: partial JSON merged into the object)
	//   - "application/strategic-merge-patch+json" (Strategic Merge Patch: Kubernetes-specific, merges arrays by key)
	//   - "application/apply-patch+yaml"           (Server-Side Apply: field ownership tracking, requires fieldManager)
	RawPatchType string
}
