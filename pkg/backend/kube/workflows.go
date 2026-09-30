package kube

import (
	"context"
	"fmt"

	v1alpha1 "github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"github.com/tinkerbell/tinkerbell/pkg/data"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (b *Backend) CreateWorkflow(ctx context.Context, w *v1alpha1.Workflow) error {
	if err := b.cluster.GetClient().Create(ctx, w); err != nil {
		return fmt.Errorf("failed to create workflow %s: %w", w.Name, err)
	}

	return nil
}

func (b *Backend) ReadWorkflow(ctx context.Context, name, namespace string) (*v1alpha1.Workflow, error) {
	wflw := &v1alpha1.Workflow{}
	if err := b.cluster.GetClient().Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, wflw); err != nil {
		return nil, fmt.Errorf("failed to get workflow %s/%s: %w", namespace, name, err)
	}
	return wflw, nil
}

func (b *Backend) ListWorkflows(ctx context.Context, opts data.WorkflowFilter) ([]v1alpha1.Workflow, error) {
	if opts.ByAgentID == "" {
		return b.listWorkflowsByField(ctx, opts.InNamespace, "", "")
	}

	// A Workflow can address its target the ordinary single-machine way
	// (Spec.HardwareRef, a Hardware object's name) rather than Spec.HardwareMap - resolve
	// the Agent's own Hardware name first, so a Workflow awaiting check-in that uses
	// HardwareRef can also be cross-referenced by it below (WorkflowHardwareRefIndex is
	// keyed by Hardware name, not Agent ID, since a Workflow's field index can't itself
	// perform this cross-object lookup). Any error here (not found, ambiguous, ...) just
	// means this Agent has no uniquely identifiable Hardware object to cross-reference by
	// name; the other two indexes below still apply.
	var hwName string
	if hw, err := b.FilterHardware(ctx, data.HardwareFilter{InNamespace: opts.InNamespace, ByAgentID: opts.ByAgentID}); err == nil {
		hwName = hw.Name
	}

	// Three separate indexes can each name a Workflow as belonging to this Agent:
	// WorkflowAgentIDIndex (status.agentID, only set once rendering has assigned a Task to
	// this Agent), WorkflowHardwareMapIndex (spec.hardwareMap), and WorkflowHardwareRefIndex
	// (spec.hardwareRef, resolved via hwName above) - the latter two are what let a
	// Workflow awaiting its target Agent's first check-in, before it has ever rendered and
	// so has no status.agentID yet, be found at all, however it addresses its Hardware.
	// Query every index that applies (concurrently - this runs on tink-server's hot
	// check-in/action-poll path) and dedupe by UID, since a Workflow can appear in more
	// than one.
	var byStatus, byHardwareMap, byHardwareRef []v1alpha1.Workflow
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		byStatus, err = b.listWorkflowsByField(egCtx, opts.InNamespace, WorkflowAgentIDIndex, opts.ByAgentID)
		return err
	})
	eg.Go(func() error {
		var err error
		byHardwareMap, err = b.listWorkflowsByField(egCtx, opts.InNamespace, WorkflowHardwareMapIndex, opts.ByAgentID)
		return err
	})
	if hwName != "" {
		eg.Go(func() error {
			var err error
			byHardwareRef, err = b.listWorkflowsByField(egCtx, opts.InNamespace, WorkflowHardwareRefIndex, hwName)
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	seen := make(map[types.UID]bool, len(byStatus)+len(byHardwareMap)+len(byHardwareRef))
	merged := make([]v1alpha1.Workflow, 0, len(byStatus)+len(byHardwareMap)+len(byHardwareRef))
	for _, list := range [][]v1alpha1.Workflow{byStatus, byHardwareMap, byHardwareRef} {
		for _, wf := range list {
			if seen[wf.UID] {
				continue
			}
			seen[wf.UID] = true
			merged = append(merged, wf)
		}
	}
	return merged, nil
}

// listWorkflowsByField lists Workflows in namespace, optionally matching a single indexed
// field/value pair. field is left empty to list without any field-index filter.
func (b *Backend) listWorkflowsByField(ctx context.Context, namespace, field, value string) ([]v1alpha1.Workflow, error) {
	stored := &v1alpha1.WorkflowList{}
	los := []client.ListOption{}
	if namespace != "" {
		los = append(los, client.InNamespace(namespace))
	}
	if field != "" {
		los = append(los, client.MatchingFields{field: value})
	}
	if err := b.cluster.GetClient().List(ctx, stored, los...); err != nil {
		return nil, fmt.Errorf("failed to list workflows in namespace %s: %w", namespace, err)
	}

	return stored.Items, nil
}

func (b *Backend) UpdateWorkflow(ctx context.Context, wf *v1alpha1.Workflow, opts data.UpdateOptions) error {
	cc := b.cluster.GetClient()

	if p, err := patchFromOpts(opts); err != nil {
		return fmt.Errorf("invalid patch options for workflow %s: %w", wf.Name, err)
	} else if p != nil {
		if opts.StatusOnly {
			if err := cc.Status().Patch(ctx, wf, p); err != nil {
				return fmt.Errorf("failed to patch workflow status %s: %w", wf.Name, err)
			}
			return nil
		}
		if err := cc.Patch(ctx, wf, p); err != nil {
			return fmt.Errorf("failed to patch workflow %s: %w", wf.Name, err)
		}
		return nil
	}

	if opts.StatusOnly {
		// Only update the status subresource of the workflow. This is used by the tinkerbell server to update the workflow status without having to worry about conflicts with the controller which may be updating the workflow spec at the same time.
		if err := cc.Status().Update(ctx, wf); err != nil {
			return fmt.Errorf("failed to update workflow status %s: %w", wf.Name, err)
		}
		return nil
	}
	if err := cc.Update(ctx, wf); err != nil {
		return fmt.Errorf("failed to update workflow %s: %w", wf.Name, err)
	}

	return nil
}
