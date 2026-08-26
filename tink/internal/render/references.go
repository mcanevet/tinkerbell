package render

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"quamina.net/go/quamina"
)

// ReferenceRules holds the allow/deny-list rules used to decide whether a
// Hardware.Spec.References entry may be resolved and exposed to a Template.
type ReferenceRules struct {
	Allowlist []string
	Denylist  []string
}

// DefaultDenylist denies every Hardware.Spec.References entry. It's the effective
// policy whenever an operator hasn't configured an explicit deny-list rule, shared by
// both the workflow controller (tink/controller/controller.go) and tink-server's
// check-in-time render (cmd/tinkerbell/cmd.go) so a Template can't resolve arbitrary
// References by default on one render path but not the other.
var DefaultDenylist = []string{`{"reference": {"name": [{"wildcard": "*"}]}}`}

// DynamicReader reads a single arbitrary-GVR object as an unstructured map. Both the
// workflow controller and tink-server's render-on-checkin path need this to resolve
// Hardware.Spec.References; each supplies its own implementation (a Kubernetes dynamic
// client, in both current cases).
type DynamicReader interface {
	DynamicRead(ctx context.Context, gvr schema.GroupVersionResource, name, namespace string) (map[string]interface{}, error)
}

// ResolveReferences evaluates hardware.Spec.References against rules and reads each
// allowed reference via dc, returning a map keyed by reference name suitable for
// Input.References. References that are denied, fail evaluation, or fail to read are
// skipped rather than aborting the others; all such failures are joined into the
// returned error so the caller can log or surface them, but a partial reference map is
// still usable for rendering.
func ResolveReferences(ctx context.Context, dc DynamicReader, rules ReferenceRules, hardware tinkerbell.Hardware) (map[string]interface{}, error) {
	references := make(map[string]interface{})
	if len(hardware.Spec.References) == 0 {
		// Nothing to evaluate rules against - skip building matchers entirely rather
		// than paying for two quamina.New()+AddPattern calls on every check-in/creation
		// render for the common case of a Hardware object with no References at all.
		return references, nil
	}
	var errs error

	// The deny/allow rules are the same for every Reference entry below - build each
	// matcher once per call rather than once per entry. This now runs synchronously on
	// tink-server's hot check-in path (not just once per Workflow creation, as in the
	// reconciler), so rebuilding a quamina matcher per entry is avoidable overhead on a
	// request path an Agent is actively waiting on.
	denyMatcher, denyErr := newRuleMatcher(rules.Denylist)
	allowMatcher, allowErr := newRuleMatcher(rules.Allowlist)

	for refName, rf := range hardware.Spec.References {
		ed := evaluationData{
			Source: source{
				Name:      hardware.Name,
				Namespace: hardware.Namespace,
			},
			Reference: rf,
		}
		if denyErr != nil {
			errs = errors.Join(errs, fmt.Errorf("evaluating denylist for reference %q: %w", refName, denyErr))
			continue
		}
		denied, _, err := evaluate(denyMatcher, ed)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("evaluating denylist for reference %q: %w", refName, err))
			continue
		}
		if allowErr != nil {
			errs = errors.Join(errs, fmt.Errorf("evaluating allowlist for reference %q: %w", refName, allowErr))
			continue
		}
		allowed, _, err := evaluate(allowMatcher, ed)
		if err != nil {
			errs = errors.Join(errs, fmt.Errorf("evaluating allowlist for reference %q: %w", refName, err))
			continue
		}
		if denied && !allowed {
			errs = errors.Join(errs, fmt.Errorf("reference %q denied", refName))
			continue
		}
		gvr := schema.GroupVersionResource{Group: rf.Group, Version: rf.Version, Resource: rf.Resource}
		if v, err := dc.DynamicRead(ctx, gvr, rf.Name, rf.Namespace); err == nil || v != nil {
			references[refName] = v
		} else {
			errs = errors.Join(errs, fmt.Errorf("reading reference %q: %w", refName, err))
		}
	}
	return references, errs
}

// evaluateData is the data structure used for evaluating rules.
// In Quamina, this is called the "event".
type evaluationData struct {
	// Source is the Object that contains the references.
	Source source `json:"source,omitempty"`
	// Reference is a reference to another Object from the source.
	Reference tinkerbell.Reference `json:"reference,omitempty"`
}

// source is the Object that contains the references.
type source struct {
	// Name is the name of the source object.
	Name string `json:"name,omitempty"`
	// Namespace is the namespace of the source object.
	Namespace string `json:"namespace,omitempty"`
}

// newRuleMatcher builds a quamina matching engine loaded with rules. Callers evaluating
// the same rules against more than one evaluationData (e.g. ResolveReferences, once per
// Hardware.Spec.References entry) should build it once and reuse it via evaluate, rather
// than rebuilding it per entry.
func newRuleMatcher(rules []string) (*quamina.Quamina, error) {
	q, err := quamina.New()
	if err != nil {
		return nil, fmt.Errorf("error creating rule evaluation engine: %w", err)
	}
	for _, r := range rules {
		if err := q.AddPattern(fmt.Sprintf("pattern-%v", r), r); err != nil {
			return nil, fmt.Errorf("error adding matching pattern: %v err: %w", r, err)
		}
	}
	return q, nil
}

// evaluate checks if data matches any pattern loaded into q.
// It returns a boolean indicating if at least one rule was matched, the rule that matched for the decision, and an error if any occurred.
func evaluate(q *quamina.Quamina, data evaluationData) (bool, string, error) {
	jsonEvent, err := json.Marshal(&data)
	if err != nil {
		return false, "", fmt.Errorf("error while marshalling data: %w", err)
	}
	matches, err := q.MatchesForEvent(jsonEvent)
	if err != nil {
		return false, "", fmt.Errorf("error while matching pattern: %w", err)
	}
	if len(matches) == 0 {
		return false, "", nil
	}

	var rs []string
	for idx, match := range matches {
		if m, ok := match.(string); ok {
			rs = append(rs, m)
		} else {
			rs = append(rs, fmt.Sprintf("pattern-%d", idx))
		}
	}

	return true, strings.Join(rs, ";"), nil
}
