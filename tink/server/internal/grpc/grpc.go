package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/go-logr/logr"
	"github.com/tinkerbell/tinkerbell/api/v1alpha1/tinkerbell"
	"github.com/tinkerbell/tinkerbell/pkg/constant"
	"github.com/tinkerbell/tinkerbell/pkg/data"
	"github.com/tinkerbell/tinkerbell/pkg/journal"
	"github.com/tinkerbell/tinkerbell/pkg/proto"
	"github.com/tinkerbell/tinkerbell/tink/internal/render"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	errInvalidWorkflowID = "invalid workflow id"
	errInvalidTaskName   = "invalid task name"
	errInvalidActionName = "invalid action name"

	// maxAnnotationSize is the maximum allowed size for agent attributes annotations.
	maxAnnotationSize = 64 * 1024 // 64KB
)

var (
	ErrBackendRead  = errors.New("error reading from backend")
	ErrBackendWrite = errors.New("error writing to backend")
)

type Backend interface {
	WorkflowReader
	WorkflowUpdater
	WorkflowLister
	HardwareReader
	HardwareFilterer
	HardwareUpdater
	HardwareInBandAttributesApplier
	TemplateReader
}

type WorkflowCreator interface {
	CreateWorkflow(ctx context.Context, wf *tinkerbell.Workflow) error
}

// TemplateReader is needed for Workflows whose Template opted into Spec.RequiresCheckIn,
// which render on the target Agent's first check-in instead of at creation time -
// rendering (see renderOnCheckIn in checkin.go) needs the Template's data, which
// tink-server otherwise never reads (the workflow controller renders every other
// Workflow itself).
type TemplateReader interface {
	ReadTemplate(ctx context.Context, name, namespace string) (*tinkerbell.Template, error)
}

type WorkflowReader interface {
	ReadWorkflow(ctx context.Context, name, namespace string) (*tinkerbell.Workflow, error)
}

type WorkflowLister interface {
	ListWorkflows(ctx context.Context, opts data.WorkflowFilter) ([]tinkerbell.Workflow, error)
}

type WorkflowUpdater interface {
	UpdateWorkflow(ctx context.Context, wf *tinkerbell.Workflow, opts data.UpdateOptions) error
}

type WorkflowRuleSetLister interface {
	ListWorkflowRuleSets(ctx context.Context, opts data.WorkflowFilter) ([]tinkerbell.WorkflowRuleSet, error)
}

type HardwareReader interface {
	ReadHardware(ctx context.Context, name, namespace string) (*tinkerbell.Hardware, error)
}

type HardwareFilterer interface {
	FilterHardware(ctx context.Context, opts data.HardwareFilter) (*tinkerbell.Hardware, error)
}

type HardwareUpdater interface {
	UpdateHardware(ctx context.Context, hw *tinkerbell.Hardware, opts data.UpdateOptions) error
}

type HardwareInBandAttributesApplier interface {
	ApplyHardwareInBandAttributes(ctx context.Context, name, namespace string, attrs *tinkerbell.Attributes) error
}

type HardwareCreator interface {
	CreateHardware(ctx context.Context, hw *tinkerbell.Hardware) error
}

// Handler is a server that implements a workflow API.
type Handler struct {
	Logger           logr.Logger
	Backend          Backend
	NowFunc          func() time.Time
	AutoCapabilities AutoCapabilities
	RetryOptions     []backoff.RetryOption

	// DynamicClient and ReferenceRules are used by renderOnCheckIn (checkin.go) to resolve
	// Hardware.Spec.References the same way the workflow controller does. DynamicClient may
	// be nil, in which case References are simply left unresolved (empty), same as if none
	// were allow-listed.
	DynamicClient  render.DynamicReader
	ReferenceRules render.ReferenceRules

	proto.UnimplementedWorkflowServiceServer
}

type options struct {
	AutoCapabilities AutoCapabilities
}

type Option func(*Handler)

func NewHandler(opts ...Option) *Handler {
	h := &Handler{
		NowFunc: time.Now,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

func (h *Handler) GetAction(ctx context.Context, req *proto.ActionRequest) (*proto.ActionResponse, error) {
	operation := func() (*proto.ActionResponse, error) {
		opts := options{
			AutoCapabilities: h.AutoCapabilities,
		}
		return h.doGetAction(ctx, req, opts)
	}
	if len(h.RetryOptions) == 0 {
		h.RetryOptions = []backoff.RetryOption{
			backoff.WithMaxElapsedTime(time.Minute),
			backoff.WithBackOff(backoff.NewConstantBackOff(time.Second)),
		}
	}
	// We retry multiple times as we read-write to the Workflow Status and there can be caching and eventually consistent issues
	// that would cause the write to fail. A retry to get the latest Workflow resolves these types of issues.
	resp, err := backoff.Retry(ctx, operation, h.RetryOptions...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (h *Handler) doGetAction(ctx context.Context, req *proto.ActionRequest, opts options) (*proto.ActionResponse, error) {
	select {
	case <-ctx.Done():
		return nil, status.Error(codes.Unavailable, "server shutting down")
	default:
	}

	ctx = journal.New(ctx)
	log := h.Logger.WithValues("agent", req.GetAgentId())
	defer func() {
		log.V(1).Info("GetAction code flow journal", "journal", journal.Journal(ctx))
	}()
	if req.GetAgentId() == "" {
		journal.Log(ctx, "invalid Agent ID")
		return nil, status.Errorf(codes.InvalidArgument, "invalid Agent ID")
	}

	attrs := convert(req.GetAgentAttributes())

	// hwRef is used in auto discovery and enrollment to avoid multiple lookups of the Hardware object.
	var hwRef *tinkerbell.Hardware
	// handle auto discovery
	if opts.AutoCapabilities.Discovery.Enabled {
		journal.Log(ctx, "auto discovery triggered")
		// Check if there is an existing Hardware Object.
		// If not, Discovery creates one.
		hw, err := h.Discover(ctx, req.GetAgentId(), attrs)
		if err != nil {
			journal.Log(ctx, "error auto discovering Hardware", "error", err)
			log.Error(err, "error auto discovering Hardware")
			hw = hwRef
			// We don't return the error here as we don't want to disrupt any Workflows from running.
		}
		hwRef = hw
	}

	wfs, err := h.Backend.ListWorkflows(ctx, data.WorkflowFilter{ByAgentID: req.GetAgentId()})
	if err != nil {
		// TODO: This is where we handle auto capabilities
		journal.Log(ctx, "error getting Workflows", "error", err)
		return nil, errors.Join(ErrBackendRead, status.Errorf(codes.Internal, "error getting workflows: %v", err))
	}
	if len(wfs) == 0 {
		if opts.AutoCapabilities.Enrollment.Enabled {
			journal.Log(ctx, "auto enrollment triggered")
			// If auto discovery is disabled, we do a Hardware object lookup.
			// If auto discovery is enabled, we rely on the lookup and/or creation of a Hardware object from the Discover method.
			// This means that only one Hardware lookup call is every made to the backend.
			if !opts.AutoCapabilities.Discovery.Enabled {
				if hw, err := h.hardware(ctx, req.GetAgentId()); err == nil {
					hwRef = hw
				}
			}
			return h.enroll(ctx, req.GetAgentId(), attrs, hwRef)
		}
		journal.Log(ctx, "no Workflow found")
		return nil, status.Error(codes.NotFound, "no Workflows found")
	}

	journal.Log(ctx, "found Workflows", "workflows", len(wfs))
	var wf tinkerbell.Workflow
	// annotatedOnCheckIn tracks whether wf was just rendered (and its Hardware read and
	// attributes-annotated) by renderOnCheckIn below, so the isFirstAction branch further
	// down can skip resolveAndAnnotateHardware's otherwise-redundant second read+write of
	// the same Hardware object for the same check-in.
	var annotatedOnCheckIn bool
	// hwCache holds every Hardware object (and its resolved References) already obtained
	// by renderOnCheckIn for this request, keyed by namespace/HardwareRef - so two or more
	// AwaitingCheckIn sibling Workflows sharing the same HardwareRef (e.g. one that fails
	// to render before a later, servable sibling) don't each redundantly re-read/re-patch
	// Hardware or re-resolve References for this same check-in.
	hwCache := map[string]*checkInCache{}
	// lastTransientErr records the most recent transient (not permanent, not a
	// conflict) renderOnCheckIn failure across candidates - if the loop ends without
	// selecting anything, the final fallback below surfaces this instead of a generic
	// NotFound, so a real (if temporary) backend problem isn't misreported as "no
	// Workflow found".
	var lastTransientErr error
	for i := range wfs {
		w := wfs[i]
		// candidateHW/candidateAnnotated hold this iteration's renderOnCheckIn result, if
		// any, local until w is actually selected below - committing them to
		// hwRef/annotatedOnCheckIn eagerly would leak this candidate's Hardware onto a
		// later, different Workflow if this one is abandoned (e.g. renders to zero Tasks).
		var candidateHW *tinkerbell.Hardware
		var candidateAnnotated bool
		if w.Status.State == tinkerbell.WorkflowStateAwaitingCheckIn {
			rendered, hw, annotated, ok, abortErr, transientErr := h.renderAwaitingCheckIn(ctx, w, attrs, hwCache)
			if abortErr != nil {
				return nil, status.Errorf(codes.Aborted, "conflict persisting rendered workflow: %v", abortErr)
			}
			if transientErr != nil {
				lastTransientErr = transientErr
			}
			if !ok {
				continue
			}
			w = rendered
			candidateHW = hw
			candidateAnnotated = annotated
		}
		if len(w.Status.Tasks) == 0 {
			continue
		}
		servable, err := workflowServable(ctx, w)
		if err != nil {
			return nil, err
		}
		if !servable {
			continue
		}
		wf = w
		if candidateHW != nil {
			hwRef = candidateHW
		}
		annotatedOnCheckIn = candidateAnnotated
		journal.Log(ctx, "found Workflow", "workflow", wf.Name)
		break
	}
	if len(wf.Status.Tasks) == 0 {
		return nil, noServableWorkflowError(ctx, lastTransientErr)
	}

	var task *tinkerbell.Task
	if isFirstAction(wf.Status.Tasks[0]) {
		task = &wf.Status.Tasks[0]
		journal.Log(ctx, "first Task, first Action")
		if !annotatedOnCheckIn {
			hwRef = h.resolveAndAnnotateHardware(ctx, log, hwRef, wf.Spec.HardwareRef, wf.Namespace, attrs)
		}
	} else {
		for i := range wf.Status.Tasks {
			// check if all actions have been run successfully in this task.
			// if so continue to the next task.
			if isTaskSuccessful(wf.Status.Tasks[i]) {
				continue
			}
			task = &wf.Status.Tasks[i]
			journal.Log(ctx, "found Task", "taskID", task.ID)
			break
		}
		if task == nil {
			journal.Log(ctx, "no Tasks found")
			return nil, status.Error(codes.NotFound, "no Tasks found")
		}
	}

	if len(task.Actions) == 0 {
		journal.Log(ctx, "no Actions found")
		return nil, status.Error(codes.NotFound, "no Actions found")
	}

	action, err := resolveAction(ctx, task, wf.Status.CurrentState)
	if err != nil {
		return nil, err
	}
	// This check goes after the action is found, so that multi task Workflows can be handled.
	if task.AgentID != req.GetAgentId() {
		journal.Log(ctx, "Task not assigned to Agent")
		return nil, status.Error(codes.NotFound, "Task not assigned to Agent")
	}

	// Applied independently of the legacy annotation above, and on whichever Task's first Action is
	// currently being served (not just the Workflow's overall first Task): the Agent handling the
	// Workflow's first Task is often an administrative Agent (e.g. for Netbox/network setup), not the
	// Agent running on the target Hardware itself. Deferred until after the Task-assignment check above
	// so an unassigned/spoofed Agent can't get its attributes written to Hardware status.
	if isFirstAction(*task) {
		h.resolveAndApplyInBandAttributes(ctx, log, hwRef, wf.Spec.HardwareRef, wf.Namespace, req.GetAgentId(), attrs)
	}

	// update the current state
	// populate the current state and then send the action to the client.
	wf.Status.CurrentState = &tinkerbell.CurrentState{
		AgentID:    req.GetAgentId(),
		TaskID:     task.ID,
		ActionID:   action.ID,
		State:      action.State,
		ActionName: action.Name,
		TaskName:   task.Name,
	}

	if err := h.Backend.UpdateWorkflow(ctx, &wf, data.UpdateOptions{StatusOnly: true}); err != nil {
		return nil, errors.Join(ErrBackendWrite, status.Errorf(codes.Internal, "error writing current state: %v", err))
	}

	ar := &proto.ActionResponse{
		WorkflowId: toPtr(wf.Namespace + "/" + wf.Name),
		TaskId:     toPtr(task.ID),
		AgentId:    toPtr(req.GetAgentId()),
		ActionId:   toPtr(action.ID),
		Name:       toPtr(action.Name),
		Image:      toPtr(action.Image),
		Timeout:    toPtr(action.Timeout),
		Command:    action.Command,
		Volumes:    append(task.Volumes, action.Volumes...),
		Environment: func() []string {
			// add task environment variables to the action environment variables.
			joined := map[string]string{}
			maps.Copy(joined, task.Environment)
			maps.Copy(joined, action.Environment)
			resp := []string{}
			for k, v := range joined {
				resp = append(resp, fmt.Sprintf("%s=%s", k, v))
			}
			sort.Strings(resp)
			return resp
		}(),
		Pid: toPtr(action.Pid), //nolint:staticcheck // intentionally read the deprecated top-level Pid for backward compatibility; namespaces.pid overrides it below when set
	}
	if action.Namespaces != nil {
		// Pass the namespace values through to the agent as-is. The container
		// runtime interprets them (e.g. network "host" shares the host's
		// network namespace); the server does not impose any semantics.
		ar.Namespaces = &proto.Namespaces{
			Network: toPtr(action.Namespaces.Network),
			Pid:     toPtr(action.Namespaces.PID),
		}
		// Prefer namespaces.pid over the deprecated top-level pid field when set.
		if action.Namespaces.PID != "" {
			ar.Pid = toPtr(action.Namespaces.PID)
		}
	}

	log.Info("sending action", "action", ar, "actionID", action.ID)
	journal.Log(ctx, "sending Action", "action", ar)
	return ar, nil
}

// workflowServable reports whether w (already past the AwaitingCheckIn render step, with
// populated Tasks) can be served this round. err is non-nil when doGetAction should
// abort the whole request: w is Preparing (still running boot orchestration - the Agent
// should wait and retry, not fall through to a different Workflow) or in some other
// unexpected non-terminal, non-Pending/Running state. A false, nil return means skip w
// and keep looking: a terminal Workflow (Success/Failed/Timeout) will never become
// servable - unlike Preparing, it doesn't warrant aborting the whole request. Workflows
// are never garbage collected, and ListWorkflows always lists an Agent's already-rendered
// Workflows before any of its still-AwaitingCheckIn ones, so without this, any Agent that
// ever completed a prior Workflow could never be served a new one.
func workflowServable(ctx context.Context, w tinkerbell.Workflow) (servable bool, err error) {
	// Don't serve Actions when in a tinkerbell.WorkflowStatePreparing state.
	// This is to prevent the Agent from starting Actions before Workflow boot options are performed.
	if w.Spec.BootOptions.BootMode != "" && w.Status.State == tinkerbell.WorkflowStatePreparing {
		journal.Log(ctx, "Workflow is in preparing state")
		return false, status.Error(codes.FailedPrecondition, "Workflow is in preparing state")
	}
	if w.Status.State == tinkerbell.WorkflowStatePending || w.Status.State == tinkerbell.WorkflowStateRunning {
		return true, nil
	}
	switch w.Status.State {
	case tinkerbell.WorkflowStateSuccess, tinkerbell.WorkflowStateFailed, tinkerbell.WorkflowStateTimeout:
		journal.Log(ctx, "workflow in terminal state, skipping", "workflow", w.Name, "state", w.Status.State)
		return false, nil
	default:
	}
	journal.Log(ctx, "Workflow not in pending or running state")
	return false, status.Error(codes.FailedPrecondition, "Workflow not in pending or running state")
}

// noServableWorkflowError builds doGetAction's error for "no candidate Workflow ended up
// servable". lastTransientErr, when non-nil, means no candidate was ever actually
// rendered because of a transient failure (not a broken Template/Hardware, which would
// have been reported Failed and simply skipped) - surfaced instead of a generic NotFound,
// so this isn't misreported as "no Workflow exists" when the real cause is a temporary
// backend problem GetAction's own outer retry can recover from.
func noServableWorkflowError(ctx context.Context, lastTransientErr error) error {
	if lastTransientErr != nil {
		journal.Log(ctx, "no servable Workflow found; last error was transient", "error", lastTransientErr)
		return errors.Join(ErrBackendRead, status.Errorf(codes.Unavailable, "error rendering template on check-in: %v", lastTransientErr))
	}
	journal.Log(ctx, "no Tasks found in Workflow")
	return status.Error(codes.NotFound, "no Tasks found in Workflow")
}

// renderAwaitingCheckIn attempts to render an AwaitingCheckIn candidate Workflow w using
// attrs, updating hwCache with anything renderOnCheckIn obtained along the way (see
// doGetAction's hwCache).
//
// ok is true when w was actually rendered and should be considered as the caller's
// selection loop candidate. ok is false with a nil abortErr when this candidate should
// simply be passed over (continue the loop) rather than considered further: no
// attributes yet on this check-in, or a render failure (renderOnCheckIn has already
// persisted the terminal ones as Failed).
//
// abortErr is non-nil only for a resourceVersion conflict on the final persist: w
// actually rendered successfully but lost a race to save it, so retrying this exact
// candidate (a fresh doGetAction attempt against current state) will very likely
// succeed immediately - unlike a genuinely broken sibling, this isn't something a
// different candidate can substitute for. The caller should abort the whole request with
// abortErr so GetAction's outer backoff.Retry redoes it, rather than silently serving a
// different, possibly wrong Workflow this round.
//
// renderErr, when ok is false and abortErr is nil, is renderOnCheckIn's actual failure
// (permanent or transient - not reclassified here) purely so the caller can surface it if
// no candidate ends up servable, instead of a generic NotFound that would otherwise hide
// a real (if temporary) backend problem behind "no Workflow found".
func (h *Handler) renderAwaitingCheckIn(ctx context.Context, w tinkerbell.Workflow, attrs *data.AgentAttributes, hwCache map[string]*checkInCache) (rendered tinkerbell.Workflow, hw *tinkerbell.Hardware, annotated, ok bool, abortErr, renderErr error) {
	if attrs == nil {
		// No attributes on this check-in yet (e.g. an Agent build predating attribute
		// reporting) - nothing to render with. Leave the Workflow waiting; the Agent
		// will retry.
		journal.Log(ctx, "workflow awaiting check-in, no attributes on this request", "workflow", w.Name)
		return w, nil, false, false, nil, nil
	}
	cacheKey := w.Namespace + "/" + w.Spec.HardwareRef
	result, cache, err := h.renderOnCheckIn(ctx, &w, attrs, hwCache[cacheKey])
	// Cache regardless of err: renderOnCheckIn still returns what it obtained (read and
	// attributes-annotated Hardware, resolved References) on a failure that happens after
	// acquiring them, e.g. a broken Template - a later sibling sharing this HardwareRef
	// can still reuse them.
	if cache != nil && cache.hw != nil && w.Spec.HardwareRef != "" {
		hwCache[cacheKey] = cache
	}
	if err != nil {
		if kerrors.IsConflict(err) {
			return w, nil, false, false, err, nil
		}
		// Don't abort the whole request over one broken Workflow: this Agent ID can have
		// more than one Workflow outstanding (e.g. an administrative Agent shared across
		// Workflows), and a sibling Workflow later in wfs may still be perfectly
		// servable. renderOnCheckIn has already persisted this one as Failed, so it
		// won't be re-selected on a future check-in.
		journal.Log(ctx, "error rendering template on check-in", "error", err, "workflow", w.Name)
		return w, nil, false, false, nil, err
	}
	// annotated is always true on success, even when w.Spec.HardwareRef == "" and
	// cache.hw is just the zero-value placeholder hardwareAndReferencesForCheckIn falls
	// back to (never actually annotated, nothing to annotate). This is deliberate, not
	// an approximation: resolveAndAnnotateHardware/resolveHardware only ever check
	// hwRef's nilness, not whether it's a meaningful object, so if annotated were false
	// here doGetAction would call resolveAndAnnotateHardware with this non-nil empty
	// placeholder as hwRef - which resolveHardware returns unchanged (skipping its own
	// hardwareRef=="" guard entirely) - and setHardwareAttributesAnnotation would then
	// try to UpdateHardware an empty-Name object with no such guard of its own.
	return *result, cache.hw, true, true, nil, nil
}

// resolveAction determines which action to serve for the given task based on the workflow's current state.
func resolveAction(ctx context.Context, task *tinkerbell.Task, currentState *tinkerbell.CurrentState) (*tinkerbell.Action, error) {
	switch {
	case isFirstAction(*task):
		journal.Log(ctx, "first Action")
		return &task.Actions[0], nil

	case currentState != nil && (currentState.State == tinkerbell.WorkflowStateRunning || currentState.State == tinkerbell.WorkflowStatePending):
		// Re-serve the current action. This handles two restart scenarios:
		// 1. Server restarts after serving an action but before the agent reports RUNNING
		//    (currentState persisted as PENDING by GetAction).
		// 2. Server restarts while the agent is executing (currentState is RUNNING).
		for idx := range task.Actions {
			if task.Actions[idx].ID == currentState.ActionID {
				journal.Log(ctx, "re-serving in-progress Action", "actionID", task.Actions[idx].ID, "state", currentState.State)
				return &task.Actions[idx], nil
			}
		}
		journal.Log(ctx, "in-progress Action not found in task")
		return nil, status.Error(codes.NotFound, "in-progress Action not found in task")

	default:
		// Get the next Action after the one defined in the current state.
		if currentState == nil {
			journal.Log(ctx, "no current state available")
			return nil, status.Error(codes.FailedPrecondition, "no current state available")
		}
		if currentState.State != tinkerbell.WorkflowStateSuccess {
			journal.Log(ctx, "current Action not in success state")
			return nil, status.Error(codes.FailedPrecondition, "current Action not in success state")
		}
		for idx, act := range task.Actions {
			if act.ID == currentState.ActionID {
				if idx == len(task.Actions)-1 {
					journal.Log(ctx, "last Action in task")
					return nil, status.Error(codes.NotFound, "last Action in task")
				}
				journal.Log(ctx, "found Action", "actionID", task.Actions[idx+1].ID)
				return &task.Actions[idx+1], nil
			}
		}
		journal.Log(ctx, "no Action found")
		return nil, status.Error(codes.NotFound, "no Action found")
	}
}

// isFirstAction checks if the Task is at the first Action.
func isFirstAction(t tinkerbell.Task) bool {
	if len(t.Actions) == 0 {
		return false
	}
	if t.Actions[0].State == tinkerbell.WorkflowStatePending {
		return true
	}
	return false
}

func isTaskSuccessful(t tinkerbell.Task) bool {
	if len(t.Actions) == 0 {
		return true
	}
	if t.Actions[len(t.Actions)-1].State == tinkerbell.WorkflowStateSuccess {
		return true
	}
	return false
}

func (h *Handler) ReportActionStatus(ctx context.Context, req *proto.ActionStatusRequest) (*proto.ActionStatusResponse, error) {
	operation := func() (*proto.ActionStatusResponse, error) {
		return h.doReportActionStatus(ctx, req)
	}
	if len(h.RetryOptions) == 0 {
		h.RetryOptions = []backoff.RetryOption{
			backoff.WithMaxElapsedTime(time.Minute),
			backoff.WithBackOff(backoff.NewConstantBackOff(time.Second)),
		}
	}
	// We retry multiple times as we read-write to the Workflow Status and there can be caching and eventually consistent issues
	// that would cause the write to fail and a retry to get the latest Workflow resolves these types of issues.
	resp, err := backoff.Retry(ctx, operation, h.RetryOptions...)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (h *Handler) doReportActionStatus(ctx context.Context, req *proto.ActionStatusRequest) (*proto.ActionStatusResponse, error) {
	// 1. Validate the request
	if req.GetWorkflowId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, errInvalidWorkflowID)
	}
	if req.GetTaskId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, errInvalidTaskName)
	}
	if req.GetActionId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, errInvalidActionName)
	}
	// 2. Get the workflow
	namespace, name, _ := strings.Cut(req.GetWorkflowId(), "/")
	wf, err := h.Backend.ReadWorkflow(ctx, name, namespace)
	if err != nil {
		return nil, errors.Join(ErrBackendRead, status.Errorf(codes.Internal, "error getting workflow: %v", err))
	}
	// 3. Find the Action in the workflow from the request
	for ti, task := range wf.Status.Tasks {
		for ai, action := range task.Actions {
			// action IDs match or this is the first action in a task
			if action.ID == req.GetActionId() && task.AgentID == req.GetAgentId() {
				wf.Status.Tasks[ti].Actions[ai].State = tinkerbell.WorkflowState(req.GetActionState().String())
				wf.Status.Tasks[ti].Actions[ai].ExecutionStart = &metav1.Time{Time: req.GetExecutionStart().AsTime()}
				wf.Status.Tasks[ti].Actions[ai].ExecutionStop = &metav1.Time{Time: req.GetExecutionStop().AsTime()}
				wf.Status.Tasks[ti].Actions[ai].ExecutionDuration = req.GetExecutionDuration()
				wf.Status.Tasks[ti].Actions[ai].Message = req.GetMessage().GetMessage()

				// 4. Write the updated workflow
				if req.GetActionState() != proto.ActionStatusRequest_SUCCESS {
					wf.Status.State = wf.Status.Tasks[ti].Actions[ai].State
				}
				if len(wf.Status.Tasks) == ti+1 && len(task.Actions) == ai+1 && req.GetActionState() == proto.ActionStatusRequest_SUCCESS {
					// This is the last action in the last task
					wf.Status.State = tinkerbell.WorkflowStatePost
				}

				// update the status current state
				wf.Status.CurrentState = &tinkerbell.CurrentState{
					AgentID:    req.GetAgentId(),
					TaskID:     req.GetTaskId(),
					ActionID:   req.GetActionId(),
					State:      wf.Status.Tasks[ti].Actions[ai].State,
					ActionName: req.GetActionName(),
					TaskName:   wf.Status.Tasks[ti].Name,
				}
				if err := h.Backend.UpdateWorkflow(ctx, wf, data.UpdateOptions{StatusOnly: true}); err != nil {
					return nil, status.Errorf(codes.Internal, "error writing report status: %v", err)
				}
				return &proto.ActionStatusResponse{}, nil
			}
		}
	}

	return &proto.ActionStatusResponse{}, status.Error(codes.NotFound, "action not found")
}

// resolveAndAnnotateHardware resolves the Hardware object for a Workflow and refreshes its
// agent-attributes annotation the first time it's ever seen (write-once - see
// setHardwareAttributesAnnotation). This is only called on the very first action to avoid
// unnecessary backend reads. The resolved Hardware is returned so callers that need it
// again in the same request (e.g. for status.attributes.inBand, or checkin.go's
// renderOnCheckIn) don't have to re-read it from the backend.
func (h *Handler) resolveAndAnnotateHardware(ctx context.Context, log logr.Logger, hwRef *tinkerbell.Hardware, hardwareRef, namespace string, attrs *data.AgentAttributes) *tinkerbell.Hardware {
	if attrs == nil {
		return hwRef
	}
	hwRef, _ = h.resolveHardware(ctx, hwRef, hardwareRef, namespace)
	if hwRef == nil {
		return nil
	}
	if err := h.setHardwareAttributesAnnotation(ctx, hwRef, attrs, false); err != nil {
		journal.Log(ctx, "error updating Hardware with attributes", "error", err)
		log.Error(err, "error updating Hardware with attributes")
		return hwRef
	}
	journal.Log(ctx, "updated Hardware with attributes annotation", "hardware", hwRef.Name)
	log.Info("updated Hardware with attributes annotation", "hardware", hwRef.Name)
	return hwRef
}

// setHardwareAttributesAnnotation marshals attrs onto hw.Annotations[constant.AttributesAnnotation]
// and persists it via a merge-patch (to avoid conflicts with other controllers concurrently
// modifying the same Hardware object, e.g. the workflow controller toggling allowPXE).
//
// overwrite controls whether an existing annotation value is replaced. checkin.go's
// renderOnCheckIn passes true: a Template using Spec.RequiresCheckIn needs *this*
// check-in's live values, and a write-once guard would keep serving a stale snapshot from
// Hardware's first-ever check-in forever. resolveAndAnnotateHardware (the legacy
// first-action refresh used by every Workflow, not just RequiresCheckIn ones) passes
// false, preserving the original write-once bookkeeping semantics instead of adding a
// Kubernetes API write to every Workflow's first action fleet-wide.
func (h *Handler) setHardwareAttributesAnnotation(ctx context.Context, hw *tinkerbell.Hardware, attrs *data.AgentAttributes, overwrite bool) error {
	if hw == nil || attrs == nil {
		return nil
	}
	if !overwrite && hw.Annotations[constant.AttributesAnnotation] != "" {
		return nil
	}

	// Take a snapshot before mutation so the backend can compute a minimal merge-patch.
	original := hw.DeepCopy()

	if hw.Annotations == nil {
		hw.Annotations = make(map[string]string)
	}

	a, err := json.Marshal(attrs)
	if err != nil {
		return fmt.Errorf("error marshaling attributes for annotation: %w", err)
	}
	if len(a) > maxAnnotationSize {
		return fmt.Errorf("agent attributes annotation exceeds %dKB limit (%d bytes)", maxAnnotationSize/1024, len(a))
	}

	hw.Annotations[constant.AttributesAnnotation] = string(a)
	if err := h.Backend.UpdateHardware(ctx, hw, data.UpdateOptions{PatchFrom: original}); err != nil {
		return fmt.Errorf("error updating Hardware with attributes annotation: %w", err)
	}

	return nil
}

// resolveAndApplyInBandAttributes resolves the Hardware object for a Workflow and applies the calling
// Agent's attributes to status.attributes.inBand, but only when the calling Agent is the Hardware's own
// Agent (hw.Spec.AgentID) - other Agents used by the Workflow (e.g. an administrative Agent handling
// network or Netbox tasks) don't run on the target Hardware and would otherwise overwrite its in-band
// attributes with unrelated data. Unlike the legacy annotation, this is applied every time a matching
// Agent reports in, regardless of any existing value, so Hardware changes or a Tink Agent update are
// reflected on the next report rather than being permanently masked by the first one ever recorded.
func (h *Handler) resolveAndApplyInBandAttributes(ctx context.Context, log logr.Logger, hwRef *tinkerbell.Hardware, hardwareRef, namespace, agentID string, attrs *data.AgentAttributes) {
	if attrs == nil {
		return
	}
	hwRef, _ = h.resolveHardware(ctx, hwRef, hardwareRef, namespace)
	if hwRef == nil || hwRef.Spec.AgentID != agentID {
		return
	}
	// inBandAttributesFromAgent(attrs) only returns nil when attrs is nil, already
	// ruled out by the guard above, so inBand is always non-nil here.
	inBand := inBandAttributesFromAgent(attrs)
	inBand.CollectionMethod = "agent"
	inBand.LastUpdated = &metav1.Time{Time: h.NowFunc()}
	if err := h.Backend.ApplyHardwareInBandAttributes(ctx, hwRef.Name, hwRef.Namespace, inBand); err != nil {
		journal.Log(ctx, "error applying Hardware status.attributes.inBand", "error", err)
		log.Error(err, "error applying Hardware status.attributes.inBand")
	}
}

// resolveHardware returns hwRef unchanged if already resolved, otherwise reads it from the
// backend. Returns (nil, nil) when hardwareRef is empty - nothing to look up, not a
// failure - so callers can tell that apart from a lookup that was attempted and failed.
func (h *Handler) resolveHardware(ctx context.Context, hwRef *tinkerbell.Hardware, hardwareRef, namespace string) (*tinkerbell.Hardware, error) {
	if hwRef != nil {
		return hwRef, nil
	}
	if hardwareRef == "" {
		return nil, nil
	}
	hw, err := h.Backend.ReadHardware(ctx, hardwareRef, namespace)
	if err != nil {
		return nil, err
	}
	return hw, nil
}

func toPtr[T any](v T) *T {
	return &v
}
