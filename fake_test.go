package driver

import (
	"context"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"connectrpc.com/connect"
	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
	"github.com/toshicloud/toshi-api/workspacev1/workspacev1connect"
	toshi "github.com/toshicloud/toshi-go"
)

// fake serves the calls the driver makes from one in-memory sandbox. Its
// Execute is set per test.
type fake struct {
	workspacev1connect.UnimplementedWorkspaceServiceHandler
	workspacev1connect.UnimplementedProjectServiceHandler

	mu       sync.Mutex
	projects []*workspacev1.Project
	creates  []*workspacev1.CreateWorkloadRequest
	waits    []*workspacev1.WaitWorkloadRequest
	patches  []*workspacev1.PatchWorkloadRequest
	cancels  []*workspacev1.CancelExecutionRequest
	sandbox  *workspacev1.Workload
	// waitState is the state WaitWorkload reaches.
	waitState workspacev1.WorkloadState
	// cancelled is closed by the first CancelExecution.
	cancelled chan struct{}
	execute   func(context.Context, *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error

	// workloads are listed by ListWorkloads, one per page, after the
	// sandbox; listCursor is the list's position.
	workloads  []*workspacev1.Workload
	listCursor string
	lists      []*workspacev1.ListWorkloadsRequest
	// events are the event log, each with its cursor; ListEvents returns
	// two a page.
	events     []*workspacev1.Event
	eventLists []*workspacev1.ListEventsRequest
	// changes are what WatchWorkloads sends after its cursor, by cursor,
	// once holdChanges, when set, is closed.
	changes     []*workspacev1.WatchWorkloadsResponse
	holdChanges chan struct{}
	watches     []string
	// operation is the delete's; WaitOperation finishes it with opError.
	operation *workspacev1.Operation
	opError   *workspacev1.Error
	deletes   []*workspacev1.DeleteWorkloadRequest
	opCalls   []string
}

func newFake() *fake {
	return &fake{
		projects: []*workspacev1.Project{{
			Name:         "default",
			Environments: []*workspacev1.ProjectEnvironment{{Name: "production"}},
		}},
		waitState: workspacev1.WorkloadState_WORKLOAD_STATE_RUNNING,
		cancelled: make(chan struct{}),
	}
}

func (f *fake) ListProjects(context.Context, *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return connect.NewResponse(&workspacev1.ListProjectsResponse{Projects: f.projects}), nil
}

// CreateWorkload creates the one sandbox, or returns it for a repeated key.
func (f *fake) CreateWorkload(_ context.Context, req *connect.Request[workspacev1.CreateWorkloadRequest]) (*connect.Response[workspacev1.CreateWorkloadResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, req.Msg)
	if f.sandbox == nil {
		f.sandbox = &workspacev1.Workload{
			Id:          "wl-1",
			Kind:        workspacev1.WorkloadKind_WORKLOAD_KIND_SANDBOX,
			Project:     req.Msg.GetProject(),
			Environment: req.Msg.GetEnvironment(),
			Labels:      maps.Clone(req.Msg.GetLabels()),
			Status:      &workspacev1.WorkloadStatus{State: workspacev1.WorkloadState_WORKLOAD_STATE_CREATED},
		}
	}
	return connect.NewResponse(&workspacev1.CreateWorkloadResponse{Workload: f.sandbox}), nil
}

// WaitWorkload moves the sandbox to waitState and answers at once.
func (f *fake) WaitWorkload(_ context.Context, req *connect.Request[workspacev1.WaitWorkloadRequest]) (*connect.Response[workspacev1.WaitWorkloadResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waits = append(f.waits, req.Msg)
	f.sandbox.Status.State = f.waitState
	return connect.NewResponse(&workspacev1.WaitWorkloadResponse{Workload: f.sandbox, Reached: true}), nil
}

func (f *fake) GetWorkload(_ context.Context, req *connect.Request[workspacev1.GetWorkloadRequest]) (*connect.Response[workspacev1.GetWorkloadResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sandbox == nil || req.Msg.GetId() != f.sandbox.GetId() {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	return connect.NewResponse(&workspacev1.GetWorkloadResponse{Workload: f.sandbox}), nil
}

// PatchWorkload replaces the labels, as the server does.
func (f *fake) PatchWorkload(_ context.Context, req *connect.Request[workspacev1.PatchWorkloadRequest]) (*connect.Response[workspacev1.PatchWorkloadResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.patches = append(f.patches, req.Msg)
	f.sandbox.Labels = maps.Clone(req.Msg.GetWorkload().GetLabels())
	return connect.NewResponse(&workspacev1.PatchWorkloadResponse{Workload: f.sandbox}), nil
}

func (f *fake) CancelExecution(_ context.Context, req *connect.Request[workspacev1.CancelExecutionRequest]) (*connect.Response[workspacev1.CancelExecutionResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.cancels) == 0 {
		close(f.cancelled)
	}
	f.cancels = append(f.cancels, req.Msg)
	return connect.NewResponse(&workspacev1.CancelExecutionResponse{}), nil
}

func (f *fake) Execute(ctx context.Context, stream *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error {
	return f.execute(ctx, stream)
}

// ListWorkloads returns the workloads carrying every label asked for, one
// a page.
func (f *fake) ListWorkloads(_ context.Context, req *connect.Request[workspacev1.ListWorkloadsRequest]) (*connect.Response[workspacev1.ListWorkloadsResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists = append(f.lists, req.Msg)
	var match []*workspacev1.Workload
	for _, wl := range f.workloads {
		ok := true
		for k, v := range req.Msg.GetLabels() {
			if wl.GetLabels()[k] != v {
				ok = false
			}
		}
		if ok {
			match = append(match, wl)
		}
	}
	page := 0
	if t := req.Msg.GetPageToken(); t != "" {
		page = int(t[0] - '0')
	}
	resp := &workspacev1.ListWorkloadsResponse{Cursor: f.listCursor}
	if page < len(match) {
		resp.Workloads = match[page : page+1]
	}
	if page+1 < len(match) {
		resp.NextPageToken = string(rune('0' + page + 1))
	}
	return connect.NewResponse(resp), nil
}

// after returns the index of the first event after cursor. The fake's
// cursors sort in the log's order.
func (f *fake) after(cursor string) int {
	for i, e := range f.events {
		if e.GetCursor() > cursor {
			return i
		}
	}
	return len(f.events)
}

func (f *fake) ListEvents(_ context.Context, req *connect.Request[workspacev1.ListEventsRequest]) (*connect.Response[workspacev1.ListEventsResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.eventLists = append(f.eventLists, req.Msg)
	i := f.after(req.Msg.GetCursor())
	page := f.events[i:min(i+2, len(f.events))]
	cursor := req.Msg.GetCursor()
	if len(page) > 0 {
		cursor = page[len(page)-1].GetCursor()
	}
	return connect.NewResponse(&workspacev1.ListEventsResponse{Events: page, Cursor: cursor}), nil
}

// WatchWorkloads sends the changes after the cursor and holds the stream
// open.
func (f *fake) WatchWorkloads(ctx context.Context, req *connect.Request[workspacev1.WatchWorkloadsRequest], stream *connect.ServerStream[workspacev1.WatchWorkloadsResponse]) error {
	f.mu.Lock()
	f.watches = append(f.watches, "workloads "+req.Msg.GetCursor())
	hold := f.holdChanges
	var send []*workspacev1.WatchWorkloadsResponse
	for _, c := range f.changes {
		if c.GetCursor() > req.Msg.GetCursor() {
			send = append(send, c)
		}
	}
	f.mu.Unlock()
	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, c := range send {
		if err := stream.Send(c); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// WatchEvents sends the events after the cursor and holds the stream open.
func (f *fake) WatchEvents(ctx context.Context, req *connect.Request[workspacev1.WatchEventsRequest], stream *connect.ServerStream[workspacev1.WatchEventsResponse]) error {
	f.mu.Lock()
	f.watches = append(f.watches, "events "+req.Msg.GetCursor())
	send := slices.Clone(f.events[f.after(req.Msg.GetCursor()):])
	f.mu.Unlock()
	for _, e := range send {
		if err := stream.Send(&workspacev1.WatchEventsResponse{Event: e}); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fake) DeleteWorkload(_ context.Context, req *connect.Request[workspacev1.DeleteWorkloadRequest]) (*connect.Response[workspacev1.DeleteWorkloadResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, req.Msg)
	if f.operation == nil {
		f.operation = &workspacev1.Operation{Id: "op-1", WorkloadId: req.Msg.GetId()}
	}
	return connect.NewResponse(&workspacev1.DeleteWorkloadResponse{Operation: f.operation}), nil
}

func (f *fake) GetOperation(_ context.Context, req *connect.Request[workspacev1.GetOperationRequest]) (*connect.Response[workspacev1.GetOperationResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opCalls = append(f.opCalls, "get "+req.Msg.GetId())
	return connect.NewResponse(&workspacev1.GetOperationResponse{Operation: f.operation}), nil
}

// WaitOperation finishes the operation, failed with opError when set.
func (f *fake) WaitOperation(_ context.Context, req *connect.Request[workspacev1.WaitOperationRequest]) (*connect.Response[workspacev1.WaitOperationResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opCalls = append(f.opCalls, "wait "+req.Msg.GetId())
	f.operation = &workspacev1.Operation{Id: f.operation.GetId(), WorkloadId: f.operation.GetWorkloadId(), Done: true, Error: f.opError}
	return connect.NewResponse(&workspacev1.WaitOperationResponse{Operation: f.operation}), nil
}

// calls returns copies of the requests f received.
func (f *fake) calls() (creates []*workspacev1.CreateWorkloadRequest, waits []*workspacev1.WaitWorkloadRequest, patches []*workspacev1.PatchWorkloadRequest, cancels []*workspacev1.CancelExecutionRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.creates), slices.Clone(f.waits), slices.Clone(f.patches), slices.Clone(f.cancels)
}

// states returns the LabelState values the patches set, in order.
func (f *fake) states() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, p := range f.patches {
		out = append(out, p.GetWorkload().GetLabels()[LabelState])
	}
	return out
}

// newTestDriver serves f over TLS with HTTP/2, as Execute needs, and returns
// a Driver for it.
func newTestDriver(t *testing.T, f *fake, cfg Config) *Driver {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(workspacev1connect.NewWorkspaceServiceHandler(f))
	mux.Handle(workspacev1connect.NewProjectServiceHandler(f))
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	t.Setenv("TOSHI_CA_FILE", "")
	d, err := New(cfg,
		toshi.WithServer(server.URL),
		toshi.WithToken("test-token"),
		toshi.WithHTTPClient(server.Client()),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

var testConfig = Config{Project: "default", Environment: "production", Name: "orchestrator-1"}
