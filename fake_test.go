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
	toshi "go.toshicloud.io/toshi"
)

// fake serves the calls the driver makes from one in-memory sandbox. Its
// Execute is set per test.
type fake struct {
	workspacev1connect.UnimplementedWorkloadServiceHandler
	workspacev1connect.UnimplementedProjectServiceHandler

	mu       sync.Mutex
	projects []*workspacev1.Project
	// projectPages, when set, are the pages ListProjects returns in order;
	// the page token is the index of the page to return.
	projectPages [][]*workspacev1.Project
	projectLists []*workspacev1.ListProjectsRequest
	creates      []*workspacev1.CreateSandboxRequest
	waits        []*workspacev1.WaitSandboxRequest
	updates      []*workspacev1.UpdateSandboxRequest
	cancels      []*workspacev1.CancelExecutionRequest
	sandbox      *workspacev1.Sandbox
	// waitState is the state WaitSandbox reaches.
	waitState workspacev1.SandboxState
	// cancelled is closed by the first CancelExecution.
	cancelled chan struct{}
	execute   func(context.Context, *connect.BidiStream[workspacev1.ExecuteRequest, workspacev1.ExecuteResponse]) error

	// sandboxes are listed by ListSandboxes, one per page, after the
	// sandbox created first; listCursor is the list's position.
	sandboxes  []*workspacev1.Sandbox
	listCursor string
	lists      []*workspacev1.ListSandboxesRequest
	// events are the event log, each with its cursor; ListEvents returns
	// two a page.
	events     []*workspacev1.Event
	eventLists []*workspacev1.ListEventsRequest
	// changes are what WatchSandboxes sends after its cursor, by cursor,
	// once holdChanges, when set, is closed.
	changes     []*workspacev1.WatchSandboxesResponse
	holdChanges chan struct{}
	watches     []string
	// operation is the delete's; WaitOperation finishes it with opError.
	operation *workspacev1.Operation
	opError   *workspacev1.Error
	deletes   []*workspacev1.DeleteSandboxRequest
	opCalls   []string
}

func newFake() *fake {
	return &fake{
		projects: []*workspacev1.Project{{
			Name:         "default",
			Environments: []*workspacev1.ProjectEnvironment{{Name: "production"}},
		}},
		waitState: workspacev1.SandboxState_SANDBOX_STATE_RUNNING,
		cancelled: make(chan struct{}),
	}
}

func (f *fake) ListProjects(_ context.Context, req *connect.Request[workspacev1.ListProjectsRequest]) (*connect.Response[workspacev1.ListProjectsResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.projectLists = append(f.projectLists, req.Msg)
	if f.projectPages == nil {
		return connect.NewResponse(&workspacev1.ListProjectsResponse{Projects: f.projects}), nil
	}
	page := 0
	if t := req.Msg.GetPageToken(); t != "" {
		page = int(t[0] - '0')
	}
	resp := &workspacev1.ListProjectsResponse{}
	if page < len(f.projectPages) {
		resp.Projects = f.projectPages[page]
	}
	if page+1 < len(f.projectPages) {
		resp.NextPageToken = string(rune('0' + page + 1))
	}
	return connect.NewResponse(resp), nil
}

// CreateSandbox creates the one sandbox, or returns it for a repeated key.
func (f *fake) CreateSandbox(_ context.Context, req *connect.Request[workspacev1.CreateSandboxRequest]) (*connect.Response[workspacev1.CreateSandboxResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, req.Msg)
	if f.sandbox == nil {
		f.sandbox = &workspacev1.Sandbox{
			Id:          "wl-1",
			Project:     req.Msg.GetProject(),
			Environment: req.Msg.GetEnvironment(),
			Labels:      maps.Clone(req.Msg.GetLabels()),
			Status:      &workspacev1.SandboxStatus{State: workspacev1.SandboxState_SANDBOX_STATE_CREATED},
		}
	}
	return connect.NewResponse(&workspacev1.CreateSandboxResponse{Sandbox: f.sandbox}), nil
}

// WaitSandbox moves the sandbox to waitState and answers at once.
func (f *fake) WaitSandbox(_ context.Context, req *connect.Request[workspacev1.WaitSandboxRequest]) (*connect.Response[workspacev1.WaitSandboxResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.waits = append(f.waits, req.Msg)
	f.sandbox.Status.State = f.waitState
	return connect.NewResponse(&workspacev1.WaitSandboxResponse{Sandbox: f.sandbox, Reached: true}), nil
}

func (f *fake) GetSandbox(_ context.Context, req *connect.Request[workspacev1.GetSandboxRequest]) (*connect.Response[workspacev1.GetSandboxResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sandbox == nil || req.Msg.GetId() != f.sandbox.GetId() {
		return nil, connect.NewError(connect.CodeNotFound, nil)
	}
	return connect.NewResponse(&workspacev1.GetSandboxResponse{Sandbox: f.sandbox}), nil
}

// UpdateSandbox replaces the labels, as the server does.
func (f *fake) UpdateSandbox(_ context.Context, req *connect.Request[workspacev1.UpdateSandboxRequest]) (*connect.Response[workspacev1.UpdateSandboxResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, req.Msg)
	f.sandbox.Labels = maps.Clone(req.Msg.GetSandbox().GetLabels())
	return connect.NewResponse(&workspacev1.UpdateSandboxResponse{Sandbox: f.sandbox}), nil
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

// ListSandboxes returns the sandboxes carrying every label asked for, one
// a page.
func (f *fake) ListSandboxes(_ context.Context, req *connect.Request[workspacev1.ListSandboxesRequest]) (*connect.Response[workspacev1.ListSandboxesResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists = append(f.lists, req.Msg)
	var match []*workspacev1.Sandbox
	for _, sb := range f.sandboxes {
		ok := true
		for k, v := range req.Msg.GetLabels() {
			if sb.GetLabels()[k] != v {
				ok = false
			}
		}
		if ok {
			match = append(match, sb)
		}
	}
	page := 0
	if t := req.Msg.GetPageToken(); t != "" {
		page = int(t[0] - '0')
	}
	resp := &workspacev1.ListSandboxesResponse{Cursor: f.listCursor}
	if page < len(match) {
		resp.Sandboxes = match[page : page+1]
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
	i := f.after(req.Msg.GetPageToken())
	end := min(i+2, len(f.events))
	page := f.events[i:end]
	cursor := req.Msg.GetPageToken()
	if len(page) > 0 {
		cursor = page[len(page)-1].GetCursor()
	}
	resp := &workspacev1.ListEventsResponse{Events: page, Cursor: cursor}
	if end < len(f.events) {
		resp.NextPageToken = cursor
	}
	return connect.NewResponse(resp), nil
}

// WatchSandboxes sends the changes after the cursor and holds the stream
// open.
func (f *fake) WatchSandboxes(ctx context.Context, req *connect.Request[workspacev1.WatchSandboxesRequest], stream *connect.ServerStream[workspacev1.WatchSandboxesResponse]) error {
	f.mu.Lock()
	f.watches = append(f.watches, "sandboxes "+req.Msg.GetCursor())
	hold := f.holdChanges
	var send []*workspacev1.WatchSandboxesResponse
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

func (f *fake) DeleteSandbox(_ context.Context, req *connect.Request[workspacev1.DeleteSandboxRequest]) (*connect.Response[workspacev1.DeleteSandboxResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes = append(f.deletes, req.Msg)
	if f.operation == nil {
		f.operation = &workspacev1.Operation{Id: "op-1", WorkloadId: req.Msg.GetId()}
	}
	return connect.NewResponse(&workspacev1.DeleteSandboxResponse{Operation: f.operation}), nil
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
func (f *fake) calls() (creates []*workspacev1.CreateSandboxRequest, waits []*workspacev1.WaitSandboxRequest, updates []*workspacev1.UpdateSandboxRequest, cancels []*workspacev1.CancelExecutionRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.creates), slices.Clone(f.waits), slices.Clone(f.updates), slices.Clone(f.cancels)
}

// states returns the LabelState values the updates set, in order.
func (f *fake) states() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, u := range f.updates {
		out = append(out, u.GetSandbox().GetLabels()[LabelState])
	}
	return out
}

// newTestDriver serves f over TLS with HTTP/2, as Execute needs, and returns
// a Driver for it.
func newTestDriver(t *testing.T, f *fake, cfg Config) *Driver {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(workspacev1connect.NewWorkloadServiceHandler(f))
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
