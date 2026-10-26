package driver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	workspacev1 "go.toshicloud.io/api/workspacev1"
	toshi "go.toshicloud.io/toshi"
)

// Command is one command to run in a sandbox.
type Command struct {
	// Argv is the program and its arguments; Argv[0] is looked up on the
	// guest's PATH.
	Argv []string
	// Env is added to the environment the command starts with.
	Env map[string]string
	// Cwd is the working directory; empty means the guest's default.
	Cwd string
	// Stdin is fed to the command's standard input, which is closed when it
	// ends. Nil closes standard input at once. A read of Stdin that fails
	// before the command ended closes standard input too, and fails Execute
	// with the read's error. Execute does not wait for a read of Stdin to
	// return after the command ended.
	Stdin io.Reader
	// Stdout and Stderr receive the command's output as it streams. Nil
	// discards it.
	Stdout, Stderr io.Writer
	// Timeout is how long the command may run before the driver cancels it
	// with CancelExecution. Zero means no limit.
	Timeout time.Duration
}

// Result is how a command ended.
type Result struct {
	// ExecutionID is the execution's ID.
	ExecutionID string
	// ExitCode is the command's exit status, or -1 when it ended without
	// one.
	ExitCode int
	// Cancelled reports a command that was cancelled before it exited.
	Cancelled bool
	// TimedOut reports that the timeout passed and the driver asked the
	// server to cancel the command.
	TimedOut bool
}

// Execute runs cmd in the sandbox with Sandboxes.Execute. It records the
// sandbox as StateBusy, streams standard input and output, cancels the
// command at its timeout, and records the sandbox as StateReady once the
// command ended. When Execute fails, its stream is lost or ctx ends before
// the command ended, the driver cannot always tell whether the command
// started or still runs, so Execute returns the error and leaves the sandbox
// StateBusy.
func (d *Driver) Execute(ctx context.Context, sb *Sandbox, cmd Command) (Result, error) {
	if err := d.setState(ctx, sb, StateBusy); err != nil {
		return Result{}, err
	}
	res, ended, err := d.run(ctx, sb, cmd)
	if !ended {
		return res, err
	}
	if stateErr := d.setState(ctx, sb, StateReady); stateErr != nil {
		return res, errors.Join(err, stateErr)
	}
	return res, err
}

// run runs cmd and reports whether it ended. When it ended, the error says
// what else went wrong: a read of Stdin, a write to Stdout or Stderr, or a
// cancel at the timeout that failed while the command ran on to its end.
func (d *Driver) run(ctx context.Context, sb *Sandbox, cmd Command) (Result, bool, error) {
	p, err := d.client.Workload().Sandboxes().Execute(ctx, &workspacev1.ExecuteStart{
		SandboxId: sb.ID(),
		Command:   &workspacev1.Command{Argv: cmd.Argv, Env: cmd.Env, Cwd: cmd.Cwd},
	})
	if err != nil {
		return Result{}, false, fmt.Errorf("driver: execute in sandbox %s: %w", sb.ID(), err)
	}
	res := Result{ExecutionID: p.ExecutionID()}

	// The output is read to its end even when a writer fails: the stream
	// stops while an output pipe is not read.
	stdout, stderr := &outWriter{w: cmd.Stdout}, &outWriter{w: cmd.Stderr}
	var copies sync.WaitGroup
	for _, c := range []struct {
		w *outWriter
		r io.Reader
	}{{stdout, p.Stdout()}, {stderr, p.Stderr()}} {
		copies.Add(1)
		go func() {
			defer copies.Done()
			_, _ = io.Copy(c.w, c.r)
		}()
	}
	// A write or the close fails only once the command ended or the stream
	// broke, which Wait reports.
	stdin := &inReader{r: cmd.Stdin}
	go func() {
		if cmd.Stdin != nil {
			_, _ = io.Copy(p.Stdin(), stdin)
		}
		_ = p.Stdin().Close()
	}()

	var cancelErr chan error
	var timer *time.Timer
	if cmd.Timeout > 0 {
		cancelErr = make(chan error, 1)
		timer = time.AfterFunc(cmd.Timeout, func() {
			_, err := d.client.Workload().Sandboxes().CancelExecution(ctx, &workspacev1.CancelExecutionRequest{
				ExecutionId: res.ExecutionID,
			})
			cancelErr <- err
		})
	}

	waitErr := p.Wait()
	copies.Wait()
	var late []error
	if err := stdin.failed(); err != nil {
		late = append(late, fmt.Errorf("driver: standard input of execution %s: %w", res.ExecutionID, err))
	}
	if timer != nil && !timer.Stop() {
		res.TimedOut = true
		if err := <-cancelErr; err != nil {
			late = append(late, fmt.Errorf("driver: cancel execution %s at its timeout: %w", res.ExecutionID, err))
		}
	}

	var exit *toshi.ExitError
	switch {
	case waitErr == nil:
	case errors.As(waitErr, &exit):
		res.Cancelled = exit.Exit.GetCancelled()
		res.ExitCode = -1
		if exit.Exit.ExitCode != nil {
			res.ExitCode = int(exit.Exit.GetExitCode())
		}
	default:
		return res, false, errors.Join(append(late, fmt.Errorf("driver: execution %s in sandbox %s: %w", res.ExecutionID, sb.ID(), waitErr))...)
	}
	for _, err := range []error{stdout.err, stderr.err} {
		if err != nil {
			late = append(late, fmt.Errorf("driver: output of execution %s: %w", res.ExecutionID, err))
		}
	}
	return res, true, errors.Join(late...)
}

// inReader reads r and keeps its first error other than io.EOF.
type inReader struct {
	r io.Reader

	mu  sync.Mutex
	err error
}

func (i *inReader) Read(b []byte) (int, error) {
	n, err := i.r.Read(b)
	if err != nil && !errors.Is(err, io.EOF) {
		i.mu.Lock()
		if i.err == nil {
			i.err = err
		}
		i.mu.Unlock()
	}
	return n, err
}

// failed returns the error a read returned so far, if any.
func (i *inReader) failed() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.err
}

// outWriter writes to w, or discards when w is nil. After w's first error it
// discards the rest and keeps the error.
type outWriter struct {
	w   io.Writer
	err error
}

func (o *outWriter) Write(b []byte) (int, error) {
	if o.w != nil && o.err == nil {
		if _, err := o.w.Write(b); err != nil {
			o.err = err
		}
	}
	return len(b), nil
}
