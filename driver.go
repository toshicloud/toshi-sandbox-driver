// Package driver runs sandboxes on Toshi for an agent orchestrator, over
// the public Workspace API through the toshi-go client.
package driver

import (
	"context"
	"fmt"
	"sync"

	"connectrpc.com/connect"
	workspacev1 "github.com/toshicloud/toshi-api/workspacev1"
	toshi "github.com/toshicloud/toshi-go"
)

// Driver holds the client and the scope its sandboxes run in.
type Driver struct {
	client *toshi.Client
	cfg    Config

	mu sync.Mutex
	// known holds the IDs of the driver's sandboxes that Recover listed or
	// the event log showed, so Recover and Follow pass on only their events.
	known map[string]bool
}

// New builds a Driver for cfg, which needs a project, an environment and a
// name. The options go to toshi.New, which reads the server, the token and a
// CA bundle from the environment unless an option names them; the scope is
// always cfg's project and environment.
func New(cfg Config, opts ...toshi.Option) (*Driver, error) {
	if err := cfg.check(); err != nil {
		return nil, err
	}
	client, err := toshi.New(append(opts, toshi.WithScope(cfg.Project, cfg.Environment))...)
	if err != nil {
		return nil, err
	}
	return &Driver{client: client, cfg: cfg, known: make(map[string]bool)}, nil
}

// Check confirms with ListProjects that the token sees the configured
// project and that the project has the configured environment.
func (d *Driver) Check(ctx context.Context) error {
	resp, err := d.client.Projects.ListProjects(ctx, connect.NewRequest(&workspacev1.ListProjectsRequest{}))
	if err != nil {
		return fmt.Errorf("driver: list projects: %w", err)
	}
	for _, p := range resp.Msg.GetProjects() {
		if p.GetName() != d.cfg.Project {
			continue
		}
		for _, env := range p.GetEnvironments() {
			if env.GetName() == d.cfg.Environment {
				return nil
			}
		}
		return fmt.Errorf("driver: project %q has no environment %q", d.cfg.Project, d.cfg.Environment)
	}
	return fmt.Errorf("driver: the token sees no project %q", d.cfg.Project)
}
