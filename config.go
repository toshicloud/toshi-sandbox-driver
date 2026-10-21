package driver

import (
	"errors"
	"os"
	"strings"
)

// Config is what the driver reads from the environment for itself. The
// server, the token and an extra CA bundle are read by the toshi-go client:
// TOSHI_SERVER, TOSHI_TOKEN or TOSHI_TOKEN_FILE, and TOSHI_CA_FILE.
type Config struct {
	// Project is the project its sandboxes run in, from TOSHI_PROJECT.
	Project string
	// Environment is the project's environment, from TOSHI_ENV.
	Environment string
	// Name names this driver among the drivers of one environment, from
	// TOSHI_DRIVER_NAME. Its sandboxes carry it in a label, and their
	// request keys are derived from it.
	Name string
}

// ConfigFromEnv reads the Config from the process environment.
func ConfigFromEnv() (Config, error) {
	return configFrom(os.Getenv)
}

// configFrom reads the Config through getenv and checks it.
func configFrom(getenv func(string) string) (Config, error) {
	cfg := Config{
		Project:     strings.TrimSpace(getenv("TOSHI_PROJECT")),
		Environment: strings.TrimSpace(getenv("TOSHI_ENV")),
		Name:        strings.TrimSpace(getenv("TOSHI_DRIVER_NAME")),
	}
	if err := cfg.check(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// check fails when the project, the environment or the driver's name is
// empty, naming the variables that set them: the API has no default for
// the first two, and a default name would let two drivers take each
// other's sandboxes.
func (c Config) check() error {
	var missing []string
	if c.Project == "" {
		missing = append(missing, "TOSHI_PROJECT")
	}
	if c.Environment == "" {
		missing = append(missing, "TOSHI_ENV")
	}
	if c.Name == "" {
		missing = append(missing, "TOSHI_DRIVER_NAME")
	}
	switch len(missing) {
	case 0:
		return nil
	case 1:
		return errors.New("driver: set " + missing[0])
	default:
		last := len(missing) - 1
		return errors.New("driver: set " + strings.Join(missing[:last], ", ") + " and " + missing[last])
	}
}
