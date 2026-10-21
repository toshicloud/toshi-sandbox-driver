package driver

import (
	"strings"
	"testing"
)

func TestConfigFrom(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		want    Config
		wantErr string
	}{
		{
			name: "both set",
			env:  map[string]string{"TOSHI_PROJECT": "default", "TOSHI_ENV": "production", "TOSHI_DRIVER_NAME": "orchestrator-1"},
			want: Config{Project: "default", Environment: "production", Name: "orchestrator-1"},
		},
		{
			name: "spaces trimmed",
			env:  map[string]string{"TOSHI_PROJECT": " web ", "TOSHI_ENV": "preview\n", "TOSHI_DRIVER_NAME": "\tci "},
			want: Config{Project: "web", Environment: "preview", Name: "ci"},
		},
		{
			name:    "project missing",
			env:     map[string]string{"TOSHI_ENV": "production", "TOSHI_DRIVER_NAME": "o"},
			wantErr: "driver: set TOSHI_PROJECT",
		},
		{
			name:    "name missing",
			env:     map[string]string{"TOSHI_PROJECT": "default", "TOSHI_ENV": "production"},
			wantErr: "driver: set TOSHI_DRIVER_NAME",
		},
		{
			name:    "two missing",
			env:     map[string]string{"TOSHI_PROJECT": " ", "TOSHI_DRIVER_NAME": "o"},
			wantErr: "driver: set TOSHI_PROJECT and TOSHI_ENV",
		},
		{
			name:    "all missing",
			env:     map[string]string{},
			wantErr: "driver: set TOSHI_PROJECT, TOSHI_ENV and TOSHI_DRIVER_NAME",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := configFrom(func(k string) string { return tt.env[k] })
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestNewNeedsServerAndToken(t *testing.T) {
	t.Setenv("TOSHI_SERVER", "")
	t.Setenv("TOSHI_TOKEN", "")
	t.Setenv("TOSHI_TOKEN_FILE", "")
	cfg := testConfig

	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "no server") {
		t.Fatalf("New without a server: err = %v", err)
	}

	t.Setenv("TOSHI_SERVER", "https://127.0.0.1:1")
	if _, err := New(cfg); err == nil || !strings.Contains(err.Error(), "no token") {
		t.Fatalf("New without a token: err = %v", err)
	}

	t.Setenv("TOSHI_TOKEN", "t")
	d, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if d.client == nil || d.cfg != cfg {
		t.Fatalf("New = %+v, want a client and cfg %+v", d, cfg)
	}
}

func TestNewNeedsTheScopeAndName(t *testing.T) {
	t.Setenv("TOSHI_SERVER", "https://127.0.0.1:1")
	t.Setenv("TOSHI_TOKEN", "t")
	_, err := New(Config{Project: "default"})
	if want := "driver: set TOSHI_ENV and TOSHI_DRIVER_NAME"; err == nil || err.Error() != want {
		t.Fatalf("New with only a project: err = %v, want %q", err, want)
	}
}
