package e2e_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	installationID = "4242"
	account        = "acme"
	projectName    = "default"
	environment    = "production"
	sandboxSize    = "toshi-linux-2x"
)

// stack is a control plane with its all-in-one agent on one qemu host, and
// a workspace token for it.
type stack struct {
	url    string
	caPath string
	token  string
}

// stackConfig is what startStack needs from the environment.
type stackConfig struct {
	bin       string
	imageDir  string
	imageName string
	accel     string
	// logDir, when set, receives a copy of each child's log at cleanup.
	logDir string
}

// startStack starts a fake GitHub and toshi serve with an agent on cfg's
// image, makes a workspace through the dev GitHub flow, and creates a
// project-wide token for it. Cleanup stops both processes.
func startStack(t *testing.T, cfg stackConfig) stack {
	t.Helper()
	dir := t.TempDir()
	caPath, certPath, keyPath := mintTLS(t, dir)
	appKeyPath := writeAppKey(t, dir)
	webhookPath := writeSecretFile(t, dir, "webhook.secret", 32)
	operatorPath := writeSecretFile(t, dir, "operator.token", 32)
	localKeyPath := writeSecretFile(t, dir, "secrets.key", 32)
	enrollPath := writeSecretFile(t, dir, "enrollment.token", 32)

	servePort := freePort(t)
	fakePort := freePort(t)
	baseURL := fmt.Sprintf("https://127.0.0.1:%d", servePort)
	configPath := filepath.Join(dir, "toshi.toml")
	writeFile(t, configPath, fmt.Sprintf(`listen = "127.0.0.1:%d"
data_dir = %q
log_level = "debug"

tls_cert_file = %q
tls_key_file = %q

[store]
driver = "sqlite"

[operator]
token_file = %q

[secrets]
local_key_file = %q

[agent_auth]
enrollment_token_file = %q

[github]
app_id = 1
base_url = "http://127.0.0.1:%d"
private_key_file = %q
webhook_secret_file = %q
`, servePort, mkdir(t, dir, "data"), certPath, keyPath, operatorPath, localKeyPath, enrollPath, fakePort, appKeyPath, webhookPath))

	// One toshi-linux-2x: the agent advertises exactly its CPUs and memory,
	// and keeps one VM warm for it. The tests have no delegated cgroup, so
	// the provider does not cap VMs in one.
	agentPath := filepath.Join(dir, "agent.toml")
	writeFile(t, agentPath, fmt.Sprintf(`control_url = %q
ca_file = %q
enrollment_token_file = %q
data_dir = %q
cpus = 2
memory_mib = 8192

[provider]
kind = "qemu"

[provider.qemu]
image_dir = %q
state_dir = %q
accel = %q
vm_caps = false

[images]
linux = %q

[runner_dir]
linux = "/opt/actions-runner"

[[pools]]
size = %q
target = 1
`, baseURL, caPath, enrollPath, mkdir(t, dir, "agent"), cfg.imageDir, mkdir(t, dir, "instances"), cfg.accel, cfg.imageName, sandboxSize))

	startChild(t, filepath.Join(dir, "fake.log"), cfg.logDir, cfg.bin, "dev", "github", "serve",
		"--i-know-this-is-fake",
		"--listen", fmt.Sprintf("127.0.0.1:%d", fakePort),
		"--installation", installationID+":"+account,
	)
	waitPort(t, fakePort, 20*time.Second)
	startChild(t, filepath.Join(dir, "serve.log"), cfg.logDir, cfg.bin, "serve", "--config", configPath, "--agent-config", agentPath)

	httpClient := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: certPool(t, caPath), MinVersion: tls.VersionTLS12}},
		Timeout:   5 * time.Second,
	}
	waitReady(t, httpClient, baseURL+"/readyz", 60*time.Second)

	operatorEnv := map[string]string{
		"TOSHI_SERVER":     baseURL,
		"TOSHI_CA_FILE":    caPath,
		"TOSHI_TOKEN_FILE": operatorPath,
	}
	run(t, nil, cfg.bin, "dev", "github", "send",
		"--i-know-this-is-fake",
		"--url", baseURL+"/webhooks/github",
		"--ca-file", caPath,
		"--secret-file", webhookPath,
		"--installation", installationID,
		"--repo", account+"/app",
		"--event", "installation",
		"--action", "created",
	)
	waitTenant(t, cfg.bin, operatorEnv, installationID, 30*time.Second)
	token := strings.TrimSpace(run(t, operatorEnv, cfg.bin, "tokens", "create",
		"--tenant", installationID,
		"--name", "sandbox-driver-e2e",
		"--project", projectName,
	))
	if token == "" {
		t.Fatal("tokens create printed no token")
	}
	return stack{url: baseURL, caPath: caPath, token: token}
}

// mintTLS makes a CA and a leaf for 127.0.0.1 and localhost.
func mintTLS(t *testing.T, dir string) (caPath, certPath, keyPath string) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "sandbox driver e2e CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	caPath = filepath.Join(dir, "ca.pem")
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	writePEM(t, caPath, "CERTIFICATE", caDER, 0o644)
	writePEM(t, certPath, "CERTIFICATE", leafDER, 0o644)
	writePEM(t, keyPath, "PRIVATE KEY", keyDER, 0o600)
	return caPath, certPath, keyPath
}

// writeAppKey writes a throwaway RSA key for the fake GitHub App.
func writeAppKey(t *testing.T, dir string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "app.pem")
	writePEM(t, path, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key), 0o600)
	return path
}

// writeSecretFile writes n random bytes as base64 on one line.
func writeSecretFile(t *testing.T, dir, name string, n int) string {
	t.Helper()
	secret := make([]byte, n)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(secret)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func writePEM(t *testing.T, path, blockType string, der []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), mode); err != nil {
		t.Fatal(err)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mkdir(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port
}

func certPool(t *testing.T, caPath string) *x509.CertPool {
	t.Helper()
	b, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		t.Fatal("CA file holds no certificate")
	}
	return pool
}

// waitPort waits for a loopback port to accept a connection.
func waitPort(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("127.0.0.1:%d did not start", port)
}

func waitReady(t *testing.T, client *http.Client, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not become ready", url)
}

// waitTenant waits for the installation's tenant to appear.
func waitTenant(t *testing.T, bin string, env map[string]string, tenant string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		out, _ := runCommand(bin, env, "tenants", "list", "--json")
		last = out
		var payload struct {
			Tenants []struct {
				ID string `json:"id"`
			} `json:"tenants"`
		}
		if err := json.Unmarshal([]byte(out), &payload); err == nil {
			for _, got := range payload.Tenants {
				if got.ID == tenant {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("tenant %s did not appear:\n%s", tenant, last)
}

// startChild runs a child process, logging to path, and kills only it in
// cleanup. On failure the log's tail is printed; with keepDir set, the log
// is copied there.
func startChild(t *testing.T, path, keepDir, bin string, args ...string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout = file
	cmd.Stderr = file
	cmd.Env = childEnv(nil)
	if err := cmd.Start(); err != nil {
		_ = file.Close()
		t.Fatalf("%s: %v", bin, err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = file.Close()
		b, err := os.ReadFile(path)
		if err != nil {
			return
		}
		if t.Failed() {
			t.Logf("%s (tail):\n%s", filepath.Base(path), tail(b, 8000))
		}
		if keepDir != "" {
			if err := os.MkdirAll(keepDir, 0o700); err != nil {
				t.Errorf("keep %s: %v", filepath.Base(path), err)
				return
			}
			if err := os.WriteFile(filepath.Join(keepDir, filepath.Base(path)), b, 0o600); err != nil {
				t.Errorf("keep %s: %v", filepath.Base(path), err)
			}
		}
	})
}

// run runs a command with the extra environment and fails on a non-zero exit.
func run(t *testing.T, env map[string]string, bin string, args ...string) string {
	t.Helper()
	out, err := runCommand(bin, env, args...)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", bin, strings.Join(args, " "), err, out)
	}
	return out
}

func runCommand(bin string, env map[string]string, args ...string) (string, error) {
	cmd := exec.Command(bin, args...)
	cmd.Env = childEnv(env)
	out, err := cmd.Output()
	return string(out), err
}

// childEnv removes inherited TOSHI_* settings and appends the given ones.
func childEnv(extra map[string]string) []string {
	var env []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(name, "TOSHI_") {
			continue
		}
		env = append(env, entry)
	}
	for name, value := range extra {
		env = append(env, name+"="+value)
	}
	return env
}

func tail(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[len(b)-n:])
}
