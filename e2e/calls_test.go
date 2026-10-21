package e2e_test

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"
	"path"
	"slices"
	"sync"
	"testing"
)

// calls counts the API calls a client makes, by procedure.
type calls struct {
	next http.RoundTripper

	mu    sync.Mutex
	count map[string]int
}

// countingClient returns an HTTP client that trusts the stack's CA, speaks
// HTTP/2 as Execute needs, and counts its calls in the returned calls.
func countingClient(t *testing.T, caPath string) (*http.Client, *calls) {
	t.Helper()
	pem, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatalf("no certificate in %s", caPath)
	}
	c := &calls{
		next: &http.Transport{
			TLSClientConfig:   &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
			ForceAttemptHTTP2: true,
		},
		count: make(map[string]int),
	}
	return &http.Client{Transport: c}, c
}

func (c *calls) RoundTrip(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.count[path.Base(path.Dir(req.URL.Path))+"/"+path.Base(req.URL.Path)]++
	c.mu.Unlock()
	return c.next.RoundTrip(req)
}

// list returns each procedure called with its count, sorted.
func (c *calls) list() (procedures []string, counts []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for p := range c.count {
		procedures = append(procedures, p)
	}
	slices.Sort(procedures)
	for _, p := range procedures {
		counts = append(counts, c.count[p])
	}
	return procedures, counts
}

// called reports whether procedure, as service/method, was called.
func (c *calls) called(procedure string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count[procedure] > 0
}
