package driver_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// exampleMark matches the comment that names the example a README Go block
// comes from.
var exampleMark = regexp.MustCompile(`^<!-- (Example[A-Za-z0-9_]*) -->$`)

// block is one Go block of README.md and the example it names.
type block struct {
	name  string
	lines []string
}

// TestREADMEExamples fails when a Go block in README.md or a page of docs/ is
// not a run of consecutive lines in the body of the example the comment above
// it names, so the documentation cannot drift from code that compiles.
func TestREADMEExamples(t *testing.T) {
	pages, err := filepath.Glob("docs/*.md")
	if err != nil {
		t.Fatal(err)
	}
	examples, err := os.ReadFile("example_test.go")
	if err != nil {
		t.Fatal(err)
	}
	bodies := exampleBodies(t, examples)

	for _, page := range append([]string{"README.md"}, pages...) {
		doc, err := os.ReadFile(page)
		if err != nil {
			t.Fatal(err)
		}
		blocks := readmeBlocks(t, page, string(doc))
		if n := strings.Count(string(doc), "```go"); n != len(blocks) {
			t.Errorf("%s has %d Go blocks, %d of them named by an example", page, n, len(blocks))
		}
		for _, b := range blocks {
			body, ok := bodies[b.name]
			if !ok {
				t.Errorf("%s names %s, which example_test.go does not define", page, b.name)
				continue
			}
			if !runs(body, b.lines) {
				t.Errorf("%s: %s: the block is not a run of lines in the example: %s", page, b.name, firstDifference(body, b.lines))
			}
		}
	}
}

// readmeBlocks returns the Go blocks of readme that a comment above them
// names, in the order they appear.
func readmeBlocks(t *testing.T, page, readme string) []block {
	t.Helper()
	lines := strings.Split(readme, "\n")
	var blocks []block
	seen := make(map[string]bool)
	for i := 0; i < len(lines); i++ {
		m := exampleMark.FindStringSubmatch(strings.TrimSpace(lines[i]))
		if m == nil {
			continue
		}
		name := m[1]
		if seen[name] {
			t.Fatalf("%s names %s twice", page, name)
		}
		seen[name] = true
		if i+1 == len(lines) || strings.TrimSpace(lines[i+1]) != "```go" {
			t.Fatalf("%s: the comment for %s is not followed by a Go block", page, name)
		}
		end := i + 2
		for end < len(lines) && strings.TrimSpace(lines[end]) != "```" {
			end++
		}
		if end == len(lines) {
			t.Fatalf("%s: the block for %s is not closed", page, name)
		}
		blocks = append(blocks, block{name: name, lines: normalize(strings.Join(lines[i+2:end], "\n"))})
		i = end
	}
	return blocks
}

// exampleBodies returns the body of each example function in src, without its
// braces, as normalized lines.
func exampleBodies(t *testing.T, src []byte) map[string][]string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "example_test.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	bodies := make(map[string][]string)
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Example") {
			continue
		}
		start := fset.Position(fn.Body.Lbrace).Offset + 1
		end := fset.Position(fn.Body.Rbrace).Offset
		bodies[fn.Name.Name] = normalize(string(src[start:end]))
	}
	return bodies
}

// normalize turns code into the lines a block is compared by: indentation and
// trailing spaces dropped, and the blank lines at either end. Import lines are
// the file's, not an example body's, so neither side carries one.
func normalize(code string) []string {
	lines := strings.Split(code, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
	}
	for len(lines) > 0 && lines[0] == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// runs reports whether part is a run of consecutive lines of body.
func runs(body, part []string) bool {
	for start := 0; start+len(part) <= len(body); start++ {
		if slices.Equal(body[start:start+len(part)], part) {
			return true
		}
	}
	return false
}

// firstDifference names the first line at which part stops matching a run of
// body, after the longest run it does match.
func firstDifference(body, part []string) string {
	best, at := 0, -1
	for start := 0; start < len(body); start++ {
		n := 0
		for n < len(part) && start+n < len(body) && body[start+n] == part[n] {
			n++
		}
		if n > best {
			best, at = n, start
		}
	}
	switch {
	case at < 0:
		return fmt.Sprintf("line 1: README.md has %q, which the example does not", part[0])
	case at+best == len(body):
		return fmt.Sprintf("line %d: the example ends, README.md has %q", best+1, part[best])
	default:
		return fmt.Sprintf("line %d: README.md has %q, the example has %q", best+1, part[best], body[at+best])
	}
}
