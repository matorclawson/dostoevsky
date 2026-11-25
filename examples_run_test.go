package dostoevsky_test

import (
	"go/ast"
	"go/doc"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// `go test` runs an example only when go/doc recognizes its `// Output:` comment: a
// comment group that also holds another comment (an end marker directly above it,
// say) is not recognized, and the example then compiles but never runs. This test
// fails for any Example function in this package's tests that go/doc does not see
// with an output to check.
func TestEveryExampleHasARecognizedOutput(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var parsed []*ast.File
	declared := map[string]string{}
	for _, name := range files {
		f, err := parser.ParseFile(fset, name, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(f.Name.Name, "_test") {
			continue // only external test files hold the documentation examples
		}
		parsed = append(parsed, f)
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Example") {
				declared[fn.Name.Name] = name
			}
		}
	}
	if len(declared) == 0 {
		t.Fatal("no examples found; the check is not looking at the right files")
	}
	runnable := map[string]bool{}
	for _, ex := range doc.Examples(parsed...) {
		if ex.Output != "" || ex.EmptyOutput {
			runnable["Example"+ex.Name] = true
		}
	}
	for name, file := range declared {
		if !runnable[name] {
			t.Errorf("%s (%s) has no recognized // Output: comment, so `go test` would only compile it; put a blank line between it and any comment above", name, file)
		}
	}
}
