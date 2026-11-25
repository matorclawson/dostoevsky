package dostoevsky_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// goBlocks returns the fenced ```go blocks of a markdown file in reading order.
func goBlocks(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var blocks []string
	var cur []string
	in := false
	for _, line := range strings.Split(string(b), "\n") {
		switch {
		case !in && strings.TrimSpace(line) == "```go":
			in, cur = true, nil
		case in && strings.HasPrefix(strings.TrimSpace(line), "```"):
			blocks = append(blocks, strings.Join(cur, "\n"))
			in = false
		case in:
			cur = append(cur, line)
		}
	}
	return blocks
}

// The guide's code as a reader pastes it: the setup block once at the top of main,
// then every later block in reading order, each step in its own braces (the steps
// are independent, not one session). The result must compile and vet, so the
// published text cannot drift from what the fixtures happen to run.
func TestGuideCodeBlocksCompile(t *testing.T) {
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("needs the go tool")
	}
	blocks := goBlocks(t, "docs/getting-started.md")
	if len(blocks) < 6 {
		t.Fatalf("found %d Go blocks in docs/getting-started.md, want the setup and five steps", len(blocks))
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	var src strings.Builder
	src.WriteString("package main\n\nimport (\n\t\"context\"\n\t\"errors\"\n\t\"fmt\"\n\t\"os\"\n\n\t\"github.com/rodriveracom/dostoevsky\"\n)\n\n")
	src.WriteString("var _ = errors.New\nvar _ = fmt.Sprint\n\nfunc main() {\n")
	src.WriteString(blocks[0] + "\n")
	for _, step := range blocks[1:] {
		src.WriteString("\t{\n" + step + "\n\t}\n")
	}
	src.WriteString("}\n")
	dir := t.TempDir()
	mod := "module guidecheck\n\ngo 1.22\n\nrequire github.com/rodriveracom/dostoevsky v0.0.0\n\nreplace github.com/rodriveracom/dostoevsky => " + filepath.ToSlash(root) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(mod), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "vet", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOWORK=off", "GOPROXY=off", "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the guide's code does not compile as pasted: %v\n%s\n--- program ---\n%s", err, out, src.String())
	}
}
