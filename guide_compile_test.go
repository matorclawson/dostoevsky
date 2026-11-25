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

// vetMain builds one main from the guide's setup block followed by a step, in the
// same scope and with no extra braces, which is how a reader pastes them, and runs
// go vet on it in a scratch module that replaces this repository. It returns vet's
// output and error, and the program for the failure message.
func vetMain(t *testing.T, setup, step string) (out string, program string, err error) {
	t.Helper()
	root, werr := os.Getwd()
	if werr != nil {
		t.Fatal(werr)
	}
	var src strings.Builder
	src.WriteString("package main\n\nimport (\n\t\"context\"\n\t\"errors\"\n\t\"fmt\"\n\t\"os\"\n\n\t\"github.com/rodriveracom/dostoevsky\"\n)\n\n")
	src.WriteString("var _ = errors.New\nvar _ = fmt.Sprint\n\nfunc main() {\n")
	src.WriteString(setup + "\n" + step + "\n}\n")
	dir := t.TempDir()
	mod := "module guidecheck\n\ngo 1.22\n\nrequire github.com/rodriveracom/dostoevsky v0.0.0\n\nreplace github.com/rodriveracom/dostoevsky => " + filepath.ToSlash(root) + "\n"
	if werr := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(mod), 0o600); werr != nil {
		t.Fatal(werr)
	}
	if werr := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src.String()), 0o600); werr != nil {
		t.Fatal(werr)
	}
	cmd := exec.Command("go", "vet", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOWORK=off", "GOPROXY=off", "CGO_ENABLED=0")
	b, err := cmd.CombinedOutput()
	return string(b), src.String(), err
}

func needGo(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("needs the go tool")
	}
}

// The guide as a reader uses it: the setup block, then one step, pasted into the same
// function. Each of the five steps must compile and vet that way.
func TestGuideCodeBlocksCompile(t *testing.T) {
	needGo(t)
	blocks := goBlocks(t, "docs/getting-started.md")
	if len(blocks) != 6 {
		t.Fatalf("found %d Go blocks in docs/getting-started.md, want the setup and five steps", len(blocks))
	}
	for i, step := range blocks[1:] {
		if out, program, err := vetMain(t, blocks[0], step); err != nil {
			t.Errorf("step %d does not compile after the setup as pasted: %v\n%s\n--- program ---\n%s", i+1, err, out, program)
		}
	}
}

// Positive control: the test must be able to fail. The restart step with ':=' instead of
// '=' redeclares the setup's variables, which a reader would hit, so it must not compile.
func TestGuideCompileCheckCatchesARedeclaration(t *testing.T) {
	needGo(t)
	blocks := goBlocks(t, "docs/getting-started.md")
	var restart string
	for _, b := range blocks {
		if strings.Contains(b, "dostoevsky.Open(dir, cfg)") && strings.Contains(b, "node.Close()") && strings.Contains(b, "Position") {
			restart = b
		}
	}
	if !strings.Contains(restart, "node, err = dostoevsky.Open(dir, cfg)") {
		t.Fatalf("the restart step was not found with its assignment:\n%s", restart)
	}
	broken := strings.Replace(restart, "node, err = dostoevsky.Open(dir, cfg)", "node, err := dostoevsky.Open(dir, cfg)", 1)
	out, _, err := vetMain(t, blocks[0], broken)
	if err == nil || !strings.Contains(out, "no new variables") {
		t.Fatalf("the broken step compiled, so the compile check is blind: %v\n%s", err, out)
	}
	// And an unused setup variable is also caught: step bodies must use what the setup declares.
	if out, _, err := vetMain(t, blocks[0], "_ = dir"); err == nil || !strings.Contains(out, "declared and not used") {
		t.Fatalf("an unused setup variable was not reported: %v\n%s", err, out)
	}
}
