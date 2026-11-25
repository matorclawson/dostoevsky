package dostoevsky_test

import (
	"os"
	"strings"
	"testing"
)

// The runtime module is standard library only. Benchmark-only dependencies
// (JetStream, go-redis, SQLite, Kafka and RabbitMQ clients) live in
// benchmarks/embedded/go.mod, never here.
func TestRuntimeModuleHasNoDependencies(t *testing.T) {
	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(mod), "\n") {
		if f := strings.Fields(line); len(f) > 0 && (f[0] == "require" || f[0] == "replace") {
			t.Errorf("go.mod has %q: the runtime module must not depend on anything", line)
		}
	}
	if sum, err := os.ReadFile("go.sum"); err == nil && len(strings.TrimSpace(string(sum))) > 0 {
		t.Error("go.sum is not empty: the runtime module must not depend on anything")
	}
}
