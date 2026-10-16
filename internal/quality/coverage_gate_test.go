package quality

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestStateMachineCoverage enforces the committed coverage gate for the pure
// core. internal/statemachine needs no containers, so 85% is measurable
// everywhere; the profile is written by `go test -coverprofile`, which CI
// runs with that flag. Without the flag the gate is skipped locally.
func TestStateMachineCoverage(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "coverage.out"))
	if err != nil {
		t.Skipf("coverage.out not present (run tests with -coverprofile): %v", err)
	}
	total, covered := 0, 0
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		// fields[0] is "<file>:<range>"; fields[1] counts statements and
		// fields[2] counts executions, so a block counts as covered
		// when it actually ran.
		stmts, err1 := strconv.Atoi(fields[1])
		count, err2 := strconv.Atoi(fields[2])
		if err1 != nil || err2 != nil {
			continue
		}
		total += stmts
		if count > 0 {
			covered += stmts
		}
	}
	if total == 0 {
		t.Skip("no coverage data")
	}
	if float64(covered)/float64(total) < 0.85 {
		t.Errorf("statemachine coverage is %.1f%%, want 85%%", 100*float64(covered)/float64(total))
	}
}
