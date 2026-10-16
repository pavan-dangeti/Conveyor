package quality

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

type runRecord struct {
	System     string  `json:"system"`
	Jobs       int64   `json:"jobs"`
	Throughput float64 `json:"throughput_jobs_per_sec"`
	P50        float64 `json:"p50_ms"`
	P95        float64 `json:"p95_ms"`
	P99        float64 `json:"p99_ms"`
}

type resultTuple struct {
	jobs          int64
	throughput    float64
	p50, p95, p99 float64
}

var numberRe = regexp.MustCompile(`[0-9]+(?:\.[0-9]+)?`)

// TestReadmeFiguresMatchResults fails when any number quoted in the README
// results table disagrees with the committed result files, or when a
// committed run is missing from the table. The README is the human-facing
// summary of results/benchmarks-*.json, so both must agree exactly.
func TestReadmeFiguresMatchResults(t *testing.T) {
	root := repoRoot(t)
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatalf("read README: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "results"))
	if err != nil {
		t.Fatalf("read results: %v", err)
	}
	var path string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "benchmarks") {
			path = filepath.Join(root, "results", e.Name())
			break
		}
	}
	if path == "" {
		t.Skip("no results/benchmarks-*.json committed")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var file struct {
		Runs []runRecord `json:"runs"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	committed := make(map[resultTuple]bool, len(file.Runs))

	table := parseResultsTable(t, string(readme))
	if len(table) == 0 {
		t.Fatalf("no results table found in README")
	}
	for _, r := range table {
		committed[r.tuple()] = true
	}

	for _, r := range file.Runs {
		tuple := resultTuple{r.Jobs, r.Throughput, r.P50, r.P95, r.P99}
		if !committed[tuple] {
			t.Errorf("committed run %q is missing from the README table (jobs=%d throughput=%.0f p50=%.0f p95=%.0f p99=%.0f)",
				r.System, r.Jobs, r.Throughput, r.P50, r.P95, r.P99)
		}
	}
	for _, r := range table {
		if !committed[r.tuple()] {
			t.Errorf("README quotes a result with no matching run: jobs=%d throughput=%.0f p50=%.0f p95=%.0f p99=%.0f",
				r.jobs, r.throughput, r.p50, r.p95, r.p99)
		}
	}
}

type resultsRow struct {
	label      string
	jobs       int64
	throughput float64
	p50        float64
	p95        float64
	p99        float64
}

func (r resultsRow) tuple() resultTuple {
	return resultTuple{r.jobs, r.throughput, r.p50, r.p95, r.p99}
}

// parseResultsTable extracts the figures from the first markdown table after
// the "## Results" heading and returns them. The table's columns are
// label | workers | jobs | throughput | p50 | p95 | p99.
func parseResultsTable(t *testing.T, readme string) []resultsRow {
	t.Helper()
	var rows []resultsRow
	for _, line := range strings.Split(readme, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			continue
		}
		cells := splitRow(line)
		if len(cells) < 7 {
			continue
		}
		switch strings.TrimSpace(cells[0]) {
		case "system":
			continue // header row
		}
		if strings.Contains(cells[0], "---") {
			continue // separator row
		}
		row := resultsRow{label: strings.TrimSpace(cells[0])}
		if v, err := strconv.ParseInt(strings.TrimSpace(cells[2]), 10, 64); err == nil {
			row.jobs = v
		}
		for i, cell := range []string{cells[3], cells[4], cells[5], cells[6]} {
			v, err := parseNumber(cell)
			if err != nil {
				continue
			}
			switch i {
			case 0:
				row.throughput = v
			case 1:
				row.p50 = v
			case 2:
				row.p95 = v
			case 3:
				row.p99 = v
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func splitRow(line string) []string {
	parts := strings.Split(line, "|")
	out := make([]string, 0, len(parts))
	for _, p := range parts[1 : len(parts)-1] {
		out = append(out, strings.TrimSpace(strings.Trim(p, "`* ")))
	}
	return out
}

func parseNumber(cell string) (float64, error) {
	m := numberRe.FindString(cell)
	if m == "" {
		return 0, fmt.Errorf("no number in %q", cell)
	}
	v, err := strconv.ParseFloat(m, 64)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// repoRoot is the repository root: this test file lives at
// <root>/internal/quality, so three directory steps up is the root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("honesty_drift_test.go")
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(dir)))
}
