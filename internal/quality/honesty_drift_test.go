// Package quality holds repository-level checks that run with the tests.
package quality

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The README quotes results only inside marked tables. Every row there is
// rebuilt, byte for byte, from the newest committed results file, so a
// number cannot be edited by hand, rounded up, or left stale.

type stat struct {
	Mean float64 `json:"mean"`
	CI95 float64 `json:"ci95"`
}

type benchFile struct {
	Configs []struct {
		Name    string          `json:"name"`
		Summary map[string]stat `json:"summary"`
	} `json:"configs"`
}

type faultFile struct {
	Runs []struct {
		Run     int     `json:"run"`
		Seconds float64 `json:"seconds_to_drain"`
		Check   struct {
			Total      int            `json:"total"`
			ByState    map[string]int `json:"by_state"`
			LedgerRows int            `json:"ledger_rows"`
			Violations []string       `json:"violations"`
		} `json:"check"`
		LeaseExpirations int64 `json:"lease_expirations"`
		Reclaimed        int64 `json:"jobs_claimed_more_than_once"`
		MaxFencingToken  int64 `json:"max_fencing_token"`
	} `json:"runs"`
}

func benchRow(name string, s map[string]stat) string {
	t, p50, p95, p99 := s["throughput_jobs_per_sec"], s["p50_ms"], s["p95_ms"], s["p99_ms"]
	return fmt.Sprintf("| %s | %.0f ± %.0f | %.1f | %.1f | %.1f ± %.1f |", name, t.Mean, t.CI95, p50.Mean, p95.Mean, p99.Mean, p99.CI95)
}

func TestReadmeBenchmarkRowsMatchResults(t *testing.T) {
	var f benchFile
	path := newest(t, "bench-*.json", &f)
	want := map[string]string{}
	for _, c := range f.Configs {
		want[c.Name] = benchRow(c.Name, c.Summary)
	}
	rows := markedRows(t, "bench")
	if len(rows) < 3 {
		t.Fatalf("README benchmark table has %d rows, want at least the baseline, the before and River", len(rows))
	}
	for _, row := range rows {
		name := strings.TrimSpace(strings.Split(row, "|")[1])
		exp, ok := want[name]
		switch {
		case !ok:
			t.Errorf("README row %q names no configuration in %s", name, path)
		case row != exp:
			t.Errorf("README row drifted from %s:\n got: %s\nwant: %s", path, row, exp)
		}
	}
}

// Figures quoted outside the tables, such as the headline summary, must be
// copies of a table cell: every "X ± Y" in the README appears in a row.
func TestReadmeQuotedFiguresComeFromTheTable(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	rows := strings.Join(markedRows(t, "bench"), "\n")
	for _, fig := range regexp.MustCompile(`\d+(?:\.\d+)? ± \d+(?:\.\d+)?`).FindAllString(string(b), -1) {
		if !strings.Contains(rows, fig) {
			t.Errorf("README quotes %q, which is not in the benchmark table", fig)
		}
	}
}

func TestReadmeFaultRowsMatchResults(t *testing.T) {
	var f faultFile
	path := newest(t, "fault-*.json", &f)
	var want []string
	for _, r := range f.Runs {
		want = append(want, fmt.Sprintf("| %d | %d | %d | %d | %d | %d | %d | %d | %.0f s |",
			r.Run, r.Check.Total, r.Check.ByState["completed"], r.Check.LedgerRows, len(r.Check.Violations),
			r.LeaseExpirations, r.Reclaimed, r.MaxFencingToken, r.Seconds))
	}
	got := markedRows(t, "fault")
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("README fault table drifted from %s:\n got:\n%s\nwant:\n%s", path, strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func newest(t *testing.T, pattern string, v any) string {
	t.Helper()
	paths, _ := filepath.Glob(filepath.Join("..", "..", "results", pattern))
	if len(paths) == 0 {
		t.Fatalf("no results/%s committed", pattern)
	}
	sort.Strings(paths) // names carry ISO dates, so the last is the newest
	path := paths[len(paths)-1]
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return path
}

// markedRows returns the data rows (header and separator dropped) of the
// table between <!-- results:NAME --> and <!-- /results:NAME --> in README.md.
func markedRows(t *testing.T, name string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	start := strings.Index(s, "<!-- results:"+name+" -->")
	end := strings.Index(s, "<!-- /results:"+name+" -->")
	if start < 0 || end < start {
		t.Fatalf("README has no <!-- results:%s --> block", name)
	}
	var rows []string
	for _, line := range strings.Split(s[start:end], "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "|") && !strings.Contains(line, "---") {
			rows = append(rows, line)
		}
	}
	if len(rows) == 0 {
		return nil
	}
	return rows[1:]
}
