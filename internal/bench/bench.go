// Package bench holds the measurement code shared by cmd/loadgen and
// cmd/riverbench, so both systems are timed and summarised identically.
package bench

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Rep is one repetition of a workload.
type Rep struct {
	Throughput float64        `json:"throughput_jobs_per_sec"`
	P50Ms      float64        `json:"p50_ms"`
	P95Ms      float64        `json:"p95_ms"`
	P99Ms      float64        `json:"p99_ms"`
	Errors     int64          `json:"errors"`
	WaitEvents map[string]int `json:"wait_events,omitempty"`
}

// Stat is a sample mean with a two-sided 95% Student-t confidence interval.
type Stat struct {
	Mean float64 `json:"mean"`
	CI95 float64 `json:"ci95"`
}

// Config is one benchmark configuration with all its repetitions.
type Config struct {
	Name    string          `json:"name"`
	System  string          `json:"system"`
	Params  map[string]any  `json:"params"`
	Command string          `json:"command"`
	Reps    []Rep           `json:"reps"`
	Summary map[string]Stat `json:"summary"`
	// TopWaits sums wait-event samples over all reps: where Postgres
	// backends spent time while this configuration ran.
	TopWaits []WaitCount `json:"top_wait_events"`
}

type WaitCount struct {
	Event   string `json:"event"`
	Samples int    `json:"samples"`
}

// Latencies records enqueue-to-start latency samples from many goroutines.
type Latencies struct {
	mu sync.Mutex
	ms []float64
}

func (l *Latencies) Add(d time.Duration) {
	l.mu.Lock()
	l.ms = append(l.ms, float64(d.Microseconds())/1000)
	l.mu.Unlock()
}

// Percentile uses the nearest-rank method on sorted samples.
func Percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	return sorted[max(0, min(rank, len(sorted)-1))]
}

// Finish turns the collected samples into a Rep.
func (l *Latencies) Finish(completed int64, elapsed time.Duration, errors int64, waits map[string]int) Rep {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := slices.Clone(l.ms)
	sort.Float64s(s)
	return Rep{
		Throughput: float64(completed) / elapsed.Seconds(),
		P50Ms:      Percentile(s, 50), P95Ms: Percentile(s, 95), P99Ms: Percentile(s, 99),
		Errors: errors, WaitEvents: waits,
	}
}

// t975 holds two-sided 95% Student-t critical values indexed by degrees of freedom.
var t975 = []float64{0, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228}

// Summarise computes the mean and 95% CI of each metric across reps.
func Summarise(reps []Rep) map[string]Stat {
	out := map[string]Stat{}
	for name, get := range map[string]func(Rep) float64{
		"throughput_jobs_per_sec": func(r Rep) float64 { return r.Throughput },
		"p50_ms":                  func(r Rep) float64 { return r.P50Ms },
		"p95_ms":                  func(r Rep) float64 { return r.P95Ms },
		"p99_ms":                  func(r Rep) float64 { return r.P99Ms },
	} {
		xs := make([]float64, len(reps))
		for i, r := range reps {
			xs[i] = get(r)
		}
		out[name] = MeanCI(xs)
	}
	return out
}

func MeanCI(xs []float64) Stat {
	n := len(xs)
	if n == 0 {
		return Stat{}
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	mean := sum / float64(n)
	if n == 1 {
		return Stat{Mean: mean}
	}
	var ss float64
	for _, x := range xs {
		ss += (x - mean) * (x - mean)
	}
	t := 1.96
	if df := n - 1; df < len(t975) {
		t = t975[df]
	}
	return Stat{Mean: mean, CI95: t * math.Sqrt(ss/float64(n-1)) / math.Sqrt(float64(n))}
}

// SampleWaits polls pg_stat_activity every 10 ms until ctx ends and counts
// what active backends in this database were doing: wait_event_type:wait_event,
// or "CPU" when not waiting.
func SampleWaits(ctx context.Context, db *sql.DB) map[string]int {
	counts := map[string]int{}
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return counts
		case <-t.C:
		}
		rows, err := db.QueryContext(ctx, `
SELECT COALESCE(wait_event_type || ':' || wait_event, 'CPU') FROM pg_stat_activity
WHERE datname = current_database() AND state = 'active' AND pid <> pg_backend_pid()`)
		if err != nil {
			continue
		}
		for rows.Next() {
			var ev string
			if rows.Scan(&ev) == nil {
				counts[ev]++
			}
		}
		_ = rows.Close()
	}
}

func topWaits(reps []Rep, n int) []WaitCount {
	total := map[string]int{}
	for _, r := range reps {
		for ev, c := range r.WaitEvents {
			total[ev] += c
		}
	}
	out := make([]WaitCount, 0, len(total))
	for ev, c := range total {
		out = append(out, WaitCount{ev, c})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Samples > out[j].Samples })
	return out[:min(n, len(out))]
}

// Append adds cfg to the JSON results file at path, creating it with host
// and version metadata if needed and replacing any config of the same name.
func Append(path string, cfg Config, db *sql.DB) error {
	cfg.Summary = Summarise(cfg.Reps)
	cfg.TopWaits = topWaits(cfg.Reps, 5)
	cfg.Command = "go run ./cmd/" + cfg.System + " " + strings.Join(os.Args[1:], " ")
	var file struct {
		Date     string            `json:"date"`
		Host     string            `json:"host"`
		Versions map[string]string `json:"versions"`
		Method   string            `json:"method"`
		Configs  []Config          `json:"configs"`
	}
	if b, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(b, &file); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	if file.Date == "" {
		var pg string
		_ = db.QueryRow(`SHOW server_version`).Scan(&pg)
		host, _ := exec.Command("sysctl", "-n", "machdep.cpu.brand_string", "hw.memsize", "hw.ncpu").Output()
		file.Date = time.Now().UTC().Format("2006-01-02")
		file.Host = fmt.Sprintf("%s/%s; sysctl cpu, memsize, ncpu: %s", runtime.GOOS, runtime.GOARCH, strings.Join(strings.Fields(string(host)), " "))
		file.Versions = map[string]string{"go": runtime.Version(), "postgres": pg}
		file.Method = "Each config runs its reps back to back against one Postgres container on the same host as the Go process. " +
			"Per rep the bench queue is emptied, then 4 producer goroutines enqueue all jobs while the workers drain them. " +
			"Throughput = jobs / (first enqueue to last handler start). Latency = enqueue timestamp carried in the job args to handler start, " +
			"both read from the same process clock; percentiles are nearest-rank over every job in the rep. " +
			"Summary = mean over reps with a two-sided 95% Student-t interval."
	}
	file.Configs = slices.DeleteFunc(file.Configs, func(c Config) bool { return c.Name == cfg.Name })
	file.Configs = append(file.Configs, cfg)
	b, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
