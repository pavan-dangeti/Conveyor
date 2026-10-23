package bench

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/pgtest"
)

func TestPercentileNearestRank(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for p, want := range map[float64]float64{50: 5, 95: 10, 99: 10, 10: 1} {
		if got := Percentile(s, p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
	if Percentile(nil, 50) != 0 {
		t.Error("empty input")
	}
}

func TestMeanCI(t *testing.T) {
	// mean 3, sample sd sqrt(2.5), t(4) = 2.776, n = 5
	got := MeanCI([]float64{1, 2, 3, 4, 5})
	if got.Mean != 3 || math.Abs(got.CI95-2.776*math.Sqrt(2.5)/math.Sqrt(5)) > 1e-9 {
		t.Fatalf("got %+v", got)
	}
	if MeanCI([]float64{7}).CI95 != 0 || MeanCI(nil).Mean != 0 {
		t.Fatal("degenerate cases")
	}
}

func TestFinishAndAppend(t *testing.T) {
	var l Latencies
	for _, ms := range []int{3, 1, 2} {
		l.Add(time.Duration(ms) * time.Millisecond)
	}
	rep := l.Finish(3, time.Second, 0, map[string]int{"CPU": 2, "IO:WalSync": 5})
	if rep.Throughput != 3 || rep.P50Ms != 2 || rep.P99Ms != 3 {
		t.Fatalf("rep: %+v", rep)
	}
	db, _ := pgtest.Start(t)
	path := filepath.Join(t.TempDir(), "bench.json")
	for _, name := range []string{"a", "b", "a"} {
		if err := Append(path, Config{Name: name, System: "loadgen", Reps: []Rep{rep}}, db); err != nil {
			t.Fatal(err)
		}
	}
	var file struct {
		Versions map[string]string `json:"versions"`
		Configs  []Config          `json:"configs"`
	}
	b, _ := os.ReadFile(path)
	if err := json.Unmarshal(b, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Configs) != 2 || file.Versions["postgres"] == "" || file.Configs[1].TopWaits[0].Event != "IO:WalSync" {
		t.Fatalf("file: %s", b)
	}

	// pg_sleep keeps one backend active, so the sampler must observe it.
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _, _ = db.ExecContext(ctx, `SELECT pg_sleep(1)`) }()
	time.AfterFunc(300*time.Millisecond, cancel)
	if waits := SampleWaits(ctx, db); waits["Timeout:PgSleep"] == 0 {
		t.Fatalf("sampler missed the sleeping backend: %v", waits)
	}
}
