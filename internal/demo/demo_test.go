package demo

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/pgtest"
)

// The test binary doubles as the "worker" the demo supervises.
func TestMain(m *testing.M) {
	if os.Getenv("DEMO_FAKE_WORKER") == "1" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		<-ctx.Done()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestFleetChaosAndEndpoints(t *testing.T) {
	db, dsn := pgtest.Start(t)
	if _, err := db.Exec(`INSERT INTO queues (name) VALUES ('default')`); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DEMO_FAKE_WORKER", "1")
	bin, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	d := New(db, dsn, bin, 2, 5, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)
	mux := http.NewServeMux()
	d.Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	get := func(path string, v any) int {
		r, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Body.Close() }()
		_ = json.NewDecoder(r.Body).Decode(v)
		return r.StatusCode
	}
	var ws []worker
	pgtest.Eventually(t, "both workers alive", func() bool {
		get("/demo/workers", &ws)
		return len(ws) == 2 && ws[0].Alive && ws[1].Alive
	})
	r, err := http.Post(srv.URL+"/demo/chaos", "application/json", nil)
	if err != nil || r.StatusCode != http.StatusOK {
		t.Fatalf("chaos: %v %v", r.StatusCode, err)
	}
	if r, _ := http.Post(srv.URL+"/demo/chaos", "application/json", nil); r.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second kill within cooldown: %d", r.StatusCode)
	}
	pgtest.Eventually(t, "a worker marked dead", func() bool {
		get("/demo/workers", &ws)
		return !ws[0].Alive || !ws[1].Alive
	})
	pgtest.Eventually(t, "synthetic load", func() bool {
		var n int
		_ = db.QueryRow(`SELECT count(*) FROM jobs`).Scan(&n)
		return n > 0
	})
	var rep struct {
		Total int `json:"total"`
	}
	if code := get("/demo/check", &rep); code != http.StatusOK || rep.Total == 0 {
		t.Fatalf("check: %d %+v", code, rep)
	}
	time.Sleep(restartDelay + time.Second)
	get("/demo/workers", &ws)
	if !ws[0].Alive || !ws[1].Alive || ws[0].Restarts+ws[1].Restarts != 1 {
		t.Fatalf("killed worker should be restarted once: %+v", ws)
	}
}
