package api_test

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	conveyorv1 "github.com/pavan-dangeti/Conveyor/api/proto/conveyor/v1"
	"github.com/pavan-dangeti/Conveyor/internal/api"
	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setup(t *testing.T) (*sql.DB, http.Handler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	c, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("conveyor"),
		postgres.WithUsername("conveyor"),
		postgres.WithPassword("conveyor"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrate.Up(ctx, db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO queues(name) VALUES ('q1')`); err != nil {
		t.Fatal(err)
	}
	mux := runtime.NewServeMux()
	if err := conveyorv1.RegisterConveyorHandlerServer(context.Background(), mux, api.NewServer(db)); err != nil {
		t.Fatal(err)
	}
	return db, mux
}

func do(t *testing.T, h http.Handler, method, path, body string) (int, string) {
	t.Helper()
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, path, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code, w.Body.String()
}

func TestHTTPGatewayEndToEnd(t *testing.T) {
	_, h := setup(t)
	code, resp := do(t, h, "POST", "/v1/jobs", `{"queue":"q1","kind":"email","args":"e30=","idempotencyKey":"k1","priority":3}`)
	if code != 200 || !strings.Contains(resp, `"id"`) {
		t.Fatalf("enqueue: %d %s", code, resp)
	}
	code, _ = do(t, h, "POST", "/v1/jobs", `{"queue":"q1","kind":"email","args":"e30=","idempotencyKey":"k1"}`)
	if code != 200 {
		t.Fatalf("idempotent enqueue: %d", code)
	}
	code, resp = do(t, h, "GET", "/v1/queues", "")
	if code != 200 || !strings.Contains(resp, "q1") {
		t.Fatalf("queues: %d %s", code, resp)
	}
	code, resp = do(t, h, "POST", "/v1/cron", `{"queue":"q1","kind":"ping","args":"e30=","spec":"*/5 * * * *"}`)
	if code != 200 || !strings.Contains(resp, "nextRunAt") {
		t.Fatalf("cron: %d %s", code, resp)
	}
	code, resp = do(t, h, "POST", "/v1/cron", `{"queue":"q1","kind":"ping","args":"e30=","spec":"not-a-cron"}`)
	if code != 400 {
		t.Fatalf("bad cron: %d %s", code, resp)
	}
	code, _ = do(t, h, "POST", "/v1/jobs/1:cancel", `{}`)
	if code != 200 {
		t.Fatalf("cancel: %d", code)
	}
	code, _ = do(t, h, "POST", "/v1/jobs", `{"queue":"q1","kind":"x","args":"not-json"}`)
	if code != 400 && code != 500 {
		t.Fatalf("bad args: %d", code)
	}
}
