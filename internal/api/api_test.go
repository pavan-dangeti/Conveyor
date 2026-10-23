package api_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	conveyorv1 "github.com/pavan-dangeti/Conveyor/api/proto/conveyor/v1"
	"github.com/pavan-dangeti/Conveyor/internal/api"
	"github.com/pavan-dangeti/Conveyor/internal/pgtest"
)

func setup(t *testing.T) (*sql.DB, http.Handler) {
	t.Helper()
	db, _ := pgtest.Start(t)
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
	code, resp := do(t, h, "POST", "/v1/jobs", `{"queue":"q1","kind":"email","args":"{}","idempotencyKey":"k1","priority":3}`)
	if code != 200 || !strings.Contains(resp, `"id"`) {
		t.Fatalf("enqueue: %d %s", code, resp)
	}
	code, _ = do(t, h, "POST", "/v1/jobs", `{"queue":"q1","kind":"email","args":"{}","idempotencyKey":"k1"}`)
	if code != 200 {
		t.Fatalf("idempotent enqueue: %d", code)
	}
	code, resp = do(t, h, "GET", "/v1/queues", "")
	if code != 200 || !strings.Contains(resp, "q1") {
		t.Fatalf("queues: %d %s", code, resp)
	}
	code, resp = do(t, h, "POST", "/v1/cron", `{"queue":"q1","kind":"ping","args":"{}","spec":"*/5 * * * *"}`)
	if code != 200 || !strings.Contains(resp, "nextRunAt") {
		t.Fatalf("cron: %d %s", code, resp)
	}
	code, resp = do(t, h, "POST", "/v1/cron", `{"queue":"q1","kind":"ping","args":"{}","spec":"not-a-cron"}`)
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

// TestOperatorAndStatsEndpoints covers run_at forwarding, manual discard
// and the stats RPC through the HTTP gateway.
func TestOperatorAndStatsEndpoints(t *testing.T) {
	_, h := setup(t)
	code, resp := do(t, h, "POST", "/v1/jobs",
		`{"queue":"q1","kind":"email","args":"{}","runAt":"2030-01-01T00:00:00Z"}`)
	if code != 200 {
		t.Fatalf("enqueue: %d %s", code, resp)
	}
	id := extractID(t, resp)
	code, resp = do(t, h, "GET", fmt.Sprintf("/v1/jobs/%d", id), "")
	if code != 200 || !strings.Contains(resp, "2030-01-01T00:00:00Z") {
		t.Fatalf("run_at not forwarded: %d %s", code, resp)
	}
	code, resp = do(t, h, "POST", fmt.Sprintf("/v1/jobs/%d:discard", id), `{}`)
	if code != 200 {
		t.Fatalf("discard: %d %s", code, resp)
	}
	code, resp = do(t, h, "GET", fmt.Sprintf("/v1/jobs/%d", id), "")
	if code != 200 || !strings.Contains(resp, `"state":"discarded"`) {
		t.Fatalf("discarded job: %d %s", code, resp)
	}
	code, resp = do(t, h, "GET", "/v1/stats?window_seconds=60", "")
	if code != 200 || !strings.Contains(resp, "completedPerSec") {
		t.Fatalf("stats: %d %s", code, resp)
	}
}

func extractID(t *testing.T, resp string) int64 {
	t.Helper()
	// The gateway encodes int64 as a JSON string, per the proto3 JSON mapping.
	var r struct {
		ID int64 `json:"id,string"`
	}
	if err := json.Unmarshal([]byte(resp), &r); err != nil || r.ID == 0 {
		t.Fatalf("no job id in %q: %v", resp, err)
	}
	return r.ID
}
