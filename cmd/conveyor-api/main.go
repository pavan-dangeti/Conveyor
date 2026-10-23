// conveyor-api serves the gRPC API, its HTTP/JSON gateway, the dashboard and
// /metrics, and takes part in maintenance-leader election.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	conveyorv1 "github.com/pavan-dangeti/Conveyor/api/proto/conveyor/v1"
	"github.com/pavan-dangeti/Conveyor/internal/api"
	"github.com/pavan-dangeti/Conveyor/internal/demo"
	"github.com/pavan-dangeti/Conveyor/internal/maintenance"
	"github.com/pavan-dangeti/Conveyor/internal/metrics"
	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/store"
	"github.com/pavan-dangeti/Conveyor/internal/telemetry"
	"github.com/pavan-dangeti/Conveyor/internal/ui"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL not set")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := telemetry.Setup(ctx, "conveyor-api")
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	// Migrations run as the schema owner when MIGRATIONS_DATABASE_URL is set,
	// so DATABASE_URL can be a role with DML rights only.
	if err := runMigrations(ctx, getenv("MIGRATIONS_DATABASE_URL", dsn)); err != nil {
		return err
	}
	db, err := store.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `INSERT INTO queues (name) VALUES ('default') ON CONFLICT DO NOTHING`); err != nil {
		return err
	}
	prometheus.MustRegister(metrics.NewDBCollector(db))

	rate, _ := strconv.ParseFloat(getenv("CONVEYOR_RATE_LIMIT", "50"), 64)
	interceptor := api.Interceptor(api.InterceptorConfig{
		Token:      os.Getenv("CONVEYOR_API_TOKEN"),
		PublicRead: os.Getenv("CONVEYOR_PUBLIC_READ") == "1",
		RatePerSec: rate,
		Burst:      2 * rate,
	}, logger)
	grpcAddr := getenv("GRPC_ADDR", ":9090")
	httpAddr := getenv("HTTP_ADDR", ":8080")

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return err
	}
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(interceptor),
		grpc.MaxRecvMsgSize(api.MaxArgsBytes+16*1024),
	)
	conveyorv1.RegisterConveyorServer(grpcServer, api.NewServer(db))
	hs := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, hs)
	go func() {
		logger.Info("grpc listening", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc", "err", err)
		}
	}()
	go func() {
		for ctx.Err() == nil {
			if err := maintenance.RunElectedLoop(ctx, db, logger, 500*time.Millisecond); err != nil {
				logger.Error("maintenance", "err", err)
				time.Sleep(time.Second)
			}
		}
	}()

	gateway := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(func(h string) (string, bool) {
		if http.CanonicalHeaderKey(h) == "X-Request-Id" {
			return "x-request-id", true
		}
		return runtime.DefaultHeaderMatcher(h)
	}), runtime.WithOutgoingHeaderMatcher(func(h string) (string, bool) {
		if h == "x-request-id" {
			return "X-Request-Id", true
		}
		return "", false
	}))
	if err := conveyorv1.RegisterConveyorHandlerFromEndpoint(ctx, gateway, loopback(grpcAddr), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/", http.MaxBytesHandler(gateway, api.MaxArgsBytes+16*1024))
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.PingContext(r.Context()); err != nil {
			http.Error(w, "database unreachable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("GET /{$}", ui.Handler())

	if os.Getenv("CONVEYOR_DEMO") == "1" {
		n, _ := strconv.Atoi(getenv("DEMO_WORKERS", "3"))
		rate, _ := strconv.Atoi(getenv("DEMO_RATE", "4"))
		d := demo.New(db, dsn, getenv("DEMO_WORKER_BIN", "/usr/local/bin/harness-worker"), n, rate, logger)
		d.Register(mux)
		go d.Run(ctx)
		logger.Info("demo mode", "workers", n, "jobs_per_sec", rate)
	}

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           otelhttp.NewHandler(mux, "http", otelhttp.WithFilter(func(r *http.Request) bool { return r.URL.Path != "/metrics" })),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	go func() {
		logger.Info("http listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http", "err", err)
		}
	}()

	<-ctx.Done()
	hs.Shutdown()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	grpcServer.GracefulStop()
	return nil
}

func runMigrations(ctx context.Context, dsn string) error {
	db, err := store.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return migrate.Up(ctx, db)
}

// loopback turns a listen address like ":9090" into a dialable one.
func loopback(addr string) string {
	if host, port, err := net.SplitHostPort(addr); err == nil && host == "" {
		return "127.0.0.1:" + port
	}
	return addr
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
