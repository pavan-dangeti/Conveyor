package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	conveyorv1 "github.com/pavan-dangeti/Conveyor/api/proto/conveyor/v1"
	"github.com/pavan-dangeti/Conveyor/internal/api"
	"github.com/pavan-dangeti/Conveyor/internal/maintenance"
	"github.com/pavan-dangeti/Conveyor/internal/migrate"
	"github.com/pavan-dangeti/Conveyor/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL not set")
		os.Exit(1)
	}
	token := os.Getenv("CONVEYOR_API_TOKEN")
	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		logger.Error("open store", "err", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()
	if err := migrate.Up(context.Background(), db); err != nil {
		logger.Error("migrate", "err", err)
		os.Exit(1)
	}
	grpcAddr := getenv("GRPC_ADDR", ":9090")
	httpAddr := getenv("HTTP_ADDR", ":8080")

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(authInterceptor(token)))
	conveyorv1.RegisterConveyorServer(grpcServer, api.NewServer(db))
	hg := health.NewServer()
	hg.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, hg)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		for ctx.Err() == nil {
			if err := maintenance.RunElectedLoop(ctx, db, logger, 500*time.Millisecond); err != nil {
				logger.Error("maintenance", "err", err)
				time.Sleep(time.Second)
			}
		}
	}()
	go func() {
		logger.Info("grpc listening", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc", "err", err)
		}
	}()

	mux := runtime.NewServeMux()
	if err := conveyorv1.RegisterConveyorHandlerFromEndpoint(ctx, mux, grpcAddr, []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}); err != nil {
		logger.Error("gateway", "err", err)
		os.Exit(1)
	}
	httpMux := http.NewServeMux()
	httpMux.Handle("/v1/", maxBytes(authHTTP(token, mux), 64*1024))
	httpMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	httpMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.PingContext(r.Context()); err != nil {
			http.Error(w, "db down", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(200)
	})
	httpServer := &http.Server{Addr: httpAddr, Handler: httpMux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("http listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http", "err", err)
		}
	}()
	<-ctx.Done()
	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(context.Background())
}

func authInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token == "" {
			return handler(ctx, req)
		}
		md, _ := metadata.FromIncomingContext(ctx)
		auth := ""
		if v := md.Get("authorization"); len(v) > 0 {
			auth = v[0]
		}
		if auth != "Bearer "+token {
			return nil, status.Errorf(codes.Unauthenticated, "bad token")
		}
		return handler(ctx, req)
	}
}

func authHTTP(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" || r.Header.Get("Authorization") == "Bearer "+token {
			next.ServeHTTP(w, r)
			return
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func maxBytes(next http.Handler, n int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, n)
		next.ServeHTTP(w, r)
	})
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
