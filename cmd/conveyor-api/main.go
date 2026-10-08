package main

import (
	"context"
	"log/slog"
	"os"

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
	logger.Info("migrations applied; API server stub — gRPC/HTTP lands in Phase 3")
}
