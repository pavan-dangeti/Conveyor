// Package maintenance runs one leader that promotes due jobs, reaps expired
// leases, and archives old terminal rows. Leader election is a Postgres
// session-level advisory lock: if the leader dies, its lock releases and a
// standby can take over.
package maintenance

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/pavan-dangeti/Conveyor/internal/metrics"
	"github.com/pavan-dangeti/Conveyor/internal/queue"
)

const advisoryLockKey = 0x636f6e766579

// RunElectedLoop tries to hold the advisory lock; the holder ticks.
// Returns nil on ctx cancel or unrecoverable connection failure.
func RunElectedLoop(ctx context.Context, db *sql.DB, logger *slog.Logger, tick time.Duration) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	for {
		if ctx.Err() != nil {
			return nil
		}
		var held bool
		if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, advisoryLockKey).Scan(&held); err != nil {
			return err
		}
		if !held {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(tick):
			}
			continue
		}
		logger.Info("maintenance leader acquired")
		metrics.IsLeader.Set(1)
		metrics.LeaderChanges.Inc()
		t := time.NewTicker(tick)
		leaderCtx, cancelLeader := context.WithCancel(ctx)
		go func() {
			// Watch the lock connection for failure; if it dies, drop leadership.
			for {
				if err := ctx.Err(); err != nil {
					return
				}
				if _, err := conn.ExecContext(leaderCtx, "SELECT 1"); err != nil {
					cancelLeader()
					return
				}
				time.Sleep(tick)
			}
		}()
	loop:
		for {
			select {
			case <-ctx.Done():
				t.Stop()
				cancelLeader()
				metrics.IsLeader.Set(0)
				_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, advisoryLockKey)
				return nil
			case <-leaderCtx.Done():
				t.Stop()
				break loop
			case <-t.C:
				if _, err := queue.PromoteDue(ctx, db); err != nil {
					logger.Error("promote", "err", err)
				}
				if _, err := queue.ExpandDueCron(ctx, db, time.Now()); err != nil {
					logger.Error("cron", "err", err)
				}
				if n, err := queue.ReapExpiredLeases(ctx, db); err != nil {
					logger.Error("reap", "err", err)
				} else if n > 0 {
					logger.Warn("reaped expired leases", "n", n)
				}
				if n, err := queue.ArchiveOld(ctx, db, 24*time.Hour); err != nil {
					logger.Error("archive", "err", err)
				} else if n > 0 {
					logger.Info("archived terminal jobs", "n", n)
				}
			}
		}
		cancelLeader()
		metrics.IsLeader.Set(0)
		logger.Warn("maintenance leadership lost")
	}
}
