package goschedule

import (
	"context"
	"log/slog"
	"time"
)

type janitor struct {
	store    Store
	clock    Clock
	logger   *slog.Logger
	workerID string
	every    time.Duration
}

func (j *janitor) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-j.clock.After(j.every):
		}
		now := j.clock.Now()
		if err := j.store.Heartbeat(ctx, j.workerID, now); err != nil {
			j.logger.Error("goschedule: heartbeat failed", slog.Any("err", err))
		}
		if recovered, err := j.store.RecoverStale(ctx, now); err != nil {
			j.logger.Error("goschedule: recover stale failed", slog.Any("err", err))
		} else if recovered > 0 {
			j.logger.Info("goschedule: recovered stale jobs", slog.Int("count", recovered))
		}
	}
}
