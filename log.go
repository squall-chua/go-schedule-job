package goschedule

import (
	"log/slog"
	"time"
)

func logEnqueue(l *slog.Logger, id JobID, name, queue string, runAt time.Time) {
	l.Debug("goschedule: enqueued",
		slog.String("id", string(id)),
		slog.String("name", name),
		slog.String("queue", queue),
		slog.Time("run_at", runAt),
	)
}

func logStart(l *slog.Logger, id JobID, name, queue string, attempt int) {
	l.Debug("goschedule: starting",
		slog.String("id", string(id)),
		slog.String("name", name),
		slog.String("queue", queue),
		slog.Int("attempt", attempt),
	)
}

func logSuccess(l *slog.Logger, id JobID, name, queue string, attempt int, d time.Duration) {
	l.Info("goschedule: succeeded",
		slog.String("id", string(id)),
		slog.String("name", name),
		slog.String("queue", queue),
		slog.Int("attempt", attempt),
		slog.Duration("duration", d),
	)
}

func logFailure(l *slog.Logger, id JobID, name, queue string, attempt int, err error) {
	l.Error("goschedule: failed",
		slog.String("id", string(id)),
		slog.String("name", name),
		slog.String("queue", queue),
		slog.Int("attempt", attempt),
		slog.Any("err", err),
	)
}

func logRetry(l *slog.Logger, id JobID, name, queue string, attempt int, err error, nextAt time.Time) {
	l.Warn("goschedule: retrying",
		slog.String("id", string(id)),
		slog.String("name", name),
		slog.String("queue", queue),
		slog.Int("attempt", attempt),
		slog.Time("next_at", nextAt),
		slog.Any("err", err),
	)
}
