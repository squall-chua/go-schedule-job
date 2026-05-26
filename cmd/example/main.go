// Example: in-memory scheduler with three trigger styles. Runs for 3 seconds then exits.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/memstore"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	sched, err := gs.NewScheduler(
		gs.WithStore(memstore.New()),
		gs.WithLogger(logger),
		gs.WithPollInterval(50*time.Millisecond),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	sched.Register("hello", func(_ context.Context, payload []byte) error {
		fmt.Printf("hello: %s\n", payload)
		return nil
	})

	_, _ = sched.Now("hello", []byte("now"))
	_, _ = sched.After(500*time.Millisecond, "hello", []byte("after 500ms"))
	_, _ = sched.Every(700*time.Millisecond, "hello", []byte("every 700ms"))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()
	if err := sched.Start(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
