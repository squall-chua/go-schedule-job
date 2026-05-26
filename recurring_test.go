package goschedule_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/memstore"
)

func TestScheduler_Every_FiresMultipleTimes(t *testing.T) {
	store := memstore.New()
	s, _ := gs.NewScheduler(gs.WithStore(store), gs.WithPollInterval(10*time.Millisecond))
	var n int32
	fired := make(chan struct{}, 4)
	s.Register("tick", func(_ context.Context, _ []byte) error {
		atomic.AddInt32(&n, 1)
		select {
		case fired <- struct{}{}:
		default:
		}
		return nil
	})
	if _, err := s.Every(30*time.Millisecond, "tick", nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _ = s.Start(ctx) }()
	for i := 0; i < 3; i++ {
		select {
		case <-fired:
		case <-ctx.Done():
			t.Fatalf("only %d fires observed", atomic.LoadInt32(&n))
		}
	}
}

func TestScheduler_Cron_InvalidExpressionErrors(t *testing.T) {
	s, _ := gs.NewScheduler(gs.WithStore(memstore.New()))
	s.Register("x", func(_ context.Context, _ []byte) error { return nil })
	if _, err := s.Cron("not a cron expression", "x", nil); err == nil {
		t.Error("expected error for invalid cron")
	}
}
