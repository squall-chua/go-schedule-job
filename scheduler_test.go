package goschedule_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	gs "github.com/squallchua/goschedule"
	"github.com/squallchua/goschedule/memstore"
)

func TestNewScheduler_RequiresStore(t *testing.T) {
	if _, err := gs.NewScheduler(); err == nil {
		t.Fatal("expected error when no store provided")
	}
}

func TestNewScheduler_OK(t *testing.T) {
	if _, err := gs.NewScheduler(gs.WithStore(memstore.New())); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestScheduler_RegisterAndLookup(t *testing.T) {
	s, _ := gs.NewScheduler(gs.WithStore(memstore.New()))
	s.Register("greet", func(_ context.Context, _ []byte) error { return errors.New("ok") })
	if !s.IsRegistered("greet") {
		t.Errorf("expected greet to be registered")
	}
	if s.IsRegistered("unknown") {
		t.Errorf("unknown should not be registered")
	}
}

func TestScheduler_AtSavesPending(t *testing.T) {
	store := memstore.New()
	s, _ := gs.NewScheduler(gs.WithStore(store))
	s.Register("greet", func(_ context.Context, _ []byte) error { return nil })

	when := time.Now().Add(time.Hour)
	id, err := s.At(when, "greet", []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("empty job id")
	}
	if n, _ := store.QueueSize(context.Background(), "default"); n != 1 {
		t.Errorf("expected queue size 1, got %d", n)
	}
}

func TestScheduler_DispatchRejectsUnknownHandler(t *testing.T) {
	s, _ := gs.NewScheduler(gs.WithStore(memstore.New()))
	if _, err := s.Now("does-not-exist", nil); err == nil {
		t.Error("expected error for unregistered handler")
	}
}

func TestScheduler_Now_RunAtIsCurrentTime(t *testing.T) {
	store := memstore.New()
	s, _ := gs.NewScheduler(gs.WithStore(store), gs.WithClock(gs.RealClock()))
	s.Register("x", func(_ context.Context, _ []byte) error { return nil })
	before := time.Now()
	_, err := s.Now("x", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Claim with a "now" slightly after — must surface the job.
	got, _ := store.ClaimDue(context.Background(), "default", before.Add(time.Second), 1, "w", before.Add(time.Minute))
	if len(got) != 1 {
		t.Errorf("expected claimable: %+v", got)
	}
}

func TestScheduler_Cancel(t *testing.T) {
	store := memstore.New()
	s, _ := gs.NewScheduler(gs.WithStore(store))
	s.Register("x", func(_ context.Context, _ []byte) error { return nil })
	id, _ := s.At(time.Now().Add(time.Hour), "x", nil)
	if err := s.Cancel(id); err != nil {
		t.Fatal(err)
	}
	n, _ := store.QueueSize(context.Background(), "default")
	if n != 0 {
		t.Errorf("expected empty queue, got %d", n)
	}
}

func TestScheduler_Reschedule(t *testing.T) {
	store := memstore.New()
	s, _ := gs.NewScheduler(gs.WithStore(store))
	s.Register("x", func(_ context.Context, _ []byte) error { return nil })
	id, _ := s.At(time.Now().Add(time.Hour), "x", nil)
	if err := s.Reschedule(id, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, _ := store.ClaimDue(context.Background(), "default", time.Now().Add(time.Second), 1, "w", time.Now().Add(time.Minute))
	if len(got) != 1 {
		t.Errorf("expected rescheduled job claimable: %+v", got)
	}
}

func TestScheduler_Start_RunsJob(t *testing.T) {
	store := memstore.New()
	done := make(chan struct{}, 1)
	s, _ := gs.NewScheduler(
		gs.WithStore(store),
		gs.WithPollInterval(20*time.Millisecond),
	)
	s.Register("ping", func(_ context.Context, _ []byte) error {
		done <- struct{}{}
		return nil
	})
	if _, err := s.Now("ping", nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		_ = s.Start(ctx)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("job did not run within timeout")
	}
}

func TestScheduler_Start_RetriesThenFails(t *testing.T) {
	store := memstore.New()
	var attempts int32
	failed := make(chan struct{}, 1)
	s, _ := gs.NewScheduler(
		gs.WithStore(store),
		gs.WithPollInterval(10*time.Millisecond),
		gs.WithDefaultBackoff(gs.ExponentialBackoff{Base: 5 * time.Millisecond, Cap: 50 * time.Millisecond}),
		gs.WithHooks(gs.Hooks{
			OnFailure: func(_ gs.JobID, _ string, _ string, _ int, _ error) { failed <- struct{}{} },
		}),
	)
	s.Register("flaky", func(_ context.Context, _ []byte) error {
		atomic.AddInt32(&attempts, 1)
		return errors.New("nope")
	})
	_, _ = s.Now("flaky", nil, gs.WithMaxAttempts(3))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = s.Start(ctx) }()

	select {
	case <-failed:
	case <-ctx.Done():
		t.Fatalf("terminal failure not observed; attempts so far: %d", atomic.LoadInt32(&attempts))
	}
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Errorf("expected exactly 3 attempts, got %d", got)
	}
}
