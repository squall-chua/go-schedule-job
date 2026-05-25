package goschedule_test

import (
	"context"
	"errors"
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
