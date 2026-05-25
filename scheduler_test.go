package goschedule_test

import (
	"context"
	"errors"
	"testing"

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
