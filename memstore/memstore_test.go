package memstore_test

import (
	"context"
	"testing"
	"time"

	gs "github.com/squallchua/goschedule"
	"github.com/squallchua/goschedule/memstore"
)

func TestMemStore_SaveAndClaim(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	job := gs.Job{
		ID:          "j1",
		Name:        "do",
		Queue:       "default",
		Priority:    gs.PriorityNormal,
		RunAt:       now,
		State:       gs.StatePending,
		MaxAttempts: 3,
		CreatedAt:   now,
	}
	if err := s.Save(ctx, job); err != nil {
		t.Fatalf("save: %v", err)
	}

	claimed, err := s.ClaimDue(ctx, "default", now, 10, "w1", now.Add(time.Minute))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "j1" {
		t.Fatalf("expected [j1], got %+v", claimed)
	}
	if claimed[0].State != gs.StateClaimed {
		t.Errorf("claimed state = %v, want StateClaimed", claimed[0].State)
	}
}

func TestMemStore_ClaimRespectsRunAt(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "later", Queue: "default", RunAt: now.Add(time.Hour), State: gs.StatePending})
	got, err := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("future job should not be claimed yet, got %d", len(got))
	}
}

func TestMemStore_ClaimOrdersByPriorityThenRunAt(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "low-early", Queue: "default", Priority: gs.PriorityLow, RunAt: now.Add(-2 * time.Second), State: gs.StatePending})
	_ = s.Save(ctx, gs.Job{ID: "high-late", Queue: "default", Priority: gs.PriorityHigh, RunAt: now.Add(-1 * time.Second), State: gs.StatePending})
	got, err := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "high-late" || got[1].ID != "low-early" {
		t.Errorf("priority order wrong: got %+v", []gs.JobID{got[0].ID, got[1].ID})
	}
}

func TestMemStore_Ack(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", RunAt: now, State: gs.StatePending})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Ack(ctx, "j"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", now.Add(time.Hour), 10, "w", now.Add(time.Hour).Add(time.Minute))
	if len(got) != 0 {
		t.Errorf("acked job should be gone, got %d", len(got))
	}
}
