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

func TestMemStore_AckRejectsPendingJob(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "pending-j", Queue: "default", RunAt: now, State: gs.StatePending})
	// Ack on a still-pending job should not succeed silently.
	if err := s.Ack(ctx, "pending-j"); err == nil {
		t.Fatal("expected error when Acking a still-pending job")
	}
	// And the job must still be claimable afterward.
	got, _ := s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if len(got) != 1 || got[0].ID != "pending-j" {
		t.Fatalf("expected pending job to remain claimable, got %+v", got)
	}
}

func TestMemStore_FailReschedulesForRetry(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", RunAt: now, State: gs.StatePending, MaxAttempts: 3})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))

	nextAt := now.Add(2 * time.Second)
	if err := s.Fail(ctx, "j", "boom", nextAt); err != nil {
		t.Fatalf("fail: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", nextAt.Add(time.Millisecond), 1, "w", nextAt.Add(time.Minute))
	if len(got) != 1 || got[0].Attempt != 1 || got[0].LastError != "boom" {
		t.Errorf("expected re-enqueued job with attempt=1, got %+v", got)
	}
}

func TestMemStore_CancelRemovesPending(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", RunAt: time.Now(), State: gs.StatePending})
	if err := s.Cancel(ctx, "j"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", time.Now(), 10, "w", time.Now().Add(time.Minute))
	if len(got) != 0 {
		t.Errorf("cancelled job should not be claimed, got %d", len(got))
	}
}

func TestMemStore_CancelClaimedReturnsErrJobNotPending(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", RunAt: now, State: gs.StatePending})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Cancel(ctx, "j"); err != gs.ErrJobNotPending {
		t.Errorf("expected ErrJobNotPending, got %v", err)
	}
}

func TestMemStore_Reschedule(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", RunAt: now.Add(time.Hour), State: gs.StatePending})
	if err := s.Reschedule(ctx, "j", now); err != nil {
		t.Fatalf("reschedule: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if len(got) != 1 {
		t.Errorf("rescheduled job should be claimable now, got %d", len(got))
	}
}

func TestMemStore_FailRejectsPendingJob(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "pending-j", Queue: "default", RunAt: now, State: gs.StatePending, MaxAttempts: 3})
	// Fail on a still-pending job should not silently corrupt the heap.
	if err := s.Fail(ctx, "pending-j", "boom", now.Add(time.Second)); err == nil {
		t.Fatal("expected error when failing a still-pending job")
	}
	// The original pending job must still be claimable exactly once.
	got, _ := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if len(got) != 1 || got[0].ID != "pending-j" {
		t.Fatalf("expected pending job to remain claimable exactly once, got %+v", got)
	}
}

func TestMemStore_FailExhaustsAttemptsAndStops(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	j := gs.Job{ID: "j", Queue: "default", RunAt: now, State: gs.StatePending, MaxAttempts: 1}
	_ = s.Save(ctx, j)
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Fail(ctx, "j", "boom", now.Add(time.Second)); err != nil {
		t.Fatalf("fail: %v", err)
	}
	// After exhausting attempts the job must not reappear.
	got, _ := s.ClaimDue(ctx, "default", now.Add(time.Hour), 10, "w", now.Add(time.Hour).Add(time.Minute))
	if len(got) != 0 {
		t.Fatalf("exhausted job should not reappear, got %+v", got)
	}
}

func TestMemStore_RecurringUpsertListDelete(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	spec := gs.RecurringSpec{ID: "r1", Name: "tick", Queue: "default", Every: time.Second}
	if err := s.UpsertRecurring(ctx, spec); err != nil {
		t.Fatal(err)
	}
	list, _ := s.ListRecurring(ctx)
	if len(list) != 1 || list[0].ID != "r1" {
		t.Errorf("list: got %+v", list)
	}
	if err := s.DeleteRecurring(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	list, _ = s.ListRecurring(ctx)
	if len(list) != 0 {
		t.Errorf("expected empty list, got %+v", list)
	}
}

func TestMemStore_AcquireRecurringLease_AlwaysTrue(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	ok, err := s.AcquireRecurringLease(ctx, "r1", time.Now().Add(time.Minute), "w1")
	if err != nil || !ok {
		t.Errorf("expected (true, nil), got (%v, %v)", ok, err)
	}
}

func TestMemStore_QueueSize(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "a", Queue: "default", RunAt: now, State: gs.StatePending})
	_ = s.Save(ctx, gs.Job{ID: "b", Queue: "default", RunAt: now, State: gs.StatePending})
	n, err := s.QueueSize(ctx, "default")
	if err != nil || n != 2 {
		t.Errorf("expected 2, got (%d, %v)", n, err)
	}
}

func TestMemStore_HeartbeatAndRecoverStaleAreNoops(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	if err := s.Heartbeat(ctx, "w1", time.Now()); err != nil {
		t.Fatal(err)
	}
	n, err := s.RecoverStale(ctx, time.Now())
	if err != nil || n != 0 {
		t.Errorf("expected (0, nil), got (%d, %v)", n, err)
	}
}
