// Package storetest provides a shared conformance suite for Store implementations.
package storetest

import (
	"context"
	"testing"
	"time"

	gs "github.com/squallchua/goschedule"
)

// Factory returns a fresh, empty Store for one subtest, plus a teardown callback.
type Factory func(t *testing.T) (gs.Store, func())

// Run executes the full conformance suite.
func Run(t *testing.T, f Factory) {
	t.Helper()
	t.Run("SaveAndClaim", func(t *testing.T) { testSaveAndClaim(t, f) })
	t.Run("ClaimRespectsRunAt", func(t *testing.T) { testClaimRespectsRunAt(t, f) })
	t.Run("ClaimOrdersByPriority", func(t *testing.T) { testClaimOrdersByPriority(t, f) })
	t.Run("Ack", func(t *testing.T) { testAck(t, f) })
	t.Run("FailRetries", func(t *testing.T) { testFailRetries(t, f) })
	t.Run("FailExhausts", func(t *testing.T) { testFailExhausts(t, f) })
	t.Run("CancelPending", func(t *testing.T) { testCancelPending(t, f) })
	t.Run("CancelClaimedRejected", func(t *testing.T) { testCancelClaimedRejected(t, f) })
	t.Run("Reschedule", func(t *testing.T) { testReschedule(t, f) })
	t.Run("RecurringCRUD", func(t *testing.T) { testRecurringCRUD(t, f) })
	t.Run("AcquireRecurringLeaseAtLeastOnce", func(t *testing.T) { testRecurringLease(t, f) })
	t.Run("QueueSize", func(t *testing.T) { testQueueSize(t, f) })
	t.Run("MissingJobErrors", func(t *testing.T) { testMissingJobErrors(t, f) })
}

func mkJob(id, queue string, runAt time.Time, p gs.Priority) gs.Job {
	return gs.Job{ID: gs.JobID(id), Queue: queue, RunAt: runAt, State: gs.StatePending, Priority: p, MaxAttempts: 3}
}

// --- scenarios ---

func testSaveAndClaim(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	now := time.Now()
	if err := s.Save(ctx, mkJob("a", "default", now, gs.PriorityNormal)); err != nil {
		t.Fatal(err)
	}
	got, err := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("want [a], got %+v", got)
	}
}

func testClaimRespectsRunAt(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	now := time.Now()
	_ = s.Save(ctx, mkJob("future", "default", now.Add(time.Hour), gs.PriorityNormal))
	got, _ := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if len(got) != 0 {
		t.Fatalf("want 0, got %d", len(got))
	}
}

func testClaimOrdersByPriority(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	now := time.Now()
	_ = s.Save(ctx, mkJob("low", "default", now.Add(-2*time.Second), gs.PriorityLow))
	_ = s.Save(ctx, mkJob("crit", "default", now.Add(-1*time.Second), gs.PriorityCritical))
	got, _ := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if len(got) < 1 || got[0].ID != "crit" {
		t.Fatalf("expected crit first, got %+v", got)
	}
}

func testAck(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	now := time.Now()
	_ = s.Save(ctx, mkJob("j", "default", now, gs.PriorityNormal))
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Ack(ctx, "j"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ClaimDue(ctx, "default", now.Add(time.Hour), 10, "w", now.Add(time.Hour).Add(time.Minute))
	if len(got) != 0 {
		t.Fatalf("acked should be gone, got %d", len(got))
	}
}

func testFailRetries(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	now := time.Now()
	_ = s.Save(ctx, mkJob("j", "default", now, gs.PriorityNormal))
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	next := now.Add(2 * time.Second)
	_ = s.Fail(ctx, "j", "boom", next)
	got, _ := s.ClaimDue(ctx, "default", next.Add(time.Millisecond), 1, "w", next.Add(time.Minute))
	if len(got) != 1 || got[0].Attempt != 1 || got[0].LastError != "boom" {
		t.Fatalf("retry not visible: %+v", got)
	}
}

func testFailExhausts(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	now := time.Now()
	j := mkJob("j", "default", now, gs.PriorityNormal)
	j.MaxAttempts = 1
	_ = s.Save(ctx, j)
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	_ = s.Fail(ctx, "j", "boom", now.Add(time.Second))
	got, _ := s.ClaimDue(ctx, "default", now.Add(time.Hour), 1, "w", now.Add(time.Hour).Add(time.Minute))
	if len(got) != 0 {
		t.Fatalf("exhausted job should not reappear, got %+v", got)
	}
}

func testCancelPending(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	_ = s.Save(ctx, mkJob("j", "default", time.Now(), gs.PriorityNormal))
	if err := s.Cancel(ctx, "j"); err != nil {
		t.Fatal(err)
	}
}

func testCancelClaimedRejected(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	now := time.Now()
	_ = s.Save(ctx, mkJob("j", "default", now, gs.PriorityNormal))
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Cancel(ctx, "j"); err != gs.ErrJobNotPending {
		t.Fatalf("want ErrJobNotPending, got %v", err)
	}
}

func testReschedule(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	now := time.Now()
	_ = s.Save(ctx, mkJob("j", "default", now.Add(time.Hour), gs.PriorityNormal))
	if err := s.Reschedule(ctx, "j", now); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if len(got) != 1 {
		t.Fatalf("rescheduled job should be claimable: %+v", got)
	}
}

func testRecurringCRUD(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	_ = s.UpsertRecurring(ctx, gs.RecurringSpec{ID: "r1", Name: "tick", Queue: "default", Every: time.Second})
	list, _ := s.ListRecurring(ctx)
	if len(list) != 1 {
		t.Fatalf("want 1, got %d", len(list))
	}
	_ = s.DeleteRecurring(ctx, "r1")
	list, _ = s.ListRecurring(ctx)
	if len(list) != 0 {
		t.Fatalf("want 0, got %d", len(list))
	}
}

func testRecurringLease(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	ok, err := s.AcquireRecurringLease(ctx, "r1", time.Now().Add(time.Minute), "w1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("first lease should succeed")
	}
	// We don't assert exclusivity here — that's distributed-only behavior and tested in backend plans.
}

func testQueueSize(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	now := time.Now()
	_ = s.Save(ctx, mkJob("a", "default", now, gs.PriorityNormal))
	_ = s.Save(ctx, mkJob("b", "default", now, gs.PriorityNormal))
	n, err := s.QueueSize(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("want 2, got %d", n)
	}
}

func testMissingJobErrors(t *testing.T, f Factory) {
	s, done := f(t)
	defer done()
	ctx := context.Background()
	if err := s.Ack(ctx, "missing"); err != gs.ErrJobNotFound {
		t.Errorf("Ack(missing): want ErrJobNotFound, got %v", err)
	}
	if err := s.Fail(ctx, "missing", "x", time.Now()); err != gs.ErrJobNotFound {
		t.Errorf("Fail(missing): want ErrJobNotFound, got %v", err)
	}
	if err := s.Cancel(ctx, "missing"); err != gs.ErrJobNotFound {
		t.Errorf("Cancel(missing): want ErrJobNotFound, got %v", err)
	}
	if err := s.Reschedule(ctx, "missing", time.Now()); err != gs.ErrJobNotFound {
		t.Errorf("Reschedule(missing): want ErrJobNotFound, got %v", err)
	}
}
