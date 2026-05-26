package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/store/sqlite"
	"github.com/squall-chua/go-schedule-job/storetest"
)

func openTempStore(t *testing.T) *sqlite.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := sqlite.New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSQLiteStore_OpenAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestSQLiteStore_OpenAppliesSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := sqlite.New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Opening again should be idempotent (CREATE TABLE IF NOT EXISTS).
	s2, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	s2.Close()
}

func TestSQLiteStore_SaveInsertsRow(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	j := gs.Job{
		ID:          "j1",
		Queue:       "default",
		Name:        "do",
		Priority:    gs.PriorityNormal,
		RunAt:       now,
		State:       gs.StatePending,
		MaxAttempts: 3,
		CreatedAt:   now,
	}
	if err := s.Save(ctx, j); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Re-Save acts as upsert.
	j.Name = "renamed"
	if err := s.Save(ctx, j); err != nil {
		t.Fatalf("Save (upsert): %v", err)
	}
}

func TestSQLiteStore_ClaimDueRespectsPriority(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	mkj := func(id string, p gs.Priority, runAt time.Time) gs.Job {
		return gs.Job{ID: gs.JobID(id), Queue: "default", Name: "n", Priority: p, RunAt: runAt, State: gs.StatePending, MaxAttempts: 3, CreatedAt: now}
	}
	_ = s.Save(ctx, mkj("low", gs.PriorityLow, now.Add(-2*time.Second)))
	_ = s.Save(ctx, mkj("crit", gs.PriorityCritical, now.Add(-1*time.Second)))

	got, err := s.ClaimDue(ctx, "default", now, 10, "w1", now.Add(time.Minute))
	if err != nil {
		t.Fatalf("ClaimDue: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 jobs, got %d", len(got))
	}
	if got[0].ID != "crit" || got[1].ID != "low" {
		t.Errorf("priority order wrong: %+v", []gs.JobID{got[0].ID, got[1].ID})
	}
	for _, j := range got {
		if j.State != gs.StateClaimed || j.LockedBy != "w1" {
			t.Errorf("job %s not marked claimed: %+v", j.ID, j)
		}
	}
}

func TestSQLiteStore_ClaimDueSkipsFutureJobs(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "later", Queue: "default", Name: "n", RunAt: now.Add(time.Hour), State: gs.StatePending, MaxAttempts: 3, CreatedAt: now})
	got, _ := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if len(got) != 0 {
		t.Errorf("future job claimed: %+v", got)
	}
}

// --- Ack ---

func TestSQLiteStore_Ack(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending, MaxAttempts: 3, CreatedAt: now})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))

	if err := s.Ack(ctx, "j"); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", now.Add(time.Hour), 10, "w", now.Add(time.Hour).Add(time.Minute))
	if len(got) != 0 {
		t.Errorf("acked job reappeared: %+v", got)
	}
}

func TestSQLiteStore_AckMissingReturnsErrJobNotFound(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	if err := s.Ack(ctx, "missing"); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound, got %v", err)
	}
}

func TestSQLiteStore_AckPendingReturnsErrJobNotFound(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: time.Now(), State: gs.StatePending, MaxAttempts: 3})
	if err := s.Ack(ctx, "j"); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound for pending Ack, got %v", err)
	}
}

// --- Fail ---

func TestSQLiteStore_FailRetries(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending, MaxAttempts: 3, CreatedAt: now})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))

	next := now.Add(2 * time.Second)
	if err := s.Fail(ctx, "j", "boom", next); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", next.Add(time.Millisecond), 1, "w", next.Add(time.Minute))
	if len(got) != 1 || got[0].Attempt != 1 || got[0].LastError != "boom" {
		t.Fatalf("retry not visible: %+v", got)
	}
}

func TestSQLiteStore_FailExhausts(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending, MaxAttempts: 1, CreatedAt: now})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Fail(ctx, "j", "boom", now.Add(time.Second)); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", now.Add(time.Hour), 10, "w", now.Add(time.Hour).Add(time.Minute))
	if len(got) != 0 {
		t.Errorf("exhausted job reappeared: %+v", got)
	}
}

func TestSQLiteStore_FailPendingReturnsErrJobNotFound(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: time.Now(), State: gs.StatePending})
	if err := s.Fail(ctx, "j", "x", time.Now()); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound for pending Fail, got %v", err)
	}
}

// --- Cancel ---

func TestSQLiteStore_CancelPending(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: time.Now(), State: gs.StatePending})
	if err := s.Cancel(ctx, "j"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", time.Now(), 10, "w", time.Now().Add(time.Minute))
	if len(got) != 0 {
		t.Errorf("cancelled job reappeared: %+v", got)
	}
}

func TestSQLiteStore_CancelClaimedRejected(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending, MaxAttempts: 3})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Cancel(ctx, "j"); err != gs.ErrJobNotPending {
		t.Errorf("want ErrJobNotPending, got %v", err)
	}
}

func TestSQLiteStore_CancelMissing(t *testing.T) {
	s := openTempStore(t)
	if err := s.Cancel(context.Background(), "missing"); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound, got %v", err)
	}
}

// --- Reschedule ---

func TestSQLiteStore_Reschedule(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: now.Add(time.Hour), State: gs.StatePending, MaxAttempts: 3})
	if err := s.Reschedule(ctx, "j", now); err != nil {
		t.Fatalf("Reschedule: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if len(got) != 1 {
		t.Errorf("rescheduled job not claimable: %+v", got)
	}
}

func TestSQLiteStore_RescheduleClaimedRejected(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending, MaxAttempts: 3})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Reschedule(ctx, "j", now); err != gs.ErrJobNotPending {
		t.Errorf("want ErrJobNotPending, got %v", err)
	}
}

func TestSQLiteStore_RescheduleMissing(t *testing.T) {
	s := openTempStore(t)
	if err := s.Reschedule(context.Background(), "missing", time.Now()); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound, got %v", err)
	}
}

// --- Heartbeat / RecoverStale / QueueSize ---

func TestSQLiteStore_RecoverStale(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending, MaxAttempts: 3})
	// Claim with a lockUntil in the past so the claim is immediately stale.
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(-time.Minute))

	recovered, err := s.RecoverStale(ctx, now)
	if err != nil {
		t.Fatalf("RecoverStale: %v", err)
	}
	if recovered != 1 {
		t.Errorf("want 1 recovered, got %d", recovered)
	}
	got, _ := s.ClaimDue(ctx, "default", now, 1, "w2", now.Add(time.Minute))
	if len(got) != 1 {
		t.Errorf("recovered job not re-claimable: %+v", got)
	}
}

func TestSQLiteStore_HeartbeatIsNoop(t *testing.T) {
	s := openTempStore(t)
	if err := s.Heartbeat(context.Background(), "w1", time.Now()); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
}

func TestSQLiteStore_QueueSize(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now()
	_ = s.Save(ctx, gs.Job{ID: "a", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending})
	_ = s.Save(ctx, gs.Job{ID: "b", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending})
	n, err := s.QueueSize(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("want 2, got %d", n)
	}
}

// --- Recurring CRUD ---

func TestSQLiteStore_RecurringCRUD(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	spec := gs.RecurringSpec{
		ID: "r1", Name: "tick", Queue: "default", Every: time.Second, Priority: gs.PriorityNormal, MaxAttempts: 3,
	}
	if err := s.UpsertRecurring(ctx, spec); err != nil {
		t.Fatal(err)
	}
	list, err := s.ListRecurring(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != "r1" || list[0].Every != time.Second {
		t.Fatalf("list mismatch: %+v", list)
	}

	// Upsert again with a different name — should update.
	spec.Name = "tock"
	if err := s.UpsertRecurring(ctx, spec); err != nil {
		t.Fatal(err)
	}
	list, _ = s.ListRecurring(ctx)
	if list[0].Name != "tock" {
		t.Errorf("upsert did not update name: %+v", list[0])
	}

	if err := s.DeleteRecurring(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	list, _ = s.ListRecurring(ctx)
	if len(list) != 0 {
		t.Errorf("expected empty list, got %+v", list)
	}
}

func TestSQLiteStore_UpdateRecurringNextRun(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	spec := gs.RecurringSpec{ID: "r1", Name: "t", Queue: "default", Every: time.Second}
	_ = s.UpsertRecurring(ctx, spec)
	next := now.Add(time.Minute)
	if err := s.UpdateRecurringNextRun(ctx, "r1", next, now); err != nil {
		t.Fatal(err)
	}
	list, _ := s.ListRecurring(ctx)
	if !list[0].NextRunAt.Equal(next) || !list[0].LastFireAt.Equal(now) {
		t.Errorf("update did not apply: %+v", list[0])
	}
}

// --- AcquireRecurringLease ---

func TestSQLiteStore_AcquireRecurringLease(t *testing.T) {
	s := openTempStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.UpsertRecurring(ctx, gs.RecurringSpec{ID: "r1", Name: "n", Queue: "default", Every: time.Second})

	ok, err := s.AcquireRecurringLease(ctx, "r1", now.Add(time.Minute), "w1")
	if err != nil || !ok {
		t.Fatalf("first lease: ok=%v err=%v", ok, err)
	}
	// Second attempt by a different worker, while w1 still holds the lease, must fail.
	ok2, err := s.AcquireRecurringLease(ctx, "r1", now.Add(time.Minute), "w2")
	if err != nil {
		t.Fatal(err)
	}
	if ok2 {
		t.Error("second lease should be rejected while first holds it")
	}
	// Time-warp isn't available, so to verify the expiration path we upsert a
	// fresh spec whose lease is already in the past, then prove a new worker
	// can acquire it.
	_ = s.UpsertRecurring(ctx, gs.RecurringSpec{
		ID: "r2", Name: "n", Queue: "default", Every: time.Second,
		LeaseUntil: now.Add(-time.Minute), LeasedBy: "old",
	})
	ok3, err := s.AcquireRecurringLease(ctx, "r2", now.Add(time.Minute), "w3")
	if err != nil || !ok3 {
		t.Fatalf("expired lease should be acquirable: ok=%v err=%v", ok3, err)
	}
}

func TestSQLiteStore_Conformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) (gs.Store, func()) {
		s := openTempStore(t)
		return s, func() {} // openTempStore already registers Close via t.Cleanup
	})
}

func TestSQLiteStore_SurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := sqlite.New(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := s.Save(ctx, gs.Job{
		ID: "persist", Queue: "default", Name: "n",
		RunAt: now, State: gs.StatePending, MaxAttempts: 3, CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	got, err := s2.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "persist" {
		t.Fatalf("persisted job not visible after reopen: %+v", got)
	}
}
