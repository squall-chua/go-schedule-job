package redis_test

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/store/redis"
)

// openTestStore boots a fresh miniredis and returns a Store pointing at it.
// The miniredis is torn down via t.Cleanup, so each test gets clean state.
func openTestStore(t *testing.T) *redis.Store {
	t.Helper()
	mr := miniredis.RunT(t) // miniredis.RunT registers its own cleanup
	s, err := redis.New(t.Context(), mr.Addr())
	if err != nil {
		t.Fatalf("redis.New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestRedisStore_SaveInsertsAndUpserts(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	j := gs.Job{
		ID: "j1", Queue: "default", Name: "do",
		Priority: gs.PriorityNormal, RunAt: now,
		State: gs.StatePending, MaxAttempts: 3, CreatedAt: now,
	}
	if err := s.Save(ctx, j); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Upsert path.
	j.Name = "renamed"
	if err := s.Save(ctx, j); err != nil {
		t.Fatalf("Save (upsert): %v", err)
	}
}

func TestRedisStore_ClaimDueRespectsPriority(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	mkj := func(id string, p gs.Priority, runAt time.Time) gs.Job {
		return gs.Job{ID: gs.JobID(id), Queue: "default", Name: "n", Priority: p, RunAt: runAt, State: gs.StatePending, MaxAttempts: 3, CreatedAt: now}
	}
	if err := s.Save(ctx, mkj("low", gs.PriorityLow, now.Add(-2*time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, mkj("crit", gs.PriorityCritical, now.Add(-1*time.Second))); err != nil {
		t.Fatal(err)
	}

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

func TestRedisStore_ClaimDueSkipsFutureJobs(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	if err := s.Save(ctx, gs.Job{
		ID: "later", Queue: "default", Name: "n",
		RunAt: now.Add(time.Hour), State: gs.StatePending,
		MaxAttempts: 3, CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ClaimDue(ctx, "default", now, 10, "w", now.Add(time.Minute))
	if len(got) != 0 {
		t.Errorf("future job claimed: %+v", got)
	}
}

func TestRedisStore_Ack(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
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

func TestRedisStore_AckMissing(t *testing.T) {
	s := openTestStore(t)
	if err := s.Ack(t.Context(), "missing"); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound, got %v", err)
	}
}

func TestRedisStore_AckPending(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: time.Now(), State: gs.StatePending, MaxAttempts: 3})
	if err := s.Ack(ctx, "j"); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound for pending Ack, got %v", err)
	}
}

func TestRedisStore_FailRetries(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
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

func TestRedisStore_FailExhausts(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
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

func TestRedisStore_FailMissing(t *testing.T) {
	s := openTestStore(t)
	if err := s.Fail(t.Context(), "missing", "x", time.Now()); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound, got %v", err)
	}
}

func TestRedisStore_FailPending(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: time.Now(), State: gs.StatePending})
	if err := s.Fail(ctx, "j", "x", time.Now()); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound for pending Fail, got %v", err)
	}
}
