package redis_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/store/redis"
)

// twoStores returns two distinct Store instances pointing at the same
// miniredis instance, along with the miniredis handle (for clock control).
func twoStores(t *testing.T) (a, b *redis.Store, mr *miniredis.Miniredis) {
	t.Helper()
	mr = miniredis.RunT(t)
	ctx := t.Context()
	sa, err := redis.New(ctx, mr.Addr())
	if err != nil {
		t.Fatalf("New(a): %v", err)
	}
	sb, err := redis.New(ctx, mr.Addr())
	if err != nil {
		sa.Close()
		t.Fatalf("New(b): %v", err)
	}
	t.Cleanup(func() { _ = sa.Close(); _ = sb.Close() })
	return sa, sb, mr
}

// TestRedisStore_ConcurrentClaimNoDoubleDispatch: 50 jobs, two stores claim
// concurrently; union of claimed IDs = saved set, intersection = ∅.
func TestRedisStore_ConcurrentClaimNoDoubleDispatch(t *testing.T) {
	a, b, _ := twoStores(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	const n = 50
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("j%02d", i)
		if err := a.Save(ctx, gs.Job{
			ID: gs.JobID(id), Queue: "default", Name: "n",
			RunAt: now, State: gs.StatePending, MaxAttempts: 3, CreatedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
	}

	type result struct {
		jobs []gs.Job
		err  error
	}
	doClaim := func(s *redis.Store, worker string) result {
		got, err := s.ClaimDue(ctx, "default", now, n, worker, now.Add(time.Minute))
		return result{got, err}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var ra, rb result
	go func() { defer wg.Done(); ra = doClaim(a, "wa") }()
	go func() { defer wg.Done(); rb = doClaim(b, "wb") }()
	wg.Wait()

	if ra.err != nil || rb.err != nil {
		t.Fatalf("claim errors: a=%v b=%v", ra.err, rb.err)
	}

	seen := map[gs.JobID]string{}
	for _, j := range ra.jobs {
		seen[j.ID] = "a"
	}
	for _, j := range rb.jobs {
		if prev, ok := seen[j.ID]; ok {
			t.Errorf("double-dispatch: id=%s claimed by %s and b", j.ID, prev)
		}
		seen[j.ID] = "b"
	}
	if len(seen) != n {
		t.Errorf("expected %d distinct jobs claimed, got %d (a=%d b=%d)",
			n, len(seen), len(ra.jobs), len(rb.jobs))
	}
}

// TestRedisStore_ContestedRecurringLeaseExclusivity: two workers race to
// acquire the same lease. Exactly one wins each race.
func TestRedisStore_ContestedRecurringLeaseExclusivity(t *testing.T) {
	a, b, mr := twoStores(t)
	ctx := t.Context()
	_ = a.UpsertRecurring(ctx, gs.RecurringSpec{ID: "r1", Name: "n", Queue: "default", Every: time.Second})

	const races = 20
	for r := 0; r < races; r++ {
		mr.FastForward(2 * time.Minute) // expire any prior lease TTL

		var wg sync.WaitGroup
		wg.Add(2)
		var okA, okB bool
		go func() { defer wg.Done(); okA, _ = a.AcquireRecurringLease(ctx, "r1", time.Now().Add(time.Minute), "wa") }()
		go func() { defer wg.Done(); okB, _ = b.AcquireRecurringLease(ctx, "r1", time.Now().Add(time.Minute), "wb") }()
		wg.Wait()
		if okA == okB {
			t.Fatalf("race %d: exactly one acquire must succeed, got a=%v b=%v", r, okA, okB)
		}
	}
}

// TestRedisStore_StaleRecoveryAfterDeadWorker: worker A claims with a past
// lockUntil; worker B recovers via RecoverStale and re-claims.
func TestRedisStore_StaleRecoveryAfterDeadWorker(t *testing.T) {
	a, b, _ := twoStores(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = a.Save(ctx, gs.Job{
		ID: "j", Queue: "default", Name: "n",
		RunAt: now, State: gs.StatePending, MaxAttempts: 3, CreatedAt: now,
	})

	got, err := a.ClaimDue(ctx, "default", now, 1, "wa", now.Add(-time.Minute))
	if err != nil || len(got) != 1 {
		t.Fatalf("claim a: %+v err=%v", got, err)
	}

	got2, _ := b.ClaimDue(ctx, "default", now, 1, "wb", now.Add(time.Minute))
	if len(got2) != 0 {
		t.Fatalf("worker b should not claim while a holds: %+v", got2)
	}

	rec, err := b.RecoverStale(ctx, now)
	if err != nil || rec != 1 {
		t.Fatalf("recover stale: rec=%d err=%v", rec, err)
	}

	got3, err := b.ClaimDue(ctx, "default", now, 1, "wb", now.Add(time.Minute))
	if err != nil || len(got3) != 1 {
		t.Fatalf("post-recovery claim by b: %+v err=%v", got3, err)
	}
	if got3[0].LockedBy != "wb" {
		t.Errorf("after recovery, locked_by should be wb, got %q", got3[0].LockedBy)
	}
}
