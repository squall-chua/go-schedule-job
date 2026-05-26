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
