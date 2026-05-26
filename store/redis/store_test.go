package redis_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"

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
