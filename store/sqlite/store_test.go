package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/store/sqlite"
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
