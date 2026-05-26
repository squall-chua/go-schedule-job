package postgres_test

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/store/postgres"
)

var (
	sharedDSN       string
	sharedAvailable bool
	startOnce       sync.Once
	teardown        func()
)

// pickFreePort returns an unused TCP port by binding to :0 and closing.
// embedded-postgres takes a fixed port at construction, so we pick one
// that's free right now.
func pickFreePort() (uint32, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := uint32(l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()
	return port, nil
}

func TestMain(m *testing.M) {
	startOnce.Do(func() {
		port, err := pickFreePort()
		if err != nil {
			log.Printf("pick free port: %v — embedded postgres tests will skip", err)
			return
		}
		cfg := embeddedpostgres.DefaultConfig().
			Username("postgres").
			Password("postgres").
			Database("goschedule").
			Port(port)
		db := embeddedpostgres.NewDatabase(cfg)
		if err := db.Start(); err != nil {
			log.Printf("embedded postgres start failed (network or cache?): %v — tests will skip", err)
			return
		}
		sharedDSN = fmt.Sprintf(
			"postgres://postgres:postgres@127.0.0.1:%d/goschedule?sslmode=disable",
			port,
		)
		sharedAvailable = true
		teardown = func() { _ = db.Stop() }
	})
	code := m.Run()
	if teardown != nil {
		teardown()
	}
	os.Exit(code)
}

// openTestStore returns a fresh-state Store sharing the embedded Postgres
// instance. Each call truncates all tables so tests start from a clean slate.
func openTestStore(t *testing.T) *postgres.Store {
	t.Helper()
	if !sharedAvailable {
		t.Skip("embedded postgres unavailable; skipping Postgres tests")
	}
	ctx := t.Context()
	s, err := postgres.New(ctx, sharedDSN)
	if err != nil {
		t.Fatalf("postgres.New: %v", err)
	}
	if err := s.Truncate(ctx); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPostgresStore_SaveInsertsAndUpserts(t *testing.T) {
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

func TestPostgresStore_ClaimDueRespectsPriority(t *testing.T) {
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

func TestPostgresStore_ClaimDueSkipsFutureJobs(t *testing.T) {
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

func TestPostgresStore_Ack(t *testing.T) {
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

func TestPostgresStore_AckMissing(t *testing.T) {
	s := openTestStore(t)
	if err := s.Ack(t.Context(), "missing"); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound, got %v", err)
	}
}

func TestPostgresStore_AckPending(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: time.Now(), State: gs.StatePending, MaxAttempts: 3})
	if err := s.Ack(ctx, "j"); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound for pending Ack, got %v", err)
	}
}

func TestPostgresStore_FailRetries(t *testing.T) {
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

func TestPostgresStore_FailExhausts(t *testing.T) {
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

func TestPostgresStore_FailMissing(t *testing.T) {
	s := openTestStore(t)
	if err := s.Fail(t.Context(), "missing", "x", time.Now()); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound, got %v", err)
	}
}

func TestPostgresStore_FailPending(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: time.Now(), State: gs.StatePending})
	if err := s.Fail(ctx, "j", "x", time.Now()); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound for pending Fail, got %v", err)
	}
}

func TestPostgresStore_CancelPending(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: time.Now(), State: gs.StatePending})
	if err := s.Cancel(ctx, "j"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	got, _ := s.ClaimDue(ctx, "default", time.Now(), 10, "w", time.Now().Add(time.Minute))
	if len(got) != 0 {
		t.Errorf("cancelled job reappeared: %+v", got)
	}
}

func TestPostgresStore_CancelClaimedRejected(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending, MaxAttempts: 3})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Cancel(ctx, "j"); err != gs.ErrJobNotPending {
		t.Errorf("want ErrJobNotPending, got %v", err)
	}
}

func TestPostgresStore_CancelMissing(t *testing.T) {
	s := openTestStore(t)
	if err := s.Cancel(t.Context(), "missing"); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound, got %v", err)
	}
}

func TestPostgresStore_Reschedule(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
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

func TestPostgresStore_RescheduleClaimedRejected(t *testing.T) {
	s := openTestStore(t)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	_ = s.Save(ctx, gs.Job{ID: "j", Queue: "default", Name: "n", RunAt: now, State: gs.StatePending, MaxAttempts: 3})
	_, _ = s.ClaimDue(ctx, "default", now, 1, "w", now.Add(time.Minute))
	if err := s.Reschedule(ctx, "j", now); err != gs.ErrJobNotPending {
		t.Errorf("want ErrJobNotPending, got %v", err)
	}
}

func TestPostgresStore_RescheduleMissing(t *testing.T) {
	s := openTestStore(t)
	if err := s.Reschedule(t.Context(), "missing", time.Now()); err != gs.ErrJobNotFound {
		t.Errorf("want ErrJobNotFound, got %v", err)
	}
}
