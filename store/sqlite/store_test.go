package sqlite_test

import (
	"path/filepath"
	"testing"

	"github.com/squall-chua/go-schedule-job/store/sqlite"
)

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
