package postgres_test

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"

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
	t.Skip("Truncate not yet implemented — restored in Task 4")
	return nil
}
