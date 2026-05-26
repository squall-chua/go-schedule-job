package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Listener pumps Postgres NOTIFY events for a queue onto channel C.
// Close releases the underlying connection and goroutine.
//
// Listener is a Postgres-specific convenience; it is NOT part of
// goschedule.Store. The default core dispatcher polls — use Listener if you
// want to drive your own dispatcher with sub-second wake-ups.
type Listener struct {
	C      <-chan struct{}
	conn   *pgx.Conn
	cancel context.CancelFunc
	done   chan struct{}
}

// Listen opens a dedicated connection and starts a background goroutine that
// pumps NOTIFY events for the given queue into the returned Listener.C
// channel. The goroutine exits when ctx is cancelled or l.Close is called.
func (s *Store) Listen(ctx context.Context, queue string) (*Listener, error) {
	cfg := s.pool.Config().ConnConfig.Copy()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres listen connect: %w", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+pgQuoteIdent(notifyChannel(queue))); err != nil {
		_ = conn.Close(ctx)
		return nil, fmt.Errorf("postgres LISTEN: %w", err)
	}
	pumpCtx, cancel := context.WithCancel(ctx)
	ch := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, err := conn.WaitForNotification(pumpCtx)
			if err != nil {
				return
			}
			select {
			case ch <- struct{}{}:
			default:
				// channel full — coalesce; one signal is enough to provoke a poll.
			}
		}
	}()
	return &Listener{C: ch, conn: conn, cancel: cancel, done: done}, nil
}

// Close stops the background goroutine and releases the connection.
func (l *Listener) Close() error {
	l.cancel()
	<-l.done
	return l.conn.Close(context.Background())
}

// pgQuoteIdent wraps an identifier in double quotes and escapes embedded
// quotes — sufficient for the controlled "goschedule_<queue>" channel names
// we generate, which never contain unsafe characters in practice.
func pgQuoteIdent(s string) string {
	out := []byte{'"'}
	for i := 0; i < len(s); i++ {
		if s[i] == '"' {
			out = append(out, '"', '"')
		} else {
			out = append(out, s[i])
		}
	}
	out = append(out, '"')
	return string(out)
}
