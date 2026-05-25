package goschedule

import "time"

// Clock is the time source used by the scheduler. Production uses RealClock();
// tests pass a fake clock from package schedtest for deterministic time travel.
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

// RealClock returns a Clock backed by the standard time package.
func RealClock() Clock { return realClock{} }

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) Sleep(d time.Duration)                  { time.Sleep(d) }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
