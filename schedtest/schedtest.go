// Package schedtest provides test utilities for goschedule users and contributors.
package schedtest

import (
	"sort"
	"sync"
	"time"

	gs "github.com/squallchua/goschedule"
)

// FakeClock is a deterministic Clock used in tests.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	deadline time.Time
	ch       chan time.Time
}

// NewFakeClock returns a FakeClock anchored at start.
func NewFakeClock(start time.Time) *FakeClock { return &FakeClock{now: start} }

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) Sleep(d time.Duration) {
	<-c.After(d)
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	w := &waiter{deadline: c.now.Add(d), ch: ch}
	c.waiters = append(c.waiters, w)
	sort.SliceStable(c.waiters, func(i, j int) bool { return c.waiters[i].deadline.Before(c.waiters[j].deadline) })
	if d <= 0 {
		c.fireDueLocked()
	}
	return ch
}

// Advance moves the clock forward by d and fires any waiters whose deadlines elapsed.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	c.fireDueLocked()
}

func (c *FakeClock) fireDueLocked() {
	keep := c.waiters[:0]
	for _, w := range c.waiters {
		if !w.deadline.After(c.now) {
			w.ch <- c.now
			close(w.ch)
		} else {
			keep = append(keep, w)
		}
	}
	c.waiters = keep
}

// compile-time assertion
var _ gs.Clock = (*FakeClock)(nil)
