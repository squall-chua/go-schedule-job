package schedtest_test

import (
	"testing"
	"time"

	"github.com/squallchua/goschedule/schedtest"
)

func TestFakeClock_AdvanceUnblocksAfter(t *testing.T) {
	c := schedtest.NewFakeClock(time.Unix(0, 0))
	ch := c.After(time.Second)
	c.Advance(500 * time.Millisecond)
	select {
	case <-ch:
		t.Fatal("After should not have fired yet")
	default:
	}
	c.Advance(time.Second)
	select {
	case <-ch:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("After should have fired after Advance past deadline")
	}
}
