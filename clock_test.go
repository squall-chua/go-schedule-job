package goschedule

import (
	"testing"
	"time"
)

func TestRealClock_NowAdvances(t *testing.T) {
	c := RealClock()
	a := c.Now()
	time.Sleep(time.Millisecond)
	b := c.Now()
	if !b.After(a) {
		t.Errorf("real clock should advance: a=%v b=%v", a, b)
	}
}
