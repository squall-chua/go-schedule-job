package goschedule

import (
	"testing"
	"time"
)

func TestExponentialBackoff_Doubles(t *testing.T) {
	b := ExponentialBackoff{Base: 100 * time.Millisecond, Cap: 10 * time.Second}
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
	}
	for _, c := range cases {
		if got := b.Next(c.attempt); got != c.want {
			t.Errorf("Next(%d) = %v, want %v", c.attempt, got, c.want)
		}
	}
}

func TestExponentialBackoff_CapsOut(t *testing.T) {
	b := ExponentialBackoff{Base: time.Second, Cap: 5 * time.Second}
	if got := b.Next(100); got != 5*time.Second {
		t.Errorf("expected cap of 5s, got %v", got)
	}
}
