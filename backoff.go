package goschedule

import (
	"math"
	"time"
)

// BackoffStrategy yields the next attempt delay given the failed attempt number (1-indexed).
type BackoffStrategy interface {
	Next(attempt int) time.Duration
}

// ExponentialBackoff returns Base * 2^(attempt-1), capped at Cap.
type ExponentialBackoff struct {
	Base time.Duration
	Cap  time.Duration
}

func (e ExponentialBackoff) Next(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := e.Base
	for i := 1; i < attempt; i++ {
		prev := d
		d *= 2
		if d < prev {
			// Overflow — cap at maximum non-negative duration.
			d = math.MaxInt64
			break
		}
		if e.Cap > 0 && d >= e.Cap {
			return e.Cap
		}
	}
	if e.Cap > 0 && d > e.Cap {
		return e.Cap
	}
	if d < 0 {
		d = math.MaxInt64
	}
	return d
}

// DefaultBackoff is used when no per-job override is supplied.
var DefaultBackoff BackoffStrategy = ExponentialBackoff{Base: 100 * time.Millisecond, Cap: 5 * time.Minute}
