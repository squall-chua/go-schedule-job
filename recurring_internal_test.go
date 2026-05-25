package goschedule

import (
	"testing"
	"time"
)

func TestRecurring_EveryDoesNotDrift(t *testing.T) {
	// Direct unit test on the advance helper to keep this fast and deterministic.
	// We construct a runner with a real logger; advance only inspects the spec.
	r := &recurringRunner{}
	spec := RecurringSpec{Every: time.Second, NextRunAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	// Advancing from spec.NextRunAt should give exactly one second later — no drift.
	got := r.advance(spec, spec.NextRunAt)
	want := spec.NextRunAt.Add(time.Second)
	if !got.Equal(want) {
		t.Errorf("advance from NextRunAt: got %v, want %v", got, want)
	}
}
