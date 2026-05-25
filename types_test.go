package goschedule

import "testing"

func TestPriority_String(t *testing.T) {
	cases := []struct {
		p    Priority
		want string
	}{
		{PriorityLow, "low"},
		{PriorityNormal, "normal"},
		{PriorityHigh, "high"},
		{PriorityCritical, "critical"},
		{Priority(99), "unknown"},
	}
	for _, c := range cases {
		if got := c.p.String(); got != c.want {
			t.Errorf("Priority(%d).String() = %q, want %q", c.p, got, c.want)
		}
	}
}

func TestState_Terminal(t *testing.T) {
	if !StateSucceeded.Terminal() {
		t.Error("StateSucceeded should be terminal")
	}
	if !StateFailed.Terminal() {
		t.Error("StateFailed should be terminal")
	}
	if !StateCancelled.Terminal() {
		t.Error("StateCancelled should be terminal")
	}
	if StatePending.Terminal() || StateClaimed.Terminal() {
		t.Error("non-terminal states reported terminal")
	}
}
