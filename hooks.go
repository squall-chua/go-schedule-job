package goschedule

import "time"

// Hooks lets users observe job lifecycle events. All fields are optional.
type Hooks struct {
	OnEnqueue func(id JobID, name, queue string)
	OnStart   func(id JobID, name, queue string, attempt int)
	OnSuccess func(id JobID, name, queue string, attempt int, duration time.Duration)
	OnFailure func(id JobID, name, queue string, attempt int, err error)
	OnRetry   func(id JobID, name, queue string, attempt int, err error, nextAt time.Time)
}

func (h Hooks) fireEnqueue(id JobID, name, queue string) {
	if h.OnEnqueue != nil {
		h.OnEnqueue(id, name, queue)
	}
}
func (h Hooks) fireStart(id JobID, name, queue string, attempt int) {
	if h.OnStart != nil {
		h.OnStart(id, name, queue, attempt)
	}
}
func (h Hooks) fireSuccess(id JobID, name, queue string, attempt int, d time.Duration) {
	if h.OnSuccess != nil {
		h.OnSuccess(id, name, queue, attempt, d)
	}
}
func (h Hooks) fireFailure(id JobID, name, queue string, attempt int, err error) {
	if h.OnFailure != nil {
		h.OnFailure(id, name, queue, attempt, err)
	}
}
func (h Hooks) fireRetry(id JobID, name, queue string, attempt int, err error, nextAt time.Time) {
	if h.OnRetry != nil {
		h.OnRetry(id, name, queue, attempt, err, nextAt)
	}
}
