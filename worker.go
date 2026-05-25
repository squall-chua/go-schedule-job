package goschedule

import (
	"context"
	"errors"
	"log/slog"
	"runtime/debug"
	"time"
)

type workerPool struct {
	concurrency int
	in          <-chan Job
	store       Store
	scheduler   *Scheduler
	clock       Clock
	logger      *slog.Logger
}

func (p *workerPool) run(ctx context.Context) {
	for i := 0; i < p.concurrency; i++ {
		go p.workerLoop(ctx)
	}
}

func (p *workerPool) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-p.in:
			if !ok {
				return
			}
			p.executeOne(ctx, j)
		}
	}
}

func (p *workerPool) executeOne(parent context.Context, j Job) {
	h, ok := p.scheduler.handler(j.Name)
	if !ok {
		_ = p.store.Fail(parent, j.ID, "no handler registered for "+j.Name, p.clock.Now().Add(time.Hour))
		p.scheduler.hooks.fireFailure(j.ID, j.Name, j.Queue, j.Attempt+1, errors.New("no handler"))
		logFailure(p.logger, j.ID, j.Name, j.Queue, j.Attempt+1, errors.New("no handler"))
		return
	}

	ctx := parent
	var cancel context.CancelFunc
	if j.Timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, j.Timeout)
		defer cancel()
	}

	attempt := j.Attempt + 1
	p.scheduler.hooks.fireStart(j.ID, j.Name, j.Queue, attempt)
	logStart(p.logger, j.ID, j.Name, j.Queue, attempt)

	start := p.clock.Now()
	err := safeInvoke(ctx, h, j.Payload)
	dur := p.clock.Now().Sub(start)

	if err == nil {
		if ackErr := p.store.Ack(parent, j.ID); ackErr != nil {
			p.logger.Error("goschedule: ack failed",
				slog.String("id", string(j.ID)), slog.Any("err", ackErr))
		}
		p.scheduler.hooks.fireSuccess(j.ID, j.Name, j.Queue, attempt, dur)
		logSuccess(p.logger, j.ID, j.Name, j.Queue, attempt, dur)
		return
	}

	maxAttempts := j.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if attempt >= maxAttempts {
		if failErr := p.store.Fail(parent, j.ID, err.Error(), p.clock.Now().Add(time.Hour)); failErr != nil {
			p.logger.Error("goschedule: fail (terminal) error", slog.Any("err", failErr))
		}
		p.scheduler.hooks.fireFailure(j.ID, j.Name, j.Queue, attempt, err)
		logFailure(p.logger, j.ID, j.Name, j.Queue, attempt, err)
		return
	}

	delay := p.scheduler.defaultBackoff.Next(attempt)
	nextAt := p.clock.Now().Add(delay)
	if failErr := p.store.Fail(parent, j.ID, err.Error(), nextAt); failErr != nil {
		p.logger.Error("goschedule: fail (retry) error", slog.Any("err", failErr))
		return
	}
	p.scheduler.hooks.fireRetry(j.ID, j.Name, j.Queue, attempt, err, nextAt)
	logRetry(p.logger, j.ID, j.Name, j.Queue, attempt, err, nextAt)
}

// safeInvoke wraps handler execution so a panic surfaces as an error.
func safeInvoke(ctx context.Context, h Handler, payload []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &panicError{r: r, stack: debug.Stack()}
		}
	}()
	return h(ctx, payload)
}

type panicError struct {
	r     any
	stack []byte
}

func (p *panicError) Error() string {
	return "panic in handler: " + toString(p.r)
}

func toString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case error:
		return s.Error()
	default:
		return ""
	}
}
