package goschedule

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// Every schedules a recurring job that fires every d.
func (s *Scheduler) Every(d time.Duration, name string, payload []byte, opts ...JobOption) (JobID, error) {
	if _, ok := s.handler(name); !ok {
		return "", fmt.Errorf("goschedule: no handler registered for %q", name)
	}
	if d <= 0 {
		return "", errors.New("goschedule: Every interval must be positive")
	}
	spec := buildRecurringSpec(s, name, payload, opts)
	spec.Every = d
	spec.NextRunAt = s.clock.Now().Add(d)
	if err := s.store.UpsertRecurring(context.Background(), spec); err != nil {
		return "", err
	}
	s.hooks.fireEnqueue(spec.ID, spec.Name, spec.Queue)
	logEnqueue(s.logger, spec.ID, spec.Name, spec.Queue, spec.NextRunAt)
	return spec.ID, nil
}

// Cron schedules a recurring job using a cron expression (parsed with robfig/cron/v3).
func (s *Scheduler) Cron(expr, name string, payload []byte, opts ...JobOption) (JobID, error) {
	if _, ok := s.handler(name); !ok {
		return "", fmt.Errorf("goschedule: no handler registered for %q", name)
	}
	sched, err := cron.ParseStandard(expr)
	if err != nil {
		return "", fmt.Errorf("goschedule: parse cron %q: %w", expr, err)
	}
	spec := buildRecurringSpec(s, name, payload, opts)
	spec.Cron = expr
	spec.NextRunAt = sched.Next(s.clock.Now())
	if err := s.store.UpsertRecurring(context.Background(), spec); err != nil {
		return "", err
	}
	s.hooks.fireEnqueue(spec.ID, spec.Name, spec.Queue)
	logEnqueue(s.logger, spec.ID, spec.Name, spec.Queue, spec.NextRunAt)
	return spec.ID, nil
}

func buildRecurringSpec(s *Scheduler, name string, payload []byte, opts []JobOption) RecurringSpec {
	// Reuse JobOption by applying to a Job and copying fields back.
	tmp := Job{Queue: "default", Priority: PriorityNormal, MaxAttempts: 3}
	for _, opt := range opts {
		opt(&tmp)
	}
	return RecurringSpec{
		ID:          JobID(uuid.NewString()),
		Name:        name,
		Queue:       tmp.Queue,
		Priority:    tmp.Priority,
		Payload:     payload,
		CodecName:   tmp.CodecName,
		Delivery:    tmp.Delivery,
		Timeout:     tmp.Timeout,
		MaxAttempts: tmp.MaxAttempts,
		Catchup:     tmp.catchup,
	}
}

// recurringRunner periodically scans recurring specs, acquires a lease, and enqueues the next fire.
type recurringRunner struct {
	store      Store
	clock      Clock
	logger     *slog.Logger
	workerID   string
	leaseEvery time.Duration
}

func (r *recurringRunner) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.clock.After(r.leaseEvery):
		}
		now := r.clock.Now()
		specs, err := r.store.ListRecurring(ctx)
		if err != nil {
			r.logger.Error("goschedule: recurring list failed", slog.Any("err", err))
			continue
		}
		for _, spec := range specs {
			if spec.NextRunAt.After(now) {
				continue
			}
			ok, err := r.store.AcquireRecurringLease(ctx, spec.ID, now.Add(r.leaseEvery), r.workerID)
			if err != nil || !ok {
				continue
			}
			fires := r.computeFires(spec, now)
			for _, fireAt := range fires {
				j := Job{
					ID:          JobID(uuid.NewString()),
					Name:        spec.Name,
					Queue:       spec.Queue,
					Priority:    spec.Priority,
					Payload:     spec.Payload,
					CodecName:   spec.CodecName,
					RunAt:       fireAt,
					State:       StatePending,
					MaxAttempts: spec.MaxAttempts,
					Delivery:    spec.Delivery,
					Timeout:     spec.Timeout,
					RecurringID: spec.ID,
					CreatedAt:   now,
				}
				if err := r.store.Save(ctx, j); err != nil {
					r.logger.Error("goschedule: recurring save failed",
						slog.String("spec", string(spec.ID)), slog.Any("err", err))
				}
			}
			next := r.nextAfter(spec, now)
			_ = r.store.UpdateRecurringNextRun(ctx, spec.ID, next, now)
		}
	}
}

func (r *recurringRunner) computeFires(spec RecurringSpec, now time.Time) []time.Time {
	fires := []time.Time{spec.NextRunAt}
	if !spec.Catchup {
		return fires
	}
	at := spec.NextRunAt
	for {
		next := r.advance(spec, at)
		if next.After(now) {
			break
		}
		fires = append(fires, next)
		at = next
	}
	return fires
}

func (r *recurringRunner) nextAfter(spec RecurringSpec, now time.Time) time.Time {
	return r.advance(spec, now)
}

func (r *recurringRunner) advance(spec RecurringSpec, from time.Time) time.Time {
	if spec.Every > 0 {
		return from.Add(spec.Every)
	}
	if spec.Cron != "" {
		sched, err := cron.ParseStandard(spec.Cron)
		if err != nil {
			r.logger.Error("goschedule: re-parse cron failed",
				slog.String("expr", spec.Cron), slog.Any("err", err))
			return from.Add(time.Hour)
		}
		return sched.Next(from)
	}
	return from.Add(time.Hour)
}
