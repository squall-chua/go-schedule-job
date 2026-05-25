package goschedule

import (
	"context"
	"log/slog"
	"time"
)

// dispatcher claims due jobs for a queue and pushes them onto a worker channel.
type dispatcher struct {
	queue      string
	batchSize  int
	pollEvery  time.Duration
	visibility time.Duration
	workerID   string
	store      Store
	clock      Clock
	logger     *slog.Logger
	out        chan<- Job
}

func (d *dispatcher) run(ctx context.Context) {
	timer := d.clock.After(0) // fire immediately on start
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer:
		}
		now := d.clock.Now()
		jobs, err := d.store.ClaimDue(ctx, d.queue, now, d.batchSize, d.workerID, now.Add(d.visibility))
		if err != nil && ctx.Err() == nil {
			d.logger.Error("goschedule: dispatcher claim failed",
				slog.String("queue", d.queue), slog.Any("err", err))
		}
		for _, j := range jobs {
			select {
			case <-ctx.Done():
				return
			case d.out <- j:
			}
		}
		// Sleep before next claim. We can be smarter later (e.g. compute time-to-next-due);
		// for v1 a fixed poll interval is sufficient.
		timer = d.clock.After(d.pollEvery)
	}
}
