// Package prometheus exposes a Prometheus collector for a goschedule Scheduler.
//
// Usage:
//
//	pcol := prom.New(store, []string{"default", "email"})
//	sched, _ := goschedule.NewScheduler(
//	    goschedule.WithStore(store),
//	    goschedule.WithQueues(map[string]int{"default": 4, "email": 2}),
//	    goschedule.WithHooks(pcol.Hooks()),
//	)
//	prometheus.MustRegister(pcol)
package prometheus
