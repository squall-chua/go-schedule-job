# goschedule

In-process job scheduler for Go. One-shot and recurring jobs, named queues with per-job priorities, retries, timeouts, lifecycle hooks, and a pluggable Store so you can run on a single node with the in-memory or SQLite backend and graduate to distributed Postgres/Redis without touching handler code.

## Features

- **Triggers**: `Now`, `At`, `After`, `Every` (fixed interval), `Cron` (full cron expression).
- **Named queues** with per-queue worker concurrency.
- **Priorities** (`PriorityLow`/`Normal`/`High`/`Critical`) — higher runs sooner.
- **Retries** with configurable exponential backoff and max attempts.
- **Per-job timeouts** via `context.Context`.
- **Cancel** and **Reschedule** by `JobID`.
- **Typed payloads** via the `Codec` interface — JSON and Protocol Buffers codecs ship separately.
- **Lifecycle hooks** (`OnEnqueue`/`OnStart`/`OnSuccess`/`OnFailure`/`OnRetry`) and `log/slog` integration.
- **Pluggable stores**: in-memory (default), SQLite (single-node persistent), Postgres (distributed, `LISTEN/NOTIFY` wake-ups), Redis (distributed, Cluster-compatible).
- **Distributed coordination**: per-worker heartbeats, visibility timeouts, stale-claim recovery, recurring-schedule leases.
- **Prometheus metrics** via an optional collector module.

## Install

```bash
# Core (in-memory store, no external deps beyond stdlib + cron parser)
go get github.com/squall-chua/go-schedule-job

# Optional backends — pick whichever you need
go get github.com/squall-chua/go-schedule-job/store/sqlite
go get github.com/squall-chua/go-schedule-job/store/postgres
go get github.com/squall-chua/go-schedule-job/store/redis

# Optional codecs for typed payloads
go get github.com/squall-chua/go-schedule-job/codec/json
go get github.com/squall-chua/go-schedule-job/codec/proto

# Optional Prometheus collector
go get github.com/squall-chua/go-schedule-job/metrics/prometheus
```

Go 1.25.1+.

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "os/signal"
    "syscall"
    "time"

    gs "github.com/squall-chua/go-schedule-job"
    "github.com/squall-chua/go-schedule-job/memstore"
)

func main() {
    sched, err := gs.NewScheduler(
        gs.WithStore(memstore.New()),
    )
    if err != nil {
        panic(err)
    }

    sched.Register("greet", func(_ context.Context, payload []byte) error {
        fmt.Printf("hello, %s\n", payload)
        return nil
    })

    _, _ = sched.Now("greet", []byte("world"))
    _, _ = sched.After(2*time.Second, "greet", []byte("future"))

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    _ = sched.Start(ctx) // blocks until ctx is cancelled
}
```

## Examples

### Triggers — Now, At, After, Every, Cron

```go
// One-shot, immediate.
sched.Now("send-email", payload)

// One-shot, at a specific time.
sched.At(time.Now().Add(time.Hour), "send-email", payload)

// One-shot, after a delay.
sched.After(30*time.Minute, "send-email", payload)

// Recurring, every 5 minutes.
sched.Every(5*time.Minute, "poll-feed", nil)

// Recurring, by cron expression (robfig/cron syntax).
sched.Cron("0 9 * * MON", "weekly-report", nil)
```

### Queues and priorities

Higher priority jobs are claimed first. Different queues drain in parallel with independent worker pools.

```go
sched, _ := gs.NewScheduler(
    gs.WithStore(memstore.New()),
    gs.WithQueues(map[string]int{
        "default": 8,  // 8 concurrent workers
        "email":   2,
        "video":   1,
    }),
)

sched.Now("send", payload, gs.WithQueue("email"), gs.WithPriority(gs.PriorityHigh))
sched.Now("transcode", payload, gs.WithQueue("video"), gs.WithPriority(gs.PriorityLow))
```

### Retries and timeouts

Each transient failure schedules a retry with exponential backoff (default `100ms * 2^(attempt-1)` capped at 5 minutes). After `MaxAttempts` the job moves to terminal failure.

```go
sched.Register("fetch", func(ctx context.Context, _ []byte) error {
    return externalAPICall(ctx) // honour ctx for cancellation/timeout
})

sched.Now("fetch", nil,
    gs.WithMaxAttempts(5),
    gs.WithTimeout(30*time.Second),
)
```

Override the default backoff per scheduler:

```go
sched, _ := gs.NewScheduler(
    gs.WithStore(memstore.New()),
    gs.WithDefaultBackoff(gs.ExponentialBackoff{
        Base: 500 * time.Millisecond,
        Cap:  10 * time.Minute,
    }),
)
```

### Cancel and reschedule

```go
id, _ := sched.At(time.Now().Add(time.Hour), "send-reminder", payload)

// Change the firing time:
_ = sched.Reschedule(id, time.Now().Add(2*time.Hour))

// Or cancel entirely:
_ = sched.Cancel(id)
```

### Typed payloads (Codec)

Drop the `[]byte` plumbing and dispatch typed values directly. Pair a generic helper with any `Codec` — the JSON one ships as a separate module.

```go
import jsoncodec "github.com/squall-chua/go-schedule-job/codec/json"

type EmailJob struct {
    To      string
    Subject string
    Body    string
}

codec := jsoncodec.New()

gs.RegisterTyped[EmailJob](sched, "send-email", codec,
    func(ctx context.Context, job EmailJob) error {
        return smtpSend(ctx, job.To, job.Subject, job.Body)
    },
)

gs.NowTyped(sched, "send-email", codec, EmailJob{
    To:      "alice@example.com",
    Subject: "hello",
    Body:    "world",
})

gs.AfterTyped(sched, 10*time.Minute, "send-email", codec, EmailJob{...})
gs.EveryTyped(sched, time.Hour, "send-email", codec, EmailJob{...})
gs.CronTyped(sched, "0 8 * * *", "send-email", codec, EmailJob{...})
```

`Codec` is a 3-method interface. A Protocol Buffers codec ships in `codec/proto`:

```go
import (
    protocodec "github.com/squall-chua/go-schedule-job/codec/proto"
    "google.golang.org/protobuf/types/known/wrapperspb"
)

codec := protocodec.New()

gs.RegisterTyped[*wrapperspb.StringValue](sched, "echo", codec,
    func(ctx context.Context, m *wrapperspb.StringValue) error {
        fmt.Println(m.GetValue())
        return nil
    },
)

gs.NowTyped(sched, "echo", codec, wrapperspb.String("ping"))
```

Payload types passed to the proto codec must implement `proto.Message` — generated `*mypb.Foo` types from `protoc-gen-go` work directly. Implement your own `Codec` for msgpack, CBOR, etc.

### Lifecycle hooks and slog

```go
sched, _ := gs.NewScheduler(
    gs.WithStore(memstore.New()),
    gs.WithLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil))),
    gs.WithHooks(gs.Hooks{
        OnEnqueue: func(id gs.JobID, name, queue string) {
            log.Printf("enqueued %s on %s/%s", id, queue, name)
        },
        OnStart: func(id gs.JobID, name, queue string, attempt int) {
            log.Printf("starting %s attempt %d", id, attempt)
        },
        OnSuccess: func(id gs.JobID, name, queue string, attempt int, d time.Duration) {
            log.Printf("ok %s in %s", id, d)
        },
        OnFailure: func(id gs.JobID, name, queue string, attempt int, err error) {
            log.Printf("DEAD %s after %d attempts: %v", id, attempt, err)
        },
        OnRetry: func(id gs.JobID, name, queue string, attempt int, err error, nextAt time.Time) {
            log.Printf("retry %s at %s (was: %v)", id, nextAt, err)
        },
    }),
)
```

### Backends

The default `memstore.New()` is process-local and best-effort. Swap it for a persistent or distributed Store without touching handler code.

```go
// SQLite — single-node, persistent, no CGO.
import sqlitestore "github.com/squall-chua/go-schedule-job/store/sqlite"

store, err := sqlitestore.New("./jobs.db")
if err != nil { panic(err) }
defer store.Close()
```

```go
// Postgres — distributed, FOR UPDATE SKIP LOCKED, LISTEN/NOTIFY wake-ups.
import pgstore "github.com/squall-chua/go-schedule-job/store/postgres"

store, err := pgstore.New(ctx, "postgres://user:pass@host:5432/db")
if err != nil { panic(err) }
defer store.Close()
```

```go
// Redis — distributed, Cluster-compatible (single hash tag), Lua-script claim.
import redisstore "github.com/squall-chua/go-schedule-job/store/redis"

// Standalone:
store, err := redisstore.New(ctx, "localhost:6379")
// Cluster (comma-separated seeds):
store, err = redisstore.New(ctx, "node1:6379,node2:6379,node3:6379")
if err != nil { panic(err) }
defer store.Close()
```

All three backends pass the same `storetest.Run(t, factory)` conformance suite. For Postgres and Redis you can run multiple Scheduler instances against one Store for horizontal scale — claims are coordinated via row locks (Postgres) or Lua atomics (Redis), and crashed workers are recovered after `visibilityTimeout` elapses.

### Distributed scheduler tuning

```go
sched, _ := gs.NewScheduler(
    gs.WithStore(pgStore),
    gs.WithWorkerID("worker-01"),                // default: random UUID
    gs.WithHeartbeatInterval(10 * time.Second),  // janitor cadence
    gs.WithVisibilityTimeout(5 * time.Minute),   // claim TTL before recovery
    gs.WithShutdownGrace(30 * time.Second),      // wait this long for in-flight jobs on Start ctx cancel
    gs.WithPollInterval(1 * time.Second),        // dispatcher tick (lower = lower latency, more DB load)
)
```

### Prometheus metrics

The collector module wires into the same `Hooks` interface and samples `Store.QueueSize` at scrape time.

```go
import (
    "github.com/prometheus/client_golang/prometheus"
    prom "github.com/squall-chua/go-schedule-job/metrics/prometheus"
)

pcol := prom.New(store, []string{"default", "email", "video"})

sched, _ := gs.NewScheduler(
    gs.WithStore(store),
    gs.WithQueues(map[string]int{"default": 8, "email": 2, "video": 1}),
    gs.WithHooks(pcol.Hooks()),
)

prometheus.MustRegister(pcol)
```

Exposed metrics:

| Name | Type | Labels |
|------|------|--------|
| `goschedule_jobs_enqueued_total` | counter | `queue`, `name` |
| `goschedule_jobs_succeeded_total` | counter | `queue`, `name` |
| `goschedule_jobs_failed_total` | counter | `queue`, `name` |
| `goschedule_jobs_retried_total` | counter | `queue`, `name` |
| `goschedule_job_duration_seconds` | histogram | `queue`, `name` |
| `goschedule_jobs_in_flight` | gauge | `queue` |
| `goschedule_queue_size` | gauge | `queue` |

### Shutdown

`Start(ctx)` blocks. Cancel the context to drain — dispatchers stop claiming, in-flight job contexts are cancelled, and `Start` returns once workers exit or `WithShutdownGrace` elapses, whichever comes first. Any jobs still claimed at shutdown will be recovered by another scheduler after `visibilityTimeout` (on persistent stores).

## Module layout

```
.                                # core module — Scheduler, Store interface, memstore, hooks
codec/json/                      # JSON Codec
codec/proto/                     # Protocol Buffers Codec
store/sqlite/                    # SQLite backend (modernc.org/sqlite — pure Go)
store/postgres/                  # Postgres backend (jackc/pgx v5)
store/redis/                     # Redis backend (redis/go-redis v9, standalone + Cluster)
metrics/prometheus/              # Prometheus collector
cmd/example/                     # Runnable example
```

Each backend is its own Go module so you only pull in what you import.

## Status

Pre-alpha. All five modules in place and tested; APIs may still shift. See `docs/superpowers/specs/` for the full design and `docs/superpowers/plans/` for the implementation plans.
