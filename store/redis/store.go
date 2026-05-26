// Package redis provides a Redis-backed Store for goschedule.
//
// Designed for horizontal scaling: N scheduler processes may share one
// Redis safely. Correctness rests on three mechanisms:
//
//   - ClaimDue is a single Lua script that walks priority buckets and
//     atomically moves jobs from pending into the claimed ZSET. Redis
//     serializes script execution, so concurrent schedulers hand out
//     disjoint job sets.
//   - AcquireRecurringLease uses a Lua script that combines existence
//     check, ownership check, and SET ... EX in one round trip.
//   - locked_until visibility timeouts plus RecoverStale (driven by
//     Redis's own clock) recover jobs from a crashed scheduler.
//
// All expiry comparisons route through Redis's clock (TIME command for
// RecoverStale, native TTL for leases), so schedulers with skewed wall
// clocks still agree on what is expired.
//
// Deployment modes: standalone Redis, sentinel/replica, and Redis Cluster
// are all supported through go-redis's UniversalClient. Under Cluster, all
// goschedule keys share the "{goschedule}" hash tag so they colocate on a
// single shard — you get HA + failover but not sharding of goschedule data
// itself (standard pattern for queue libraries on Cluster).
package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	gs "github.com/squall-chua/go-schedule-job"
)

// Store implements goschedule.Store backed by Redis.
type Store struct {
	rdb redis.UniversalClient
}

// New opens a Redis client against the given address(es) and returns a
// Store once a PING succeeds.
//
// The address argument accepts three forms:
//
//   - A single "host:port" string — standalone Redis.
//   - A comma-separated list of "host:port" entries — Redis Cluster.
//   - A redis://… or rediss://… URL — parsed via redis.ParseURL (single host).
//
// The same Store works against standalone Redis, sentinel/replica setups,
// and Redis Cluster because it uses go-redis's UniversalClient under the
// hood. All goschedule keys share the "{goschedule}" hash tag so multi-key
// Lua scripts work in Cluster mode without CROSSSLOT errors.
func New(ctx context.Context, addr string) (*Store, error) {
	opts, err := parseAddr(addr)
	if err != nil {
		return nil, fmt.Errorf("redis parse addr: %w", err)
	}
	rdb := redis.NewUniversalClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Store{rdb: rdb}, nil
}

// parseAddr turns the user-facing address string into go-redis universal
// options. URL form delegates to redis.ParseURL; comma-separated form is
// treated as Cluster (or sentinel if MasterName is later wired in).
func parseAddr(addr string) (*redis.UniversalOptions, error) {
	if strings.HasPrefix(addr, "redis://") || strings.HasPrefix(addr, "rediss://") {
		opt, err := redis.ParseURL(addr)
		if err != nil {
			return nil, err
		}
		return &redis.UniversalOptions{
			Addrs:    []string{opt.Addr},
			Username: opt.Username,
			Password: opt.Password,
			DB:       opt.DB,
		}, nil
	}
	addrs := strings.Split(addr, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	return &redis.UniversalOptions{Addrs: addrs}, nil
}

// Close releases the underlying client.
func (s *Store) Close() error { return s.rdb.Close() }

// Save persists or upserts the job. The HASH stores the full record; the
// pending ZSET (one per (queue, priority)) carries the ordering index.
func (s *Store) Save(ctx context.Context, j gs.Job) error {
	state := j.State
	if state == 0 {
		state = gs.StatePending
	}
	if j.MaxAttempts == 0 {
		j.MaxAttempts = 3
	}
	j.State = state

	score := float64(0)
	if !j.RunAt.IsZero() {
		score = float64(j.RunAt.UnixNano())
	}

	_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, jobKey(j.ID), serializeJob(j))
		if state == gs.StatePending {
			p.ZAdd(ctx, pendingKey(j.Queue, j.Priority), redis.Z{Score: score, Member: string(j.ID)})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("redis save: %w", err)
	}
	return nil
}

// asInt parses a HASH field as int, returning 0 on empty/missing.
func asInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// priorityBuckets returns the pending ZSET keys for the given queue in
// priority-DESC order (Critical first, Low last).
func priorityBuckets(queue string) []string {
	return []string{
		pendingKey(queue, gs.PriorityCritical),
		pendingKey(queue, gs.PriorityHigh),
		pendingKey(queue, gs.PriorityNormal),
		pendingKey(queue, gs.PriorityLow),
	}
}

func (s *Store) ClaimDue(ctx context.Context, queue string, now time.Time, n int, workerID string, lockUntil time.Time) ([]gs.Job, error) {
	buckets := priorityBuckets(queue)
	keys := append(buckets, claimedKey)

	res, err := claimDueScript.Run(ctx, s.rdb, keys,
		formatTime(now), n, workerID, formatTime(lockUntil), keyPrefix+"job:", strconv.Itoa(int(gs.StateClaimed)),
	).Result()
	if err != nil {
		return nil, fmt.Errorf("redis claim: %w", err)
	}
	idsAny, ok := res.([]any)
	if !ok {
		return nil, fmt.Errorf("redis claim: unexpected script result type %T", res)
	}
	if len(idsAny) == 0 {
		return nil, nil
	}

	// Pipelined HGETALL for each claimed ID.
	pipe := s.rdb.Pipeline()
	gets := make([]*redis.MapStringStringCmd, len(idsAny))
	for i, id := range idsAny {
		gets[i] = pipe.HGetAll(ctx, jobKey(gs.JobID(id.(string))))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("redis claim hgetall: %w", err)
	}
	out := make([]gs.Job, len(gets))
	for i, g := range gets {
		out[i] = deserializeJob(g.Val())
	}
	return out, nil
}

var ackScript = redis.NewScript(`
local job = KEYS[1]
local claimed = KEYS[2]
local id = ARGV[1]
local state_claimed = ARGV[2]
local state = redis.call('HGET', job, 'state')
if state ~= state_claimed then
    return 'missing'
end
redis.call('ZREM', claimed, id)
redis.call('DEL', job)
return 'ok'
`)

func (s *Store) Ack(ctx context.Context, id gs.JobID) error {
	res, err := ackScript.Run(ctx, s.rdb,
		[]string{jobKey(id), claimedKey},
		string(id), strconv.Itoa(int(gs.StateClaimed)),
	).Result()
	if err != nil {
		return fmt.Errorf("redis ack: %w", err)
	}
	if res != "ok" {
		return gs.ErrJobNotFound
	}
	return nil
}

func (s *Store) Fail(ctx context.Context, id gs.JobID, errMsg string, nextAttemptAt time.Time) error {
	// Look up queue + priority so we can target the right pending ZSET.
	fields, err := s.rdb.HMGet(ctx, jobKey(id), "queue", "priority").Result()
	if err != nil {
		return fmt.Errorf("redis fail lookup: %w", err)
	}
	if fields[0] == nil {
		return gs.ErrJobNotFound
	}
	queue, _ := fields[0].(string)
	priorityStr, _ := fields[1].(string)
	priority := gs.Priority(asInt(priorityStr))

	res, err := failScript.Run(ctx, s.rdb,
		[]string{jobKey(id), claimedKey, pendingKey(queue, priority)},
		string(id), errMsg, formatTime(nextAttemptAt), formatTime(time.Now()),
		strconv.Itoa(int(gs.StatePending)), strconv.Itoa(int(gs.StateClaimed)),
	).Result()
	if err != nil {
		return fmt.Errorf("redis fail: %w", err)
	}
	if res != "ok" {
		return gs.ErrJobNotFound
	}
	return nil
}

func (s *Store) Cancel(ctx context.Context, id gs.JobID) error {
	fields, err := s.rdb.HMGet(ctx, jobKey(id), "queue", "priority").Result()
	if err != nil {
		return fmt.Errorf("redis cancel lookup: %w", err)
	}
	if fields[0] == nil {
		return gs.ErrJobNotFound
	}
	queue, _ := fields[0].(string)
	priorityStr, _ := fields[1].(string)
	priority := gs.Priority(asInt(priorityStr))

	res, err := cancelScript.Run(ctx, s.rdb,
		[]string{jobKey(id), pendingKey(queue, priority)},
		string(id), strconv.Itoa(int(gs.StatePending)),
	).Result()
	if err != nil {
		return fmt.Errorf("redis cancel: %w", err)
	}
	switch res {
	case "ok":
		return nil
	case "missing":
		return gs.ErrJobNotFound
	case "not_pending":
		return gs.ErrJobNotPending
	default:
		return fmt.Errorf("redis cancel: unexpected script result %v", res)
	}
}

func (s *Store) Heartbeat(ctx context.Context, workerID string, now time.Time) error {
	if err := s.rdb.HSet(ctx, workersKey, workerID, formatTime(now)).Err(); err != nil {
		return fmt.Errorf("redis heartbeat: %w", err)
	}
	return nil
}

func (s *Store) RecoverStale(ctx context.Context, _ time.Time) (int, error) {
	res, err := recoverStaleScript.Run(ctx, s.rdb,
		[]string{claimedKey},
		keyPrefix+"job:", keyPrefix+"pending:", strconv.Itoa(int(gs.StatePending)),
	).Result()
	if err != nil {
		return 0, fmt.Errorf("redis recover stale: %w", err)
	}
	n, ok := res.(int64)
	if !ok {
		return 0, fmt.Errorf("redis recover stale: unexpected result type %T", res)
	}
	return int(n), nil
}

func (s *Store) QueueSize(ctx context.Context, queue string) (int, error) {
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, 0, 4)
	for _, p := range []gs.Priority{gs.PriorityLow, gs.PriorityNormal, gs.PriorityHigh, gs.PriorityCritical} {
		cmds = append(cmds, pipe.ZCard(ctx, pendingKey(queue, p)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("redis queue size: %w", err)
	}
	var total int64
	for _, c := range cmds {
		total += c.Val()
	}
	return int(total), nil
}

func (s *Store) Reschedule(ctx context.Context, id gs.JobID, newTime time.Time) error {
	fields, err := s.rdb.HMGet(ctx, jobKey(id), "queue", "priority").Result()
	if err != nil {
		return fmt.Errorf("redis reschedule lookup: %w", err)
	}
	if fields[0] == nil {
		return gs.ErrJobNotFound
	}
	queue, _ := fields[0].(string)
	priorityStr, _ := fields[1].(string)
	priority := gs.Priority(asInt(priorityStr))

	res, err := rescheduleScript.Run(ctx, s.rdb,
		[]string{jobKey(id), pendingKey(queue, priority)},
		string(id), formatTime(newTime), formatTime(time.Now()),
		strconv.Itoa(int(gs.StatePending)),
	).Result()
	if err != nil {
		return fmt.Errorf("redis reschedule: %w", err)
	}
	switch res {
	case "ok":
		return nil
	case "missing":
		return gs.ErrJobNotFound
	case "not_pending":
		return gs.ErrJobNotPending
	default:
		return fmt.Errorf("redis reschedule: unexpected script result %v", res)
	}
}
