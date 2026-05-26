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
	"strings"

	"github.com/redis/go-redis/v9"
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
