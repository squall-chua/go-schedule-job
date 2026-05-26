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
	"github.com/redis/go-redis/v9"
)

// Store implements goschedule.Store backed by Redis.
type Store struct {
	rdb redis.UniversalClient
}
