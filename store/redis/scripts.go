package redis

import "github.com/redis/go-redis/v9"

// claimDueScript walks priority buckets (highest priority first) and
// atomically moves jobs from pending into the claimed ZSET.
//
// KEYS:  the pending ZSETs in priority order (highest first), then the
//        claimed ZSET as the final KEY.
// ARGV:  [1] now (unix nanos)
//        [2] n (max jobs to claim)
//        [3] workerID
//        [4] lockUntil (unix nanos)
//        [5] jobKeyPrefix (e.g. "{goschedule}:job:")
//        [6] stateClaimed (e.g. "1")
//
// Returns: list of jobIDs that were claimed (in priority-DESC, run_at-ASC
// order across buckets).
var claimDueScript = redis.NewScript(`
local n = tonumber(ARGV[2])
local worker = ARGV[3]
local lock_until = ARGV[4]
local job_prefix = ARGV[5]
local state_claimed = ARGV[6]
local claimed_key = KEYS[#KEYS]

local result = {}
local remaining = n
for i = 1, #KEYS - 1 do
    if remaining <= 0 then break end
    local zset = KEYS[i]
    local ids = redis.call('ZRANGEBYSCORE', zset, '-inf', ARGV[1], 'LIMIT', 0, remaining)
    for _, id in ipairs(ids) do
        redis.call('ZREM', zset, id)
        redis.call('ZADD', claimed_key, lock_until, id)
        redis.call('HSET', job_prefix .. id,
            'state', state_claimed,
            'locked_by', worker,
            'locked_until', lock_until,
            'updated_at', ARGV[1])
        table.insert(result, id)
        remaining = remaining - 1
    end
end
return result
`)

// failScript checks the job is currently claimed, then either deletes it
// (terminal) or re-enqueues it as pending with attempt+1.
//
// KEYS: [1] job hash, [2] claimed ZSET, [3] pending ZSET (the right bucket)
// ARGV: [1] jobID, [2] errMsg, [3] nextAttemptAt (unix nanos),
//       [4] now (unix nanos), [5] statePending, [6] stateClaimed
//
// Returns: "ok" | "missing" (missing covers both not-found and not-claimed)
var failScript = redis.NewScript(`
local job = KEYS[1]
local claimed = KEYS[2]
local pending = KEYS[3]
local id = ARGV[1]
local err_msg = ARGV[2]
local next_at = ARGV[3]
local now = ARGV[4]
local state_pending = ARGV[5]
local state_claimed = ARGV[6]

local state = redis.call('HGET', job, 'state')
if state ~= state_claimed then
    return 'missing'
end
local attempt = tonumber(redis.call('HGET', job, 'attempt')) or 0
local maxa = tonumber(redis.call('HGET', job, 'max_attempts')) or 0
attempt = attempt + 1
redis.call('ZREM', claimed, id)
if maxa > 0 and attempt >= maxa then
    redis.call('DEL', job)
else
    redis.call('HSET', job,
        'attempt', tostring(attempt),
        'last_error', err_msg,
        'state', state_pending,
        'run_at', next_at,
        'locked_by', '',
        'locked_until', '0',
        'updated_at', now)
    redis.call('ZADD', pending, next_at, id)
end
return 'ok'
`)

// cancelScript: state guard then DELETE+ZREM.
//
// KEYS: [1] job hash, [2] pending ZSET (the right bucket)
// ARGV: [1] jobID, [2] statePending
//
// Returns: "ok" | "missing" | "not_pending"
var cancelScript = redis.NewScript(`
local job = KEYS[1]
local pending = KEYS[2]
local id = ARGV[1]
local state_pending = ARGV[2]

if redis.call('EXISTS', job) == 0 then
    return 'missing'
end
local state = redis.call('HGET', job, 'state')
if state ~= state_pending then
    return 'not_pending'
end
redis.call('ZREM', pending, id)
redis.call('DEL', job)
return 'ok'
`)

// rescheduleScript: state guard then UPDATE run_at + ZADD pending (with new score).
//
// KEYS: [1] job hash, [2] pending ZSET (the right bucket)
// ARGV: [1] jobID, [2] newRunAt (unix nanos), [3] now (unix nanos), [4] statePending
//
// Returns: "ok" | "missing" | "not_pending"
var rescheduleScript = redis.NewScript(`
local job = KEYS[1]
local pending = KEYS[2]
local id = ARGV[1]
local new_run = ARGV[2]
local now = ARGV[3]
local state_pending = ARGV[4]

if redis.call('EXISTS', job) == 0 then
    return 'missing'
end
local state = redis.call('HGET', job, 'state')
if state ~= state_pending then
    return 'not_pending'
end
redis.call('HSET', job, 'run_at', new_run, 'updated_at', now)
redis.call('ZADD', pending, new_run, id)
return 'ok'
`)

// recoverStaleScript scans the claimed ZSET for jobs whose locked_until is
// older than Redis's own clock, and moves them back to pending. Uses
// redis.call('TIME') so the comparison is immune to caller-clock skew.
//
// KEYS:  [1] claimed ZSET
// ARGV:  [1] jobKeyPrefix, [2] pendingKeyPrefix (e.g. "{goschedule}:pending:"),
//        [3] statePending
//
// Returns: integer count of jobs recovered
var recoverStaleScript = redis.NewScript(`
local claimed = KEYS[1]
local job_prefix = ARGV[1]
local pending_prefix = ARGV[2]
local state_pending = ARGV[3]

local t = redis.call('TIME')
local now_nanos = tonumber(t[1]) * 1000000000 + tonumber(t[2]) * 1000

local ids = redis.call('ZRANGEBYSCORE', claimed, 1, now_nanos)
local n = 0
for _, id in ipairs(ids) do
    local job = job_prefix .. id
    local queue = redis.call('HGET', job, 'queue')
    local priority = redis.call('HGET', job, 'priority')
    local run_at = redis.call('HGET', job, 'run_at')
    redis.call('ZREM', claimed, id)
    if queue and priority and run_at then
        local pending_key = pending_prefix .. queue .. ':p' .. priority
        redis.call('HSET', job,
            'state', state_pending,
            'locked_by', '',
            'locked_until', '0',
            'updated_at', tostring(now_nanos))
        redis.call('ZADD', pending_key, run_at, id)
        n = n + 1
    end
end
return n
`)

// acquireLeaseScript implements the "create or refresh" semantics for
// recurring leases. If the key doesn't exist OR is already owned by the
// same worker, the lease is granted (and its TTL refreshed). Otherwise
// the call returns 0. Redis's native TTL means lease expiry happens on
// Redis's clock — no caller-clock dependency.
//
// KEYS: [1] lease key
// ARGV: [1] workerID, [2] ttlSeconds
//
// Returns: 1 (acquired) | 0 (held by another worker)
var acquireLeaseScript = redis.NewScript(`
local lease = KEYS[1]
local worker = ARGV[1]
local ttl = tonumber(ARGV[2])
local current = redis.call('GET', lease)
if current == false or current == worker then
    redis.call('SET', lease, worker, 'EX', ttl)
    return 1
end
return 0
`)
