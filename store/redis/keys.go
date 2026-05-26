package redis

import (
	"strconv"
	"time"

	gs "github.com/squall-chua/go-schedule-job"
)

// keyPrefix wraps every goschedule key in the "{goschedule}" hash tag so
// Redis Cluster colocates them on a single slot. On standalone Redis the
// braces are inert.
const keyPrefix = "{goschedule}:"

func jobKey(id gs.JobID) string { return keyPrefix + "job:" + string(id) }
func pendingKey(queue string, p gs.Priority) string {
	return keyPrefix + "pending:" + queue + ":p" + strconv.Itoa(int(p))
}

const claimedKey = keyPrefix + "claimed"

func recurringKey(id gs.JobID) string { return keyPrefix + "recurring:" + string(id) }

const recurringAllKey = keyPrefix + "recurring:all"

func leaseKey(id gs.JobID) string { return keyPrefix + "lease:" + string(id) }

const workersKey = keyPrefix + "workers"

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "0"
	}
	return strconv.FormatInt(t.UnixNano(), 10)
}

func parseTime(s string) time.Time {
	n, _ := strconv.ParseInt(s, 10, 64)
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// serializeJob returns the field/value pairs HSET expects.
func serializeJob(j gs.Job) map[string]any {
	return map[string]any{
		"id":           string(j.ID),
		"queue":        j.Queue,
		"name":         j.Name,
		"payload":      string(j.Payload),
		"codec_name":   j.CodecName,
		"priority":     strconv.Itoa(int(j.Priority)),
		"run_at":       formatTime(j.RunAt),
		"attempt":      strconv.Itoa(j.Attempt),
		"max_attempts": strconv.Itoa(j.MaxAttempts),
		"state":        strconv.Itoa(int(j.State)),
		"timeout_ns":   strconv.FormatInt(int64(j.Timeout), 10),
		"locked_by":    j.LockedBy,
		"locked_until": formatTime(j.LockedUntil),
		"last_error":   j.LastError,
		"recurring_id": string(j.RecurringID),
		"created_at":   formatTime(j.CreatedAt),
		"updated_at":   formatTime(j.UpdatedAt),
	}
}

func deserializeJob(m map[string]string) gs.Job {
	priority, _ := strconv.Atoi(m["priority"])
	attempt, _ := strconv.Atoi(m["attempt"])
	maxAttempts, _ := strconv.Atoi(m["max_attempts"])
	state, _ := strconv.Atoi(m["state"])
	timeoutNs, _ := strconv.ParseInt(m["timeout_ns"], 10, 64)
	return gs.Job{
		ID:          gs.JobID(m["id"]),
		Queue:       m["queue"],
		Name:        m["name"],
		Payload:     []byte(m["payload"]),
		CodecName:   m["codec_name"],
		Priority:    gs.Priority(priority),
		RunAt:       parseTime(m["run_at"]),
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		State:       gs.State(state),
		Timeout:     time.Duration(timeoutNs),
		LockedBy:    m["locked_by"],
		LockedUntil: parseTime(m["locked_until"]),
		LastError:   m["last_error"],
		RecurringID: gs.JobID(m["recurring_id"]),
		CreatedAt:   parseTime(m["created_at"]),
		UpdatedAt:   parseTime(m["updated_at"]),
	}
}
