// Package memstore provides an in-memory goschedule.Store implementation.
//
// It is single-process and not durable — process crashes lose pending jobs.
// Use it for single-node deployments without persistence needs, for tests, and
// as the default when no other Store is configured.
package memstore
