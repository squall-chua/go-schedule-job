package goschedule

import (
	"context"
	"fmt"
	"time"
)

// Codec encodes and decodes typed payloads. Implementations include
// goschedule/codec/json. Typed dispatch helpers that use Codec (RegisterTyped,
// NowTyped, AtTyped, AfterTyped, EveryTyped, CronTyped) are added once the
// corresponding Scheduler methods exist.
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
	Name() string
}

// RegisterTyped registers a handler that receives a decoded value of type T.
// The codec used at registration time must match the codec used at dispatch time.
func RegisterTyped[T any](s *Scheduler, name string, codec Codec, h func(context.Context, T) error) {
	s.Register(name, func(ctx context.Context, payload []byte) error {
		var v T
		if err := codec.Decode(payload, &v); err != nil {
			return fmt.Errorf("decode %s: %w", codec.Name(), err)
		}
		return h(ctx, v)
	})
}

// dispatchTyped is the shared internal helper.
func dispatchTyped[T any](codec Codec, payload T, opts []JobOption) ([]JobOption, []byte, error) {
	data, err := codec.Encode(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("encode %s: %w", codec.Name(), err)
	}
	opts = append(opts, withCodec(codec.Name()))
	return opts, data, nil
}

// NowTyped dispatches a typed payload to run immediately.
func NowTyped[T any](s *Scheduler, name string, codec Codec, payload T, opts ...JobOption) (JobID, error) {
	opts, data, err := dispatchTyped(codec, payload, opts)
	if err != nil {
		return "", err
	}
	return s.Now(name, data, opts...)
}

// AtTyped dispatches a typed payload to run at t.
func AtTyped[T any](s *Scheduler, t time.Time, name string, codec Codec, payload T, opts ...JobOption) (JobID, error) {
	opts, data, err := dispatchTyped(codec, payload, opts)
	if err != nil {
		return "", err
	}
	return s.At(t, name, data, opts...)
}

// AfterTyped dispatches a typed payload to run after a delay.
func AfterTyped[T any](s *Scheduler, d time.Duration, name string, codec Codec, payload T, opts ...JobOption) (JobID, error) {
	opts, data, err := dispatchTyped(codec, payload, opts)
	if err != nil {
		return "", err
	}
	return s.After(d, name, data, opts...)
}

// EveryTyped schedules a recurring typed payload at fixed interval d.
func EveryTyped[T any](s *Scheduler, d time.Duration, name string, codec Codec, payload T, opts ...JobOption) (JobID, error) {
	opts, data, err := dispatchTyped(codec, payload, opts)
	if err != nil {
		return "", err
	}
	return s.Every(d, name, data, opts...)
}

// CronTyped schedules a recurring typed payload by cron expression.
func CronTyped[T any](s *Scheduler, expr, name string, codec Codec, payload T, opts ...JobOption) (JobID, error) {
	opts, data, err := dispatchTyped(codec, payload, opts)
	if err != nil {
		return "", err
	}
	return s.Cron(expr, name, data, opts...)
}
