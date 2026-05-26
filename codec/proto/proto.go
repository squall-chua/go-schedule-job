// Package proto provides a Protocol Buffers Codec for goschedule.
//
// Payload types must be generated protobuf messages — typically pointer types
// such as *mypb.Foo, since generated messages implement proto.Message via a
// pointer receiver.
//
// Usage with the typed dispatch helpers:
//
//	import (
//	    gs "github.com/squall-chua/go-schedule-job"
//	    protocodec "github.com/squall-chua/go-schedule-job/codec/proto"
//	    "google.golang.org/protobuf/types/known/wrapperspb"
//	)
//
//	codec := protocodec.New()
//	gs.RegisterTyped[*wrapperspb.StringValue](sched, "echo", codec,
//	    func(ctx context.Context, m *wrapperspb.StringValue) error {
//	        log.Println(m.GetValue())
//	        return nil
//	    },
//	)
//	gs.NowTyped(sched, "echo", codec, wrapperspb.String("hi"))
package proto

import (
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/proto"

	gs "github.com/squall-chua/go-schedule-job"
)

// ErrNotProtoMessage is returned when a value cannot be coerced to proto.Message.
var ErrNotProtoMessage = errors.New("goschedule/codec/proto: value does not implement proto.Message")

// Codec is a goschedule.Codec backed by google.golang.org/protobuf/proto.
type Codec struct{}

// New returns the default Protocol Buffers codec.
func New() gs.Codec { return Codec{} }

func (Codec) Name() string { return "proto" }

// Encode marshals v to wire-format bytes. v must implement proto.Message.
func (Codec) Encode(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", ErrNotProtoMessage, v)
	}
	return proto.Marshal(m)
}

// Decode unmarshals data into v. v may be either a proto.Message directly, or
// a pointer to a proto.Message variable (e.g. **mypb.Foo) — the latter shape
// is what gs.RegisterTyped[*mypb.Foo] passes. In the pointer-to-pointer case
// Decode allocates the underlying message if it is nil.
func (Codec) Decode(data []byte, v any) error {
	if m, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, m)
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("%w: got %T", ErrNotProtoMessage, v)
	}
	inner := rv.Elem()
	if inner.Kind() != reflect.Pointer {
		return fmt.Errorf("%w: got %T", ErrNotProtoMessage, v)
	}
	if inner.IsNil() {
		inner.Set(reflect.New(inner.Type().Elem()))
	}
	m, ok := inner.Interface().(proto.Message)
	if !ok {
		return fmt.Errorf("%w: got %T", ErrNotProtoMessage, v)
	}
	return proto.Unmarshal(data, m)
}
