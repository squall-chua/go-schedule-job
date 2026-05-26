package proto_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	gs "github.com/squall-chua/go-schedule-job"
	protocodec "github.com/squall-chua/go-schedule-job/codec/proto"
	"github.com/squall-chua/go-schedule-job/memstore"
)

func TestProtoCodec_RoundTrip(t *testing.T) {
	codec := protocodec.New()

	data, err := codec.Encode(wrapperspb.String("hello"))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var out *wrapperspb.StringValue
	if err := codec.Decode(data, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out == nil || out.GetValue() != "hello" {
		t.Fatalf("round trip mismatch: got %+v, want value=hello", out)
	}
}

func TestProtoCodec_DecodeIntoConcreteMessage(t *testing.T) {
	codec := protocodec.New()

	data, err := codec.Encode(wrapperspb.Int64(42))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	out := &wrapperspb.Int64Value{}
	if err := codec.Decode(data, out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.GetValue() != 42 {
		t.Fatalf("got %d, want 42", out.GetValue())
	}
}

func TestProtoCodec_EncodeNonMessage(t *testing.T) {
	_, err := protocodec.New().Encode(struct{ X int }{X: 1})
	if !errors.Is(err, protocodec.ErrNotProtoMessage) {
		t.Fatalf("err = %v, want ErrNotProtoMessage", err)
	}
}

func TestProtoCodec_DecodeNonPointer(t *testing.T) {
	err := protocodec.New().Decode(nil, 42)
	if !errors.Is(err, protocodec.ErrNotProtoMessage) {
		t.Fatalf("err = %v, want ErrNotProtoMessage", err)
	}
}

func TestProtoCodec_TypedHandler_EndToEnd(t *testing.T) {
	store := memstore.New()
	sched, err := gs.NewScheduler(
		gs.WithStore(store),
		gs.WithPollInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	codec := protocodec.New()
	var got atomic.Pointer[string]

	gs.RegisterTyped[*wrapperspb.StringValue](sched, "echo", codec,
		func(_ context.Context, m *wrapperspb.StringValue) error {
			v := m.GetValue()
			got.Store(&v)
			return nil
		},
	)

	if _, err := gs.NowTyped(sched, "echo", codec, wrapperspb.String("ping")); err != nil {
		t.Fatalf("NowTyped: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = sched.Start(ctx); close(done) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got.Load() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if p := got.Load(); p == nil || *p != "ping" {
		t.Fatalf("handler got %v, want 'ping'", p)
	}
}
