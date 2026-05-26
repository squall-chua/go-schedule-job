package goschedule_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	gs "github.com/squall-chua/go-schedule-job"
	"github.com/squall-chua/go-schedule-job/memstore"
)

type stubCodec struct{}

func (stubCodec) Name() string                    { return "stub" }
func (stubCodec) Encode(v any) ([]byte, error)    { return json.Marshal(v) }
func (stubCodec) Decode(data []byte, v any) error { return json.Unmarshal(data, v) }

type greetPayload struct {
	To  string `json:"to"`
	Msg string `json:"msg"`
}

func TestRegisterTyped_RoundTrips(t *testing.T) {
	s, _ := gs.NewScheduler(gs.WithStore(memstore.New()))
	got := make(chan greetPayload, 1)
	gs.RegisterTyped[greetPayload](s, "greet", stubCodec{}, func(_ context.Context, g greetPayload) error {
		got <- g
		return nil
	})
	if !s.IsRegistered("greet") {
		t.Fatal("expected greet to be registered")
	}
	if _, err := gs.NowTyped[greetPayload](s, "greet", stubCodec{}, greetPayload{To: "world", Msg: "hi"}); err != nil {
		t.Fatal(err)
	}
}

type errCodec struct{}

func (errCodec) Name() string                    { return "err" }
func (errCodec) Encode(_ any) ([]byte, error)    { return nil, errors.New("encode boom") }
func (errCodec) Decode(_ []byte, _ any) error    { return nil }

func TestNowTyped_EncodeErrorPropagates(t *testing.T) {
	s, _ := gs.NewScheduler(gs.WithStore(memstore.New()))
	s.Register("x", func(_ context.Context, _ []byte) error { return nil })
	if _, err := gs.NowTyped[int](s, "x", errCodec{}, 0); err == nil {
		t.Error("expected error from encoder")
	}
}
