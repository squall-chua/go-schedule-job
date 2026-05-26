package json_test

import (
	"context"
	"testing"
	"time"

	gs "github.com/squall-chua/go-schedule-job"
	gsjson "github.com/squall-chua/go-schedule-job/codec/json"
	"github.com/squall-chua/go-schedule-job/memstore"
)

type Greeting struct {
	To  string `json:"to"`
	Msg string `json:"msg"`
}

func TestJSONCodec_RoundTrip(t *testing.T) {
	codec := gsjson.New()
	data, err := codec.Encode(Greeting{To: "world", Msg: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	var out Greeting
	if err := codec.Decode(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.To != "world" || out.Msg != "hi" {
		t.Errorf("round trip mismatch: %+v", out)
	}
}

func TestJSONCodec_TypedHandler(t *testing.T) {
	s, _ := gs.NewScheduler(gs.WithStore(memstore.New()), gs.WithPollInterval(10*time.Millisecond))
	got := make(chan Greeting, 1)
	gs.RegisterTyped[Greeting](s, "greet", gsjson.New(), func(_ context.Context, g Greeting) error {
		got <- g
		return nil
	})
	if _, err := gs.NowTyped[Greeting](s, "greet", gsjson.New(), Greeting{To: "x", Msg: "y"}); err != nil {
		t.Fatal(err)
	}
	// We don't Start the scheduler here — just verify dispatch succeeded.
}
