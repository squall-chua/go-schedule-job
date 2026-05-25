package goschedule

import (
	"context"
	"strings"
	"testing"
)

func TestPanicError_IncludesNonStringValue(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("safeInvoke should have caught the panic, got %v", r)
		}
	}()
	err := safeInvoke(context.Background(), func(_ context.Context, _ []byte) error {
		panic(42)
	}, nil)
	if err == nil {
		t.Fatal("expected error from panicking handler")
	}
	if !strings.Contains(err.Error(), "42") {
		t.Errorf("expected panic value 42 in error message, got %q", err.Error())
	}
}

func TestPanicError_IncludesStack(t *testing.T) {
	err := safeInvoke(context.Background(), func(_ context.Context, _ []byte) error {
		panic("kaboom")
	}, nil)
	if err == nil {
		t.Fatal("expected error from panicking handler")
	}
	if !strings.Contains(err.Error(), "kaboom") {
		t.Errorf("expected 'kaboom' in error: %q", err.Error())
	}
	// Stack trace should be present — look for a goroutine marker.
	if !strings.Contains(err.Error(), "goroutine") {
		t.Errorf("expected stack trace in error: %q", err.Error())
	}
}
