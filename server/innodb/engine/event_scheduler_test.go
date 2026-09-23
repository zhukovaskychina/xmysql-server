package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestEventSchedulerIsDisabledUntilExplicitlyEnabled(t *testing.T) {
	scheduler := NewEventScheduler(false)
	if scheduler.Start(context.Background()) {
		t.Fatal("disabled event scheduler must not start")
	}
	scheduler.SetEnabled(true)
	if !scheduler.Start(context.Background()) || !scheduler.Running() {
		t.Fatal("enabled event scheduler did not start")
	}
	if !scheduler.Stop() || scheduler.Running() {
		t.Fatal("event scheduler did not stop")
	}
}

func TestEventSchedulerExecutesOneTimeEvent(t *testing.T) {
	scheduler := NewEventScheduler(true)
	var calls atomic.Int32
	requireNoEventError(t, scheduler.AddEvent(ScheduledEvent{
		Name:      "one_time",
		ExecuteAt: time.Now().Add(20 * time.Millisecond),
		Execute: func(context.Context) error {
			calls.Add(1)
			return nil
		},
	}))
	if !scheduler.Start(context.Background()) {
		t.Fatal("enabled event scheduler did not start")
	}
	defer scheduler.Stop()
	deadline := time.Now().Add(time.Second)
	for calls.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() != 1 {
		t.Fatalf("one-time event calls = %d, want 1", calls.Load())
	}
	time.Sleep(30 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("one-time event executed more than once: %d", calls.Load())
	}
}

func TestEventSchedulerRepeatsAndCapturesErrors(t *testing.T) {
	scheduler := NewEventScheduler(true)
	var calls atomic.Int32
	requireNoEventError(t, scheduler.AddEvent(ScheduledEvent{
		Name:     "repeating",
		Interval: 15 * time.Millisecond,
		Repeat:   true,
		Execute: func(context.Context) error {
			calls.Add(1)
			return errors.New("event failed")
		},
	}))
	if !scheduler.Start(context.Background()) {
		t.Fatal("enabled event scheduler did not start")
	}
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatalf("repeating event calls = %d, want at least 2", calls.Load())
	}
	if scheduler.LastError("repeating") != "event failed" {
		t.Fatalf("unexpected event error: %q", scheduler.LastError("repeating"))
	}
	if !scheduler.RemoveEvent("repeating") {
		t.Fatal("repeating event was not removed")
	}
	if !scheduler.Stop() {
		t.Fatal("event scheduler did not stop")
	}
}

func requireNoEventError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
