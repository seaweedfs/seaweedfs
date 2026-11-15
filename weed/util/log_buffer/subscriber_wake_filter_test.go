package log_buffer

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

func woken(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	case <-time.After(50 * time.Millisecond):
		return false
	}
}

func TestSubscriberWakeFilter(t *testing.T) {
	flushFn := func(*LogBuffer, time.Time, time.Time, []byte, int64, int64) {}
	logBuffer := NewLogBuffer("test", time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	wantsA := func(key []byte) bool { return bytes.Equal(key, []byte("a")) }
	// set before registration, the way a reader is set up ahead of its loop
	logBuffer.SetSubscriberWakeFilter("filtered", wantsA)
	filtered := logBuffer.RegisterSubscriber("filtered")
	defer logBuffer.UnregisterSubscriber("filtered")
	all := logBuffer.RegisterSubscriber("all")
	defer logBuffer.UnregisterSubscriber("all")
	late := logBuffer.RegisterSubscriber("late")
	defer logBuffer.UnregisterSubscriber("late")
	logBuffer.SetSubscriberWakeFilter("late", wantsA)

	if err := logBuffer.AddDataToBuffer([]byte("b"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if woken(filtered) || woken(late) {
		t.Fatal("an append under another key must not wake filtered subscribers")
	}
	if !woken(all) {
		t.Fatal("an unfiltered subscriber must wake on every append")
	}

	if err := logBuffer.AddDataToBuffer([]byte("a"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if !woken(filtered) || !woken(late) || !woken(all) {
		t.Fatal("an append under a wanted key must wake every subscriber")
	}

	logBuffer.NotifySubscribers([]byte("a"))
	if !woken(filtered) {
		t.Fatal("NotifySubscribers must wake subscribers that want the key")
	}

	logBuffer.ForceFlush()
	if !woken(filtered) || !woken(late) {
		t.Fatal("a flush must wake filtered subscribers")
	}

	logBuffer.SetSubscriberWakeFilter("late", nil)
	if err := logBuffer.AddDataToBuffer([]byte("b"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if !woken(late) {
		t.Fatal("clearing the filter must restore wake-ups")
	}
}

func TestSubscriberWakeFilterStillDelivers(t *testing.T) {
	flushFn := func(*LogBuffer, time.Time, time.Time, []byte, int64, int64) {}
	logBuffer := NewLogBuffer("test", time.Minute, flushFn, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	logBuffer.SetSubscriberWakeFilter("reader", func([]byte) bool { return false })
	start := NewMessagePosition(time.Now().UnixNano(), -2)
	got := make(chan []byte, 1)
	go logBuffer.LoopProcessLogData("reader", start, 0, func() bool { return true }, func(logEntry *filer_pb.LogEntry) (bool, error) {
		got <- bytes.Clone(logEntry.Data)
		return true, nil
	})

	time.Sleep(50 * time.Millisecond)
	if err := logBuffer.AddDataToBuffer([]byte("unwanted"), []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-got:
		if string(data) != "v" {
			t.Fatalf("got %q", data)
		}
	case <-time.After(2 * notificationHealthCheckInterval):
		t.Fatal("a reader that sleeps through an append must still read it on its next poll")
	}
}
