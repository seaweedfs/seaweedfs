package filer

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

func TestNotifyRenameTargetWakesDestinationSubscribers(t *testing.T) {
	logBuffer := log_buffer.NewLogBuffer("test", time.Minute, func(*log_buffer.LogBuffer, time.Time, time.Time, []byte, int64, int64) {}, nil, nil)
	defer logBuffer.ShutdownLogBuffer()

	logBuffer.SetSubscriberWakeFilter("dst", func(key []byte) bool { return bytes.Equal(key, []byte("/dst")) })
	dst := logBuffer.RegisterSubscriber("dst")
	defer logBuffer.UnregisterSubscriber("dst")

	woken := func() bool {
		select {
		case <-dst:
			return true
		case <-time.After(50 * time.Millisecond):
			return false
		}
	}

	notifyRenameTarget(logBuffer, &filer_pb.SubscribeMetadataResponse{Directory: "/src",
		EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "a"}, NewParentPath: "/src"}})
	if woken() {
		t.Fatal("an event that stays in its directory must not wake other directories")
	}

	notifyRenameTarget(logBuffer, &filer_pb.SubscribeMetadataResponse{Directory: "/src",
		EventNotification: &filer_pb.EventNotification{OldEntry: &filer_pb.Entry{Name: "a"}, NewEntry: &filer_pb.Entry{Name: "a"}, NewParentPath: "/dst"}})
	if !woken() {
		t.Fatal("a move must wake the destination directory's subscribers")
	}
}
