package log_buffer

import (
	"math/rand"
	"testing"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func TestNewLogBuffer(t *testing.T) {
	lb := NewLogBuffer(time.Second, func(startTime, stopTime time.Time, buf []byte) {

	}, func() {

	})

	startTime := time.Now()

	messageSize := 1024
	messageCount := 994
	var buf = make([]byte, messageSize)
	for i := 0; i < messageCount; i++ {
		rand.Read(buf)
		lb.AddToBuffer(nil, buf)
	}

	receivedmessageCount := 0
	lb.LoopProcessLogData(startTime, func() bool {
		// stop if no more messages
		return false
	}, func(logEntry *filer_pb.LogEntry) error {
		receivedmessageCount++
		return nil
	})

	if receivedmessageCount != messageCount {
		t.Errorf("sent %d received %d", messageCount, receivedmessageCount)
	}

}
