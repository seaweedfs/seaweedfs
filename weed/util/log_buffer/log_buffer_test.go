package log_buffer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func TestNewLogBufferFirstBuffer(t *testing.T) {
	lb := NewLogBuffer("test", time.Minute, func(startTime, stopTime time.Time, buf []byte) {

	}, func() {

	})

	startTime := time.Now()

	messageSize := 1024
	messageCount := 5000
	var buf = make([]byte, messageSize)
	for i := 0; i < messageCount; i++ {
		rand.Read(buf)
		lb.AddToBuffer(nil, buf, 0)
	}

	receivedmessageCount := 0
	lb.LoopProcessLogData("test", startTime, func() bool {
		// stop if no more messages
		return false
	}, func(logEntry *filer_pb.LogEntry) error {
		receivedmessageCount++
		return nil
	})

	if receivedmessageCount != messageCount {
		fmt.Printf("sent %d received %d\n", messageCount, receivedmessageCount)
	}

}
