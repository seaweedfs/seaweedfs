package messages

import (
	"testing"
	"time"
)

func TestAddMessage(t *testing.T) {
	mp := NewMessagePipeline(0, 3, 10, time.Second, &EmptyMover{})
	go func() {
		outChan := mp.OutputChan()
		for mr := range outChan {
			println(mr.sequence, mr.fileId)
		}
	}()

	for i := 0; i < 100; i++ {
		message := &Message{
			Key:        []byte("key"),
			Content:    []byte("data"),
			Properties: nil,
			Ts:         time.Now(),
		}
		mp.AddMessage(message)
	}

	mp.ShutdownStart()
	mp.ShutdownWait()
}
