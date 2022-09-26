package messages

import (
	"testing"
	"time"
)

func TestAddMessage1(t *testing.T) {
	mp := NewMessagePipeline(0, 3, 10, time.Second, &EmptyMover{})
	go func() {
		for i := 0; i < 100; i++ {
			mr := mp.NextMessageBufferReference()
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

func TestAddMessage2(t *testing.T) {
	mp := NewMessagePipeline(0, 3, 10, time.Second, &EmptyMover{})

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

	for i := 0; i < 100; i++ {
		mr := mp.NextMessageBufferReference()
		println(mr.sequence, mr.fileId)
	}

}
