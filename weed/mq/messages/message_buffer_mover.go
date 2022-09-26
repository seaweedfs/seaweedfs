package messages

import "fmt"

type MessageBufferMover interface {
	Setup()
	TearDown()
	MoveBuffer(buffer *MessageBuffer) (MessageBufferReference, error) // should be thread-safe
}
type MessageBufferReference struct {
	sequence int64
	fileId   string
}

var _ = MessageBufferMover(&EmptyMover{})

type EmptyMover struct {
}

func (e *EmptyMover) Setup() {
}

func (e *EmptyMover) TearDown() {
}

func (e *EmptyMover) MoveBuffer(buffer *MessageBuffer) (MessageBufferReference, error) {
	println("moving", buffer.sequenceBase)
	return MessageBufferReference{
		sequence: buffer.sequenceBase,
		fileId:   fmt.Sprintf("buffer %d", buffer.sequenceBase),
	}, nil
}
