package messages

import (
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type OnMessageFunc func(message *Message)

type MessagePipeline struct {
	// atomic status
	atomicPipelineStatus int64 // -1: stop

	// attributes
	ProducerId     int32
	ProducerEpoch  int32
	grpcDialOption grpc.DialOption

	emptyBuffersChan  chan *MessageBuffer
	sealedBuffersChan chan *MessageBuffer
	movedBuffersChan  chan MessageBufferReference
	onMessageFn       OnMessageFunc
	mover             MessageBufferMover
	moverPool         *util.LimitedAsyncExecutor

	// control pipeline
	doneChan  chan struct{}
	batchSize int
	timeout   time.Duration

	incomingMessageLock   sync.Mutex
	incomingMessageBuffer *MessageBuffer

	messageSequence int64
}

func NewMessagePipeline(producerId int32, workerCount int, batchSize int, timeout time.Duration, mover MessageBufferMover) *MessagePipeline {
	t := &MessagePipeline{
		ProducerId:        producerId,
		emptyBuffersChan:  make(chan *MessageBuffer, workerCount),
		sealedBuffersChan: make(chan *MessageBuffer, workerCount),
		movedBuffersChan:  make(chan MessageBufferReference, workerCount),
		doneChan:          make(chan struct{}),
		batchSize:         batchSize,
		timeout:           timeout,
		moverPool:         util.NewLimitedAsyncExecutor(workerCount),
		mover:             mover,
	}
	go t.doLoopUpload()
	return t
}

func (mp *MessagePipeline) NextMessageBufferReference() MessageBufferReference {
	return mp.moverPool.NextFuture().Await().(MessageBufferReference)
}

func (mp *MessagePipeline) AddMessage(message *Message) {
	mp.incomingMessageLock.Lock()
	defer mp.incomingMessageLock.Unlock()

	// get existing message buffer or create a new one
	if mp.incomingMessageBuffer == nil {
		select {
		case mp.incomingMessageBuffer = <-mp.emptyBuffersChan:
		default:
			mp.incomingMessageBuffer = NewMessageBuffer()
		}
		mp.incomingMessageBuffer.Reset(mp.messageSequence)
	}

	// add one message
	mp.incomingMessageBuffer.AddMessage(message)
	mp.messageSequence++

	// seal the message buffer if full
	if mp.incomingMessageBuffer.Len() >= mp.batchSize {
		mp.incomingMessageBuffer.Seal(mp.ProducerId, mp.ProducerEpoch, 0, 0)
		mp.sealedBuffersChan <- mp.incomingMessageBuffer
		mp.incomingMessageBuffer = nil
	}
}

func (mp *MessagePipeline) doLoopUpload() {

	mp.mover.Setup()
	defer mp.mover.TearDown()

	ticker := time.NewTicker(mp.timeout)
	for {
		status := atomic.LoadInt64(&mp.atomicPipelineStatus)
		if status == -100 {
			return
		} else if status == -1 {
			// entering shutting down mode
			atomic.StoreInt64(&mp.atomicPipelineStatus, -2)
			mp.incomingMessageLock.Lock()
			mp.doFlushIncomingMessages()
			mp.incomingMessageLock.Unlock()
		}

		select {
		case messageBuffer := <-mp.sealedBuffersChan:
			ticker.Reset(mp.timeout)
			mp.moverPool.Execute(func() any {
				var output MessageBufferReference
				util.RetryForever("message mover", func() error {
					if messageReference, flushErr := mp.mover.MoveBuffer(messageBuffer); flushErr != nil {
						return flushErr
					} else {
						output = messageReference
					}
					return nil
				}, func(err error) (shouldContinue bool) {
					log.Printf("failed: %v", err)
					return true
				})
				return output
			})
		case <-ticker.C:
			if atomic.LoadInt64(&mp.atomicPipelineStatus) == -2 {
				atomic.StoreInt64(&mp.atomicPipelineStatus, -100)
				return
			}
			mp.incomingMessageLock.Lock()
			mp.doFlushIncomingMessages()
			mp.incomingMessageLock.Unlock()
		}
	}

	atomic.StoreInt64(&mp.atomicPipelineStatus, -100)
	close(mp.movedBuffersChan)

}

func (mp *MessagePipeline) doFlushIncomingMessages() {
	if mp.incomingMessageBuffer == nil || mp.incomingMessageBuffer.Len() == 0 {
		return
	}
	mp.incomingMessageBuffer.Seal(mp.ProducerId, mp.ProducerEpoch, 0, 0)
	mp.sealedBuffersChan <- mp.incomingMessageBuffer
	mp.incomingMessageBuffer = nil
}

func (mp *MessagePipeline) ShutdownStart() {
	if atomic.LoadInt64(&mp.atomicPipelineStatus) == 0 {
		atomic.StoreInt64(&mp.atomicPipelineStatus, -1)
	}
}
func (mp *MessagePipeline) ShutdownWait() {
	for atomic.LoadInt64(&mp.atomicPipelineStatus) != -100 {
		time.Sleep(331 * time.Millisecond)
	}
}

func (mp *MessagePipeline) ShutdownImmediate() {
	if atomic.LoadInt64(&mp.atomicPipelineStatus) == 0 {
		atomic.StoreInt64(&mp.atomicPipelineStatus, -100)
	}
}
