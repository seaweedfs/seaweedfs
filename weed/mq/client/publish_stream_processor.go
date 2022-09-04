package client

import (
	"context"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/seaweedfs/seaweedfs/weed/mq/segment"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	batchCountLimit = 3
)

type PublishStreamProcessor struct {
	// attributes
	ProducerId     int32
	ProducerEpoch  int32
	grpcDialOption grpc.DialOption

	// input
	sync.Mutex

	timeout time.Duration

	// convert into bytes
	messagesChan           chan *Message
	builders               chan *flatbuffers.Builder
	batchMessageCountLimit int

	messagesSequence int64

	// done channel
	doneChan chan struct{}
}

type UploadProcess struct {
	bufferBuilder *flatbuffers.Builder
	batchBuilder  *segment.MessageBatchBuilder
}

func NewPublishStreamProcessor(batchMessageCountLimit int, timeout time.Duration) *PublishStreamProcessor {
	t := &PublishStreamProcessor{
		grpcDialOption:         grpc.WithTransportCredentials(insecure.NewCredentials()),
		batchMessageCountLimit: batchMessageCountLimit,
		builders:               make(chan *flatbuffers.Builder, batchCountLimit),
		messagesChan:           make(chan *Message, 1024),
		doneChan:               make(chan struct{}),
		timeout:                timeout,
	}
	for i := 0; i < batchCountLimit; i++ {
		t.builders <- flatbuffers.NewBuilder(4 * 1024 * 1024)
	}
	go t.doLoopUpload()
	return t
}

func (p *PublishStreamProcessor) AddMessage(m *Message) error {
	p.messagesChan <- m
	return nil
}

func (p *PublishStreamProcessor) Shutdown() error {
	p.doneChan <- struct{}{}
	return nil
}

func (p *PublishStreamProcessor) doFlush(stream mq_pb.SeaweedMessaging_PublishMessageClient, messages []*Message) error {

	if len(messages) == 0 {
		return nil
	}

	builder := <-p.builders
	bb := segment.NewMessageBatchBuilder(builder, p.ProducerId, p.ProducerEpoch, 3, 4)
	for _, m := range messages {
		bb.AddMessage(p.messagesSequence, m.Ts.UnixNano(), m.Properties, m.Key, m.Content)
		p.messagesSequence++
	}
	bb.BuildMessageBatch()
	defer func() {
		p.builders <- builder
	}()

	return stream.Send(&mq_pb.PublishRequest{
		Data: &mq_pb.PublishRequest_DataMessage{
			Message: bb.GetBytes(),
		},
	})

}

func (p *PublishStreamProcessor) doLoopUpload() {

	brokerGrpcAddress := "localhost:17777"

	// TOOD parallelize the uploading with separate uploader
	messages := make([]*Message, 0, p.batchMessageCountLimit)

	util.RetryForever("publish message", func() error {
		return pb.WithBrokerGrpcClient(false, brokerGrpcAddress, p.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stream, err := client.PublishMessage(ctx)
			if err != nil {
				log.Printf("grpc PublishMessage: %v", err)
				return err
			}

			var atomicStatus int64
			go func() {
				resp, err := stream.Recv()
				if err != nil {
					log.Printf("response error: %v", err)
				} else {
					log.Printf("response: %v", resp.AckSequence)
				}
				if atomic.LoadInt64(&atomicStatus) < 0 {
					return
				}
			}()

			var flushErr error
			// retry previously failed messages
			if len(messages) >= p.batchMessageCountLimit {
				flushErr = p.doFlush(stream, messages)
				if flushErr != nil {
					return flushErr
				}
				messages = messages[:0]
			}

			for {
				select {
				case m := <-p.messagesChan:
					messages = append(messages, m)
					if len(messages) >= p.batchMessageCountLimit {
						if flushErr = p.doFlush(stream, messages); flushErr != nil {
							return flushErr
						}
						messages = messages[:0]
					}
				case <-time.After(p.timeout):
					if flushErr = p.doFlush(stream, messages); flushErr != nil {
						return flushErr
					}
					messages = messages[:0]
				case <-p.doneChan:
					if flushErr = p.doFlush(stream, messages); flushErr != nil {
						return flushErr
					}
					messages = messages[:0]
					println("$ stopping ...")
					break
				}
			}

			// stop the response consuming goroutine
			atomic.StoreInt64(&atomicStatus, -1)

			return flushErr

		})
	}, func(err error) (shouldContinue bool) {
		log.Printf("failed with grpc %s: %v", brokerGrpcAddress, err)
		return true
	})

}
