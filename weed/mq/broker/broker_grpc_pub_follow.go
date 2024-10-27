package broker

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/buffered_queue"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"io"
	"time"
)

type memBuffer struct {
	buf       []byte
	startTime time.Time
	stopTime  time.Time
}

func (b *MessageQueueBroker) PublishFollowMe(stream mq_pb.SeaweedMessaging_PublishFollowMeServer) (err error) {
	var req *mq_pb.PublishFollowMeRequest
	req, err = stream.Recv()
	if err != nil {
		return err
	}
	initMessage := req.GetInit()
	if initMessage == nil {
		return fmt.Errorf("missing init message")
	}

	// create an in-memory queue of buffered messages
	inMemoryBuffers := buffered_queue.NewBufferedQueue[memBuffer](4)
	logBuffer := b.buildFollowerLogBuffer(inMemoryBuffers)

	lastFlushTsNs := time.Now().UnixNano()

	// follow each published messages
	for {
		// receive a message
		req, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			glog.V(0).Infof("topic %v partition %v publish stream error: %v", initMessage.Topic, initMessage.Partition, err)
			break
		}

		// Process the received message
		if dataMessage := req.GetData(); dataMessage != nil {

			// TODO: change this to DataMessage
			// log the message
			logBuffer.AddToBuffer(dataMessage)

			// send back the ack
			if err := stream.Send(&mq_pb.PublishFollowMeResponse{
				AckTsNs: dataMessage.TsNs,
			}); err != nil {
				glog.Errorf("Error sending response %v: %v", dataMessage, err)
			}
			// println("ack", string(dataMessage.Key), dataMessage.TsNs)
		} else if closeMessage := req.GetClose(); closeMessage != nil {
			glog.V(0).Infof("topic %v partition %v publish stream closed: %v", initMessage.Topic, initMessage.Partition, closeMessage)
			break
		} else if flushMessage := req.GetFlush(); flushMessage != nil {
			glog.V(0).Infof("topic %v partition %v publish stream flushed: %v", initMessage.Topic, initMessage.Partition, flushMessage)

			lastFlushTsNs = flushMessage.TsNs

			// drop already flushed messages
			for mem, found := inMemoryBuffers.PeekHead(); found; mem, found = inMemoryBuffers.PeekHead() {
				if mem.stopTime.UnixNano() <= flushMessage.TsNs {
					inMemoryBuffers.Dequeue()
					// println("dropping flushed messages: ", mem.startTime.UnixNano(), mem.stopTime.UnixNano(), len(mem.buf))
				} else {
					break
				}
			}

		} else {
			glog.Errorf("unknown message: %v", req)
		}
	}

	t, p := topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.Partition)

	logBuffer.ShutdownLogBuffer()
	// wait until all messages are sent to inMemoryBuffers
	for !logBuffer.IsAllFlushed() {
		time.Sleep(113 * time.Millisecond)
	}

	partitionDir := topic.PartitionDir(t, p)

	// flush the remaining messages
	inMemoryBuffers.CloseInput()
	for mem, found := inMemoryBuffers.Dequeue(); found; mem, found = inMemoryBuffers.Dequeue() {
		if len(mem.buf) == 0 {
			continue
		}

		startTime, stopTime := mem.startTime.UTC(), mem.stopTime.UTC()

		if stopTime.UnixNano() <= lastFlushTsNs {
			glog.V(0).Infof("dropping remaining data at %v %v", t, p)
			continue
		}

		// TODO trim data earlier than lastFlushTsNs

		targetFile := fmt.Sprintf("%s/%s", partitionDir, startTime.Format(topic.TIME_FORMAT))

		for {
			if err := b.appendToFile(targetFile, mem.buf); err != nil {
				glog.V(0).Infof("metadata log write failed %s: %v", targetFile, err)
				time.Sleep(737 * time.Millisecond)
			} else {
				break
			}
		}

		glog.V(0).Infof("flushed remaining data at %v to %s size %d", mem.stopTime.UnixNano(), targetFile, len(mem.buf))
	}

	glog.V(0).Infof("shut down follower for %v %v", t, p)

	return err
}

func (b *MessageQueueBroker) buildFollowerLogBuffer(inMemoryBuffers *buffered_queue.BufferedQueue[memBuffer]) *log_buffer.LogBuffer {
	lb := log_buffer.NewLogBuffer("follower",
		2*time.Minute, func(logBuffer *log_buffer.LogBuffer, startTime, stopTime time.Time, buf []byte) {
			if len(buf) == 0 {
				return
			}
			inMemoryBuffers.Enqueue(memBuffer{
				buf:       buf,
				startTime: startTime,
				stopTime:  stopTime,
			})
			glog.V(0).Infof("queue up %d~%d size %d", startTime.UnixNano(), stopTime.UnixNano(), len(buf))
		}, nil, func() {
		})
	return lb
}
