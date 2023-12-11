package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/peer"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

// PUB
// 1. gRPC API to configure a topic
//    1.1 create a topic with existing partition count
//    1.2 assign partitions to brokers
// 2. gRPC API to lookup topic partitions
// 3. gRPC API to publish by topic partitions

// SUB
// 1. gRPC API to lookup a topic partitions

// Re-balance topic partitions for publishing
//   1. collect stats from all the brokers
//   2. Rebalance and configure new generation of partitions on brokers
//   3. Tell brokers to close current gneration of publishing.
// Publishers needs to lookup again and publish to the new generation of partitions.

// Re-balance topic partitions for subscribing
//   1. collect stats from all the brokers
// Subscribers needs to listen for new partitions and connect to the brokers.
// Each subscription may not get data. It can act as a backup.

func (b *MessageQueueBroker) Publish(stream mq_pb.SeaweedMessaging_PublishServer) error {
	// 1. write to the volume server
	// 2. find the topic metadata owning filer
	// 3. write to the filer

	var localTopicPartition *topic.LocalPartition
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	response := &mq_pb.PublishResponse{}
	// TODO check whether current broker should be the leader for the topic partition
	ackInterval := 1
	initMessage := req.GetInit()
	if initMessage != nil {
		t, p := topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.Partition)
		localTopicPartition = b.localTopicManager.GetTopicPartition(t, p)
		if localTopicPartition == nil {
			response.Error = fmt.Sprintf("topic %v partition %v not setup", initMessage.Topic, initMessage.Partition)
			glog.Errorf("topic %v partition %v not setup", initMessage.Topic, initMessage.Partition)
			return stream.Send(response)
		}
		ackInterval = int(initMessage.AckInterval)
		stream.Send(response)
	} else {
		response.Error = fmt.Sprintf("missing init message")
		glog.Errorf("missing init message")
		return stream.Send(response)
	}

	clientName := fmt.Sprintf("%v-%4d/%s/%v", findClientAddress(stream.Context()), rand.Intn(10000), initMessage.Topic, initMessage.Partition)
	localTopicPartition.Publishers.AddPublisher(clientName, topic.NewLocalPublisher())

	ackCounter := 0
	var ackSequence int64
	var isStopping int32
	respChan := make(chan *mq_pb.PublishResponse, 128)
	defer func() {
		atomic.StoreInt32(&isStopping, 1)
		close(respChan)
		localTopicPartition.Publishers.RemovePublisher(clientName)
	}()
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case resp := <-respChan:
				if resp != nil {
					if err := stream.Send(resp); err != nil {
						glog.Errorf("Error sending response %v: %v", resp, err)
					}
				} else {
					return
				}
			case <-ticker.C:
				if atomic.LoadInt32(&isStopping) == 0 {
					response := &mq_pb.PublishResponse{
						AckSequence: ackSequence,
					}
					respChan <- response
				} else {
					return
				}
			case <-localTopicPartition.StopPublishersCh:
				respChan <- &mq_pb.PublishResponse{
					AckSequence: ackSequence,
					ShouldClose: true,
				}
			}
		}
	}()

	// process each published messages
	for {
		// receive a message
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		// Process the received message
		if dataMessage := req.GetData(); dataMessage != nil {
			localTopicPartition.Publish(dataMessage)
		}

		ackCounter++
		ackSequence++
		if ackCounter >= ackInterval {
			ackCounter = 0
			// send back the ack
			response := &mq_pb.PublishResponse{
				AckSequence: ackSequence,
			}
			respChan <- response
		}
	}

	glog.V(0).Infof("topic %v partition %v publish stream closed.", initMessage.Topic, initMessage.Partition)

	return nil
}

// duplicated from master_grpc_server.go
func findClientAddress(ctx context.Context) string {
	// fmt.Printf("FromContext %+v\n", ctx)
	pr, ok := peer.FromContext(ctx)
	if !ok {
		glog.Error("failed to get peer from ctx")
		return ""
	}
	if pr.Addr == net.Addr(nil) {
		glog.Error("failed to get peer address")
		return ""
	}
	return pr.Addr.String()
}
