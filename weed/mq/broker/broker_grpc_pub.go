package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/peer"
	"io"
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

func (b *MessageQueueBroker) PublishMessage(stream mq_pb.SeaweedMessaging_PublishMessageServer) error {

	req, err := stream.Recv()
	if err != nil {
		return err
	}
	response := &mq_pb.PublishMessageResponse{}
	// TODO check whether current broker should be the leader for the topic partition
	initMessage := req.GetInit()
	if initMessage == nil {
		response.Error = fmt.Sprintf("missing init message")
		glog.Errorf("missing init message")
		return stream.Send(response)
	}

	// get or generate a local partition
	t, p := topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.Partition)
	localTopicPartition, getOrGenErr := b.GetOrGenerateLocalPartition(t, p)
	if getOrGenErr != nil {
		response.Error = fmt.Sprintf("topic %v not found: %v", t, getOrGenErr)
		glog.Errorf("topic %v not found: %v", t, getOrGenErr)
		return stream.Send(response)
	}

	// connect to follower brokers
	if followerErr := localTopicPartition.MaybeConnectToFollowers(initMessage, b.grpcDialOption); followerErr != nil {
		response.Error = followerErr.Error()
		glog.Errorf("MaybeConnectToFollowers: %v", followerErr)
		return stream.Send(response)
	}

	var receivedSequence, acknowledgedSequence int64
	var isClosed bool

	// start sending ack to publisher
	ackInterval := int64(1)
	if initMessage.AckInterval > 0 {
		ackInterval = int64(initMessage.AckInterval)
	}
	go func() {
		defer func() {
			// println("stop sending ack to publisher", initMessage.PublisherName)
		}()

		lastAckTime := time.Now()
		for !isClosed {
			receivedSequence = atomic.LoadInt64(&localTopicPartition.AckTsNs)
			if acknowledgedSequence < receivedSequence && (receivedSequence-acknowledgedSequence >= ackInterval || time.Since(lastAckTime) > 1*time.Second) {
				acknowledgedSequence = receivedSequence
				response := &mq_pb.PublishMessageResponse{
					AckSequence: acknowledgedSequence,
				}
				if err := stream.Send(response); err != nil {
					glog.Errorf("Error sending response %v: %v", response, err)
				}
				// println("sent ack", acknowledgedSequence, "=>", initMessage.PublisherName)
				lastAckTime = time.Now()
			} else {
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// process each published messages
	clientName := fmt.Sprintf("%v-%4d/%s/%v", findClientAddress(stream.Context()), rand.Intn(10000), initMessage.Topic, initMessage.Partition)
	localTopicPartition.Publishers.AddPublisher(clientName, topic.NewLocalPublisher())

	defer func() {
		// remove the publisher
		localTopicPartition.Publishers.RemovePublisher(clientName)
		if localTopicPartition.MaybeShutdownLocalPartition() {
			b.localTopicManager.RemoveLocalPartition(t, p)
			glog.V(0).Infof("Removed local topic %v partition %v", initMessage.Topic, initMessage.Partition)
		}
	}()

	// send a hello message
	stream.Send(&mq_pb.PublishMessageResponse{})

	defer func() {
		isClosed = true
	}()

	// process each published messages
	for {
		// receive a message
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.V(0).Infof("topic %v partition %v publish stream from %s error: %v", initMessage.Topic, initMessage.Partition, initMessage.PublisherName, err)
			break
		}

		// Process the received message
		dataMessage := req.GetData()
		if dataMessage == nil {
			continue
		}

		// The control message should still be sent to the follower
		// to avoid timing issue when ack messages.

		// send to the local partition
		if err = localTopicPartition.Publish(dataMessage); err != nil {
			return fmt.Errorf("topic %v partition %v publish error: %v", initMessage.Topic, initMessage.Partition, err)
		}
	}

	glog.V(0).Infof("topic %v partition %v publish stream from %s closed.", initMessage.Topic, initMessage.Partition, initMessage.PublisherName)

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
