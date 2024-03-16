package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"google.golang.org/grpc/peer"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
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
	// 1. write to the volume server
	// 2. find the topic metadata owning filer
	// 3. write to the filer

	var localTopicPartition *topic.LocalPartition
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	response := &mq_pb.PublishMessageResponse{}
	// TODO check whether current broker should be the leader for the topic partition
	ackInterval := 1
	initMessage := req.GetInit()
	var t topic.Topic
	var p topic.Partition
	if initMessage != nil {
		t, p = topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.Partition)
		localTopicPartition, _, err = b.GetOrGenLocalPartition(t, p)
		if err != nil {
			response.Error = fmt.Sprintf("topic %v partition %v not setup", initMessage.Topic, initMessage.Partition)
			glog.Errorf("topic %v partition %v not setup", initMessage.Topic, initMessage.Partition)
			return stream.Send(response)
		}
		ackInterval = int(initMessage.AckInterval)
		for _, follower := range initMessage.FollowerBrokers {
			followErr := b.withBrokerClient(false, pb.ServerAddress(follower), func(client mq_pb.SeaweedMessagingClient) error {
				_, err := client.PublishFollowMe(context.Background(), &mq_pb.PublishFollowMeRequest{
					Topic:      initMessage.Topic,
					Partition:  initMessage.Partition,
					BrokerSelf: string(b.option.BrokerAddress()),
				})
				return err
			})
			if followErr != nil {
				response.Error = fmt.Sprintf("follower %v failed: %v", follower, followErr)
				glog.Errorf("follower %v failed: %v", follower, followErr)
				return stream.Send(response)
			}
		}
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
	respChan := make(chan *mq_pb.PublishMessageResponse, 128)
	defer func() {
		atomic.StoreInt32(&isStopping, 1)
		respChan <- &mq_pb.PublishMessageResponse{
			AckSequence: ackSequence,
		}
		close(respChan)
		localTopicPartition.Publishers.RemovePublisher(clientName)
		glog.V(0).Infof("topic %v partition %v published %d messges Publisher:%d Subscriber:%d", initMessage.Topic, initMessage.Partition, ackSequence, localTopicPartition.Publishers.Size(), localTopicPartition.Subscribers.Size())
		if localTopicPartition.MaybeShutdownLocalPartition() {
			b.localTopicManager.RemoveTopicPartition(t, p)
		}
	}()
	go func() {
		for resp := range respChan {
			if err := stream.Send(resp); err != nil {
				glog.Errorf("Error sending response %v: %v", resp, err)
			}
		}
	}()

	// process each published messages
	for {
		// receive a message
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.V(0).Infof("topic %v partition %v publish stream error: %v", initMessage.Topic, initMessage.Partition, err)
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
			response := &mq_pb.PublishMessageResponse{
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
