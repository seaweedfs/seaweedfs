package broker

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/proto"
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

	initMessage := req.GetInit()
	if initMessage == nil {
		response.ErrorCode, response.Error = CreateBrokerError(BrokerErrorInvalidRecord, "missing init message")
		glog.Errorf("missing init message")
		return stream.Send(response)
	}

	// Check whether current broker should be the leader for the topic partition
	leaderBroker, err := b.findBrokerForTopicPartition(initMessage.Topic, initMessage.Partition)
	if err != nil {
		response.ErrorCode, response.Error = CreateBrokerError(BrokerErrorTopicNotFound, fmt.Sprintf("failed to find leader for topic partition: %v", err))
		glog.Errorf("failed to find leader for topic partition: %v", err)
		return stream.Send(response)
	}

	currentBrokerAddress := fmt.Sprintf("%s:%d", b.option.Ip, b.option.Port)
	if leaderBroker != currentBrokerAddress {
		response.ErrorCode, response.Error = CreateBrokerError(BrokerErrorNotLeaderOrFollower, fmt.Sprintf("not the leader for this partition, leader is: %s", leaderBroker))
		glog.V(1).Infof("rejecting publish request: not the leader for partition, leader is: %s", leaderBroker)
		return stream.Send(response)
	}

	// get or generate a local partition
	t, p := topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.Partition)
	localTopicPartition, getOrGenErr := b.GetOrGenerateLocalPartition(t, p)
	if getOrGenErr != nil {
		response.ErrorCode, response.Error = CreateBrokerError(BrokerErrorTopicNotFound, fmt.Sprintf("topic %v not found: %v", t, getOrGenErr))
		glog.Errorf("topic %v not found: %v", t, getOrGenErr)
		return stream.Send(response)
	}

	// connect to follower brokers
	if followerErr := localTopicPartition.MaybeConnectToFollowers(initMessage, b.grpcDialOption); followerErr != nil {
		response.ErrorCode, response.Error = CreateBrokerError(BrokerErrorFollowerConnectionFailed, followerErr.Error())
		glog.Errorf("MaybeConnectToFollowers: %v", followerErr)
		return stream.Send(response)
	}

	var receivedSequence, acknowledgedSequence int64
	var isClosed bool

	// process each published messages
	clientName := fmt.Sprintf("%v-%4d", findClientAddress(stream.Context()), rand.IntN(10000))
	publisher := topic.NewLocalPublisher()
	localTopicPartition.Publishers.AddPublisher(clientName, publisher)

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
				// Update acknowledged offset for this publisher
				publisher.UpdateAckedOffset(acknowledgedSequence)
				// println("sent ack", acknowledgedSequence, "=>", initMessage.PublisherName)
				lastAckTime = time.Now()
			} else {
				time.Sleep(1 * time.Second)
			}
		}
	}()

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

		// Validate RecordValue structure only for schema-based messages
		// Note: Only messages sent via ProduceRecordValue should be in RecordValue format
		// Regular Kafka messages and offset management messages are stored as raw bytes
		if dataMessage.Value != nil {
			record := &schema_pb.RecordValue{}
			if err := proto.Unmarshal(dataMessage.Value, record); err == nil {
				// Successfully unmarshaled as RecordValue - validate structure
				if err := b.validateRecordValue(record, initMessage.Topic); err != nil {
					glog.V(1).Infof("RecordValue validation failed on topic %v partition %v: %v", initMessage.Topic, initMessage.Partition, err)
				}
			}
			// Note: We don't log errors for non-RecordValue messages since most Kafka messages
			// are raw bytes and should not be expected to be in RecordValue format
		}

		// The control message should still be sent to the follower
		// to avoid timing issue when ack messages.

		// Send to the local partition with offset assignment
		t, p := topic.FromPbTopic(initMessage.Topic), topic.FromPbPartition(initMessage.Partition)

		// Create offset assignment function for this partition
		assignOffsetFn := func() (int64, error) {
			return b.offsetManager.AssignOffset(t, p)
		}

		// Use offset-aware publishing
		assignedOffset, err := localTopicPartition.PublishWithOffset(dataMessage, assignOffsetFn)
		if err != nil {
			return fmt.Errorf("topic %v partition %v publish error: %w", initMessage.Topic, initMessage.Partition, err)
		}

		// Update published offset and last seen time for this publisher
		// Use the actual assigned offset instead of timestamp
		publisher.UpdatePublishedOffset(assignedOffset)
	}

	glog.V(0).Infof("topic %v partition %v publish stream from %s closed.", initMessage.Topic, initMessage.Partition, initMessage.PublisherName)

	return nil
}

// validateRecordValue validates the structure and content of a RecordValue message
// Since RecordValue messages are created from successful protobuf unmarshaling,
// their structure is already guaranteed to be valid by the protobuf library.
// Schema validation (if applicable) already happened during Kafka gateway decoding.
func (b *MessageQueueBroker) validateRecordValue(record *schema_pb.RecordValue, topic *schema_pb.Topic) error {
	// Check for nil RecordValue
	if record == nil {
		return fmt.Errorf("RecordValue is nil")
	}

	// Check for nil Fields map
	if record.Fields == nil {
		return fmt.Errorf("RecordValue.Fields is nil")
	}

	// Check for empty Fields map
	if len(record.Fields) == 0 {
		return fmt.Errorf("RecordValue has no fields")
	}

	// If protobuf unmarshaling succeeded, the RecordValue is structurally valid
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
