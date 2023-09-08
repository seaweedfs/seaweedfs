package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"sync/atomic"
	"time"
)

// For a new or re-configured topic, or one of the broker went offline,
//   the pub clients ask one broker what are the brokers for all the topic partitions.
// The broker will lock the topic on write.
//   1. if the topic is not found, create the topic, and allocate the topic partitions to the brokers
//   2. if the topic is found, return the brokers for the topic partitions
// For a topic to read from, the sub clients ask one broker what are the brokers for all the topic partitions.
// The broker will lock the topic on read.
//   1. if the topic is not found, return error
//   2. if the topic is found, return the brokers for the topic partitions
//
// If the topic needs to be re-balanced, the admin client will lock the topic,
// 1. collect throughput information for all the brokers
// 2. adjust the topic partitions to the brokers
// 3. notify the brokers to add/remove partitions to host
//    3.1 When locking the topic, the partitions and brokers should be remembered in the lock.
// 4. the brokers will stop process incoming messages if not the right partition
//    4.1 the pub clients will need to re-partition the messages and publish to the right brokers for the partition3
//    4.2 the sub clients will need to change the brokers to read from
//
// The following is from each individual component's perspective:
// For a pub client
//   For current topic/partition, ask one broker for the brokers for the topic partitions
//     1. connect to the brokers and keep sending, until the broker returns error, or the broker leader is moved.
// For a sub client
//   For current topic/partition, ask one broker for the brokers for the topic partitions
//     1. connect to the brokers and keep reading, until the broker returns error, or the broker leader is moved.
// For a broker
//   Upon a pub client lookup:
//     1. lock the topic
//       2. if already has topic partition assignment, check all brokers are healthy
//       3. if not, create topic partition assignment
//     2. return the brokers for the topic partitions
//     3. unlock the topic
//   Upon a sub client lookup:
//     1. lock the topic
//       2. if already has topic partition assignment, check all brokers are healthy
//       3. if not, return error
//     2. return the brokers for the topic partitions
//     3. unlock the topic
// For an admin tool
//   0. collect stats from all the brokers, and find the topic worth moving
//   1. lock the topic
//   2. collect throughput information for all the brokers
//   3. adjust the topic partitions to the brokers
//   4. notify the brokers to add/remove partitions to host
//   5. the brokers will stop process incoming messages if not the right partition
//   6. unlock the topic

/*
The messages are buffered in memory, and saved to filer under
	/topics/<topic>/<date>/<hour>/<segment>/*.msg
	/topics/<topic>/<date>/<hour>/segment
	/topics/<topic>/info/segment_<id>.meta



*/

func (broker *MessageQueueBroker) Publish(stream mq_pb.SeaweedMessaging_PublishServer) error {
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
		localTopicPartition = broker.localTopicManager.GetTopicPartition(t, p)
		if localTopicPartition == nil {
			localTopicPartition = topic.NewLocalPartition(t, p, true, nil)
			broker.localTopicManager.AddTopicPartition(t, localTopicPartition)
		}
		ackInterval = int(initMessage.AckInterval)
		stream.Send(response)
	} else {
		response.Error = fmt.Sprintf("topic %v partition %v not found", initMessage.Topic, initMessage.Partition)
		glog.Errorf("topic %v partition %v not found", initMessage.Topic, initMessage.Partition)
		return stream.Send(response)
	}

	ackCounter := 0
	var ackSequence int64
	var isStopping int32
	respChan := make(chan *mq_pb.PublishResponse, 128)
	defer func() {
		atomic.StoreInt32(&isStopping, 1)
		close(respChan)
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

	glog.Infof("publish stream closed")

	return nil
}

// AssignTopicPartitions Runs on the assigned broker, to execute the topic partition assignment
func (broker *MessageQueueBroker) AssignTopicPartitions(c context.Context, request *mq_pb.AssignTopicPartitionsRequest) (*mq_pb.AssignTopicPartitionsResponse, error) {
	ret := &mq_pb.AssignTopicPartitionsResponse{}
	self := pb.ServerAddress(fmt.Sprintf("%s:%d", broker.option.Ip, broker.option.Port))

	for _, brokerPartition := range request.BrokerPartitionAssignments {
		localPartiton := topic.FromPbBrokerPartitionAssignment(self, brokerPartition)
		broker.localTopicManager.AddTopicPartition(
			topic.FromPbTopic(request.Topic),
			localPartiton)
		if request.IsLeader {
			for _, follower := range localPartiton.FollowerBrokers {
				err := pb.WithBrokerGrpcClient(false, follower.String(), broker.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
					_, err := client.AssignTopicPartitions(context.Background(), request)
					return err
				})
				if err != nil {
					return ret, err
				}
			}
		}
	}

	return ret, nil
}
