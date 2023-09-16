package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// FindTopicBrokers returns the brokers that are serving the topic
//
//  1. lock the topic
//
//  2. find the topic partitions on the filer
//     2.1 if the topic is not found, return error
//     2.2 if the request is_for_publish, create the topic
//     2.2.1 if the request is_for_subscribe, return error not found
//     2.2.2 if the request is_for_publish, create the topic
//     2.2 if the topic is found, return the brokers
//
//  3. unlock the topic
func (broker *MessageQueueBroker) LookupTopicBrokers(ctx context.Context, request *mq_pb.LookupTopicBrokersRequest) (*mq_pb.LookupTopicBrokersResponse, error) {
	ret := &mq_pb.LookupTopicBrokersResponse{}
	// TODO lock the topic

	// find the topic partitions on the filer
	// if the topic is not found
	//   if the request is_for_publish
	//     create the topic
	//   if the request is_for_subscribe
	//     return error not found
	// t := topic.FromPbTopic(request.Topic)
	ret.Topic = request.Topic
	ret.BrokerPartitionAssignments = []*mq_pb.BrokerPartitionAssignment{
		{
			LeaderBroker:    "localhost:17777",
			FollowerBrokers: []string{"localhost:17777"},
			Partition: &mq_pb.Partition{
				RingSize:   MaxPartitionCount,
				RangeStart: 0,
				RangeStop:  MaxPartitionCount,
			},
		},
	}
	return ret, nil
}

// CheckTopicPartitionsStatus check the topic partitions on the broker
func (broker *MessageQueueBroker) CheckTopicPartitionsStatus(c context.Context, request *mq_pb.CheckTopicPartitionsStatusRequest) (*mq_pb.CheckTopicPartitionsStatusResponse, error) {
	ret := &mq_pb.CheckTopicPartitionsStatusResponse{}
	return ret, nil
}
