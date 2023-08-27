package broker

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"sort"
	"sync"
)

const (
	MaxPartitionCount = 1024
)

func (broker *MessageQueueBroker) FindBrokerLeader(c context.Context, request *mq_pb.FindBrokerLeaderRequest) (*mq_pb.FindBrokerLeaderResponse, error) {
	ret := &mq_pb.FindBrokerLeaderResponse{}
	err := broker.withMasterClient(false, broker.MasterClient.GetMaster(), func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
			FilerGroup: request.FilerGroup,
		})
		if err != nil {
			return err
		}
		if len(resp.ClusterNodes) == 0 {
			return nil
		}
		ret.Broker = resp.ClusterNodes[0].Address
		return nil
	})
	return ret, err
}

func (broker *MessageQueueBroker) AssignSegmentBrokers(c context.Context, request *mq_pb.AssignSegmentBrokersRequest) (*mq_pb.AssignSegmentBrokersResponse, error) {
	ret := &mq_pb.AssignSegmentBrokersResponse{}
	segment := topic.FromPbSegment(request.Segment)

	// check existing segment locations on filer
	existingBrokers, err := broker.checkSegmentOnFiler(segment)
	if err != nil {
		return ret, err
	}

	if len(existingBrokers) > 0 {
		// good if the segment is still on the brokers
		isActive, err := broker.checkSegmentsOnBrokers(segment, existingBrokers)
		if err != nil {
			return ret, err
		}
		if isActive {
			for _, broker := range existingBrokers {
				ret.Brokers = append(ret.Brokers, string(broker))
			}
			return ret, nil
		}
	}

	// randomly pick up to 10 brokers, and find the ones with the lightest load
	selectedBrokers, err := broker.selectBrokers()
	if err != nil {
		return ret, err
	}

	// save the allocated brokers info for this segment on the filer
	if err := broker.saveSegmentBrokersOnFiler(segment, selectedBrokers); err != nil {
		return ret, err
	}

	for _, broker := range selectedBrokers {
		ret.Brokers = append(ret.Brokers, string(broker))
	}
	return ret, nil
}

func (broker *MessageQueueBroker) CheckSegmentStatus(c context.Context, request *mq_pb.CheckSegmentStatusRequest) (*mq_pb.CheckSegmentStatusResponse, error) {
	ret := &mq_pb.CheckSegmentStatusResponse{}
	// TODO add in memory active segment
	return ret, nil
}

func (broker *MessageQueueBroker) CheckBrokerLoad(c context.Context, request *mq_pb.CheckBrokerLoadRequest) (*mq_pb.CheckBrokerLoadResponse, error) {
	ret := &mq_pb.CheckBrokerLoadResponse{}
	// TODO read broker's load
	return ret, nil
}

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
func (broker *MessageQueueBroker) FindTopicBrokers(c context.Context, request *mq_pb.FindTopicBrokersRequest) (*mq_pb.FindTopicBrokersResponse, error) {
	ret := &mq_pb.FindTopicBrokersResponse{}
	// lock the topic

	// find the topic partitions on the filer
	// if the topic is not found
	//   if the request is_for_publish
	//     create the topic
	//   if the request is_for_subscribe
	//     return error not found
	return ret, nil
}

// CheckTopicPartitionsStatus check the topic partitions on the broker
func (broker *MessageQueueBroker) CheckTopicPartitionsStatus(c context.Context, request *mq_pb.CheckTopicPartitionsStatusRequest) (*mq_pb.CheckTopicPartitionsStatusResponse, error) {
	ret := &mq_pb.CheckTopicPartitionsStatusResponse{}
	return ret, nil
}

// createOrUpdateTopicPartitions creates the topic partitions on the broker
// 1. check
func (broker *MessageQueueBroker) createOrUpdateTopicPartitions(topic *topic.Topic, prevAssignment *mq_pb.TopicPartitionsAssignment) (err error) {
	// create or update each partition
	if prevAssignment == nil {
		broker.createOrUpdateTopicPartition(topic, nil)
	} else {
		for _, partitionAssignment := range prevAssignment.BrokerPartitions {
			broker.createOrUpdateTopicPartition(topic, partitionAssignment)
		}
	}
	return nil
}

func (broker *MessageQueueBroker) createOrUpdateTopicPartition(topic *topic.Topic, oldAssignment *mq_pb.BrokerPartitionsAssignment) (newAssignment *mq_pb.BrokerPartitionsAssignment) {
	shouldCreate := broker.confirmBrokerPartitionAssignment(topic, oldAssignment)
	if !shouldCreate {

	}
	return
}
func (broker *MessageQueueBroker) confirmBrokerPartitionAssignment(topic *topic.Topic, oldAssignment *mq_pb.BrokerPartitionsAssignment) (shouldCreate bool) {
	if oldAssignment == nil {
		return true
	}
	for _, b := range oldAssignment.FollowerBrokers {
		pb.WithBrokerClient(false, pb.ServerAddress(b), broker.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
			_, err := client.CheckTopicPartitionsStatus(context.Background(), &mq_pb.CheckTopicPartitionsStatusRequest{
				Namespace:                  string(topic.Namespace),
				Topic:                      topic.Name,
				BrokerPartitionsAssignment: oldAssignment,
				ShouldCancelIfNotMatch:     true,
			})
			if err != nil {
				shouldCreate = true
			}
			return nil
		})
	}
	return
}

func (broker *MessageQueueBroker) checkSegmentsOnBrokers(segment *topic.Segment, brokers []pb.ServerAddress) (active bool, err error) {
	var wg sync.WaitGroup

	for _, candidate := range brokers {
		wg.Add(1)
		go func(candidate pb.ServerAddress) {
			defer wg.Done()
			broker.withBrokerClient(false, candidate, func(client mq_pb.SeaweedMessagingClient) error {
				resp, checkErr := client.CheckSegmentStatus(context.Background(), &mq_pb.CheckSegmentStatusRequest{
					Segment: &mq_pb.Segment{
						Namespace: string(segment.Topic.Namespace),
						Topic:     segment.Topic.Name,
						Id:        segment.Id,
					},
				})
				if checkErr != nil {
					err = checkErr
					glog.V(0).Infof("check segment status on %s: %v", candidate, checkErr)
					return nil
				}
				if resp.IsActive == false {
					active = false
				}
				return nil
			})
		}(candidate)
	}
	wg.Wait()
	return
}

func (broker *MessageQueueBroker) selectBrokers() (brokers []pb.ServerAddress, err error) {
	candidates, err := broker.selectCandidatesFromMaster(10)
	if err != nil {
		return
	}
	brokers, err = broker.pickLightestCandidates(candidates, 3)
	return
}

func (broker *MessageQueueBroker) selectCandidatesFromMaster(limit int32) (candidates []pb.ServerAddress, err error) {
	err = broker.withMasterClient(false, broker.MasterClient.GetMaster(), func(client master_pb.SeaweedClient) error {
		resp, err := client.ListClusterNodes(context.Background(), &master_pb.ListClusterNodesRequest{
			ClientType: cluster.BrokerType,
			FilerGroup: broker.option.FilerGroup,
			Limit:      limit,
		})
		if err != nil {
			return err
		}
		if len(resp.ClusterNodes) == 0 {
			return nil
		}
		for _, node := range resp.ClusterNodes {
			candidates = append(candidates, pb.ServerAddress(node.Address))
		}
		return nil
	})
	return
}

type CandidateStatus struct {
	address      pb.ServerAddress
	messageCount int64
	bytesCount   int64
	load         int64
}

func (broker *MessageQueueBroker) pickLightestCandidates(candidates []pb.ServerAddress, limit int) (selected []pb.ServerAddress, err error) {

	if len(candidates) <= limit {
		return candidates, nil
	}

	candidateStatuses, err := broker.checkBrokerStatus(candidates)
	if err != nil {
		return nil, err
	}

	sort.Slice(candidateStatuses, func(i, j int) bool {
		return candidateStatuses[i].load < candidateStatuses[j].load
	})

	for i, candidate := range candidateStatuses {
		if i >= limit {
			break
		}
		selected = append(selected, candidate.address)
	}

	return
}

func (broker *MessageQueueBroker) checkBrokerStatus(candidates []pb.ServerAddress) (candidateStatuses []*CandidateStatus, err error) {

	candidateStatuses = make([]*CandidateStatus, len(candidates))
	var wg sync.WaitGroup
	for i, candidate := range candidates {
		wg.Add(1)
		go func(i int, candidate pb.ServerAddress) {
			defer wg.Done()
			err = broker.withBrokerClient(false, candidate, func(client mq_pb.SeaweedMessagingClient) error {
				resp, checkErr := client.CheckBrokerLoad(context.Background(), &mq_pb.CheckBrokerLoadRequest{})
				if checkErr != nil {
					err = checkErr
					return err
				}
				candidateStatuses[i] = &CandidateStatus{
					address:      candidate,
					messageCount: resp.MessageCount,
					bytesCount:   resp.BytesCount,
					load:         resp.MessageCount + resp.BytesCount/(64*1024),
				}
				return nil
			})
		}(i, candidate)
	}
	wg.Wait()
	return
}
