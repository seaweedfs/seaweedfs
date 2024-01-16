package broker

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"time"
)

// ConfigureTopic Runs on any broker, but proxied to the balancer if not the balancer
// It generates an assignments based on existing allocations,
// and then assign the partitions to the brokers.
func (b *MessageQueueBroker) ConfigureTopic(ctx context.Context, request *mq_pb.ConfigureTopicRequest) (resp *mq_pb.ConfigureTopicResponse, err error) {
	if b.currentBalancer == "" {
		return nil, status.Errorf(codes.Unavailable, "no balancer")
	}
	if !b.lockAsBalancer.IsLocked() {
		proxyErr := b.withBrokerClient(false, b.currentBalancer, func(client mq_pb.SeaweedMessagingClient) error {
			resp, err = client.ConfigureTopic(ctx, request)
			return nil
		})
		if proxyErr != nil {
			return nil, proxyErr
		}
		return resp, err
	}

	ret := &mq_pb.ConfigureTopicResponse{}
	ret.BrokerPartitionAssignments, _, err = b.Balancer.LookupOrAllocateTopicPartitions(request.Topic, true, request.PartitionCount)

	for _, bpa := range ret.BrokerPartitionAssignments {
		fmt.Printf("create topic %s partition %+v on %s\n", request.Topic, bpa.Partition, bpa.LeaderBroker)
		if doCreateErr := b.withBrokerClient(false, pb.ServerAddress(bpa.LeaderBroker), func(client mq_pb.SeaweedMessagingClient) error {
			_, doCreateErr := client.AssignTopicPartitions(ctx, &mq_pb.AssignTopicPartitionsRequest{
				Topic: request.Topic,
				BrokerPartitionAssignments: []*mq_pb.BrokerPartitionAssignment{
					{
						Partition: bpa.Partition,
					},
				},
				IsLeader:   true,
				IsDraining: false,
			})
			if doCreateErr != nil {
				return fmt.Errorf("do create topic %s on %s: %v", request.Topic, bpa.LeaderBroker, doCreateErr)
			}
			brokerStats, found := b.Balancer.Brokers.Get(bpa.LeaderBroker)
			if !found {
				brokerStats = pub_balancer.NewBrokerStats()
				if !b.Balancer.Brokers.SetIfAbsent(bpa.LeaderBroker, brokerStats) {
					brokerStats, _ = b.Balancer.Brokers.Get(bpa.LeaderBroker)
				}
			}
			brokerStats.RegisterAssignment(request.Topic, bpa.Partition)
			return nil
		}); doCreateErr != nil {
			return nil, doCreateErr
		}
	}

	glog.V(0).Infof("ConfigureTopic: topic %s partition assignments: %v", request.Topic, ret.BrokerPartitionAssignments)

	return ret, err
}

// AssignTopicPartitions Runs on the assigned broker, to execute the topic partition assignment
func (b *MessageQueueBroker) AssignTopicPartitions(c context.Context, request *mq_pb.AssignTopicPartitionsRequest) (*mq_pb.AssignTopicPartitionsResponse, error) {
	ret := &mq_pb.AssignTopicPartitionsResponse{}
	self := pb.ServerAddress(fmt.Sprintf("%s:%d", b.option.Ip, b.option.Port))

	// drain existing topic partition subscriptions
	for _, assignment := range request.BrokerPartitionAssignments {
		t := topic.FromPbTopic(request.Topic)
		partition := topic.FromPbPartition(assignment.Partition)
		b.accessLock.Lock()
		if request.IsDraining {
			// TODO drain existing topic partition subscriptions
			b.localTopicManager.RemoveTopicPartition(t, partition)
		} else {
			var localPartition *topic.LocalPartition
			if localPartition = b.localTopicManager.GetTopicPartition(t, partition); localPartition == nil {
				localPartition = topic.FromPbBrokerPartitionAssignment(self, partition, assignment, b.genLogFlushFunc(request.Topic, assignment.Partition), b.genLogOnDiskReadFunc(request.Topic, assignment.Partition))
				b.localTopicManager.AddTopicPartition(t, localPartition)
			}
		}
		b.accessLock.Unlock()
	}

	// if is leader, notify the followers to drain existing topic partition subscriptions
	if request.IsLeader {
		for _, brokerPartition := range request.BrokerPartitionAssignments {
			for _, follower := range brokerPartition.FollowerBrokers {
				err := pb.WithBrokerGrpcClient(false, follower, b.grpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
					_, err := client.AssignTopicPartitions(context.Background(), request)
					return err
				})
				if err != nil {
					return ret, err
				}
			}
		}
	}

	glog.V(0).Infof("AssignTopicPartitions: topic %s partition assignments: %v", request.Topic, request.BrokerPartitionAssignments)
	return ret, nil
}

func (b *MessageQueueBroker) genLogFlushFunc(t *mq_pb.Topic, partition *mq_pb.Partition) log_buffer.LogFlushFuncType {
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	partitionGeneration := time.Unix(0, partition.UnixTimeNs).UTC().Format(topic.TIME_FORMAT)
	partitionDir := fmt.Sprintf("%s/%s/%04d-%04d", topicDir, partitionGeneration, partition.RangeStart, partition.RangeStop)

	return func(startTime, stopTime time.Time, buf []byte) {
		if len(buf) == 0 {
			return
		}

		startTime, stopTime = startTime.UTC(), stopTime.UTC()

		targetFile := fmt.Sprintf("%s/%s",partitionDir, startTime.Format(topic.TIME_FORMAT))

		// TODO append block with more metadata

		for {
			if err := b.appendToFile(targetFile, buf); err != nil {
				glog.V(0).Infof("metadata log write failed %s: %v", targetFile, err)
				time.Sleep(737 * time.Millisecond)
			} else {
				break
			}
		}
	}
}

func (b *MessageQueueBroker) genLogOnDiskReadFunc(t *mq_pb.Topic, partition *mq_pb.Partition) log_buffer.LogReadFromDiskFuncType {
	topicDir := fmt.Sprintf("%s/%s/%s", filer.TopicsDir, t.Namespace, t.Name)
	partitionGeneration := time.Unix(0, partition.UnixTimeNs).UTC().Format(topic.TIME_FORMAT)
	partitionDir := fmt.Sprintf("%s/%s/%04d-%04d", topicDir, partitionGeneration, partition.RangeStart, partition.RangeStop)

	lookupFileIdFn := func(fileId string) (targetUrls []string, err error) {
		return b.MasterClient.LookupFileId(fileId)
	}

	eachChunkFn := func (buf []byte, eachLogEntryFn log_buffer.EachLogEntryFuncType, starTsNs, stopTsNs int64) (processedTsNs int64, err error) {
		for pos := 0; pos+4 < len(buf); {

			size := util.BytesToUint32(buf[pos : pos+4])
			if pos+4+int(size) > len(buf) {
				err = fmt.Errorf("LogOnDiskReadFunc: read [%d,%d) from [0,%d)", pos, pos+int(size)+4, len(buf))
				return
			}
			entryData := buf[pos+4 : pos+4+int(size)]

			logEntry := &filer_pb.LogEntry{}
			if err = proto.Unmarshal(entryData, logEntry); err != nil {
				pos += 4 + int(size)
				err = fmt.Errorf("unexpected unmarshal mq_pb.Message: %v", err)
				return
			}
			if logEntry.TsNs < starTsNs {
				pos += 4 + int(size)
				continue
			}
			if stopTsNs != 0 && logEntry.TsNs > stopTsNs {
				println("stopTsNs", stopTsNs, "logEntry.TsNs", logEntry.TsNs)
				return
			}

			if err = eachLogEntryFn(logEntry); err != nil {
				err = fmt.Errorf("process log entry %v: %v", logEntry, err)
				return
			}

			processedTsNs = logEntry.TsNs

			pos += 4 + int(size)

		}

		return
	}

	eachFileFn := func(entry *filer_pb.Entry, eachLogEntryFn log_buffer.EachLogEntryFuncType, starTsNs, stopTsNs int64) (processedTsNs int64, err error) {
		if len(entry.Content) > 0 {
			glog.Warningf("this should not happen. unexpected content in %s/%s", partitionDir, entry.Name)
			return
		}
		var urlStrings []string
		for _, chunk := range entry.Chunks {
			if chunk.Size == 0 {
				continue
			}
			if chunk.IsChunkManifest{
				glog.Warningf("this should not happen. unexpected chunk manifest in %s/%s", partitionDir, entry.Name)
				return
			}
			urlStrings, err = lookupFileIdFn(chunk.FileId)
			if err != nil {
				err = fmt.Errorf("lookup %s: %v", chunk.FileId, err)
				return
			}
			if len(urlStrings) == 0 {
				err = fmt.Errorf("no url found for %s", chunk.FileId)
				return
			}

			// try one of the urlString until util.Get(urlString) succeeds
			var processed bool
			for _, urlString := range urlStrings {
				// TODO optimization opportunity: reuse the buffer
				var data []byte
				if data, _, err = util.Get(urlString); err == nil {
					processed = true
					if processedTsNs, err = eachChunkFn(data, eachLogEntryFn, starTsNs, stopTsNs); err != nil {
						return
					}
					break
				}
			}
			if !processed {
				err = fmt.Errorf("no data processed for %s %s", entry.Name, chunk.FileId)
				return
			}

		}
		return
	}

	return func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (lastReadPosition log_buffer.MessagePosition, isDone bool, err error) {
		startFileName := startPosition.UTC().Format(topic.TIME_FORMAT)
		startTsNs := startPosition.Time.UnixNano()
		stopTime := time.Unix(0, stopTsNs)
		var processedTsNs int64
		err = b.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
			return filer_pb.SeaweedList(client, partitionDir, "", func(entry *filer_pb.Entry, isLast bool) error {
				if entry.IsDirectory {
					return nil
				}
				if stopTsNs!=0 && entry.Name > stopTime.UTC().Format(topic.TIME_FORMAT) {
					isDone = true
					return nil
				}
				if entry.Name < startPosition.UTC().Format(topic.TIME_FORMAT) {
					return nil
				}
				if processedTsNs, err = eachFileFn(entry, eachLogEntryFn, startTsNs, stopTsNs); err != nil {
					return err
				}
				return nil

			}, startFileName, true, math.MaxInt32)
		})
		lastReadPosition = log_buffer.NewMessagePosition(processedTsNs, -2)
		return
	}
}
