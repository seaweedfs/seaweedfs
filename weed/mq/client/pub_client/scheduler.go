package pub_client

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/buffered_queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type EachPartitionError struct {
	*mq_pb.BrokerPartitionAssignment
	Err        error
	generation int
}

type EachPartitionPublishJob struct {
	*mq_pb.BrokerPartitionAssignment
	stopChan   chan bool
	wg         sync.WaitGroup
	generation int
	inputQueue *buffered_queue.BufferedQueue[*mq_pb.DataMessage]
}

func (p *TopicPublisher) startSchedulerThread(wg *sync.WaitGroup) error {

	if err := p.doConfigureTopic(); err != nil {
		wg.Done()
		return fmt.Errorf("configure topic %s: %v", p.config.Topic, err)
	}

	log.Printf("start scheduler thread for topic %s", p.config.Topic)

	generation := 0
	var errChan chan EachPartitionError
	for {
		glog.V(0).Infof("lookup partitions gen %d topic %s", generation+1, p.config.Topic)
		if assignments, err := p.doLookupTopicPartitions(); err == nil {
			generation++
			glog.V(0).Infof("start generation %d with %d assignments", generation, len(assignments))
			if errChan == nil {
				errChan = make(chan EachPartitionError, len(assignments))
			}
			p.onEachAssignments(generation, assignments, errChan)
		} else {
			glog.Errorf("lookup topic %s: %v", p.config.Topic, err)
			time.Sleep(5 * time.Second)
			continue
		}

		if generation == 1 {
			wg.Done()
		}

		// wait for any error to happen. If so, consume all remaining errors, and retry
		for {
			select {
			case eachErr := <-errChan:
				glog.Errorf("gen %d publish to topic %s partition %v: %v", eachErr.generation, p.config.Topic, eachErr.Partition, eachErr.Err)
				if eachErr.generation < generation {
					continue
				}
				break
			}
		}
	}
}

func (p *TopicPublisher) onEachAssignments(generation int, assignments []*mq_pb.BrokerPartitionAssignment, errChan chan EachPartitionError) {
	// TODO assuming this is not re-configured so the partitions are fixed.
	sort.Slice(assignments, func(i, j int) bool {
		return assignments[i].Partition.RangeStart < assignments[j].Partition.RangeStart
	})
	var jobs []*EachPartitionPublishJob
	hasExistingJob := len(p.jobs) == len(assignments)
	for i, assignment := range assignments {
		if assignment.LeaderBroker == "" {
			continue
		}
		if hasExistingJob {
			var existingJob *EachPartitionPublishJob
			existingJob = p.jobs[i]
			if existingJob.BrokerPartitionAssignment.LeaderBroker == assignment.LeaderBroker {
				existingJob.generation = generation
				jobs = append(jobs, existingJob)
				continue
			} else {
				if existingJob.LeaderBroker != "" {
					close(existingJob.stopChan)
					existingJob.LeaderBroker = ""
					existingJob.wg.Wait()
				}
			}
		}

		// start a go routine to publish to this partition
		job := &EachPartitionPublishJob{
			BrokerPartitionAssignment: assignment,
			stopChan:                  make(chan bool, 1),
			generation:                generation,
			inputQueue:                buffered_queue.NewBufferedQueue[*mq_pb.DataMessage](1024),
		}
		job.wg.Add(1)
		go func(job *EachPartitionPublishJob) {
			defer job.wg.Done()
			if err := p.doPublishToPartition(job); err != nil {
				log.Printf("publish to %s partition %v: %v", p.config.Topic, job.Partition, err)
				errChan <- EachPartitionError{assignment, err, generation}
			}
		}(job)
		jobs = append(jobs, job)
		// TODO assuming this is not re-configured so the partitions are fixed.
		// better just re-use the existing job
		p.partition2Buffer.Insert(assignment.Partition.RangeStart, assignment.Partition.RangeStop, job.inputQueue)
	}
	p.jobs = jobs
}

func (p *TopicPublisher) doPublishToPartition(job *EachPartitionPublishJob) error {

	log.Printf("connecting to %v for topic partition %+v", job.LeaderBroker, job.Partition)

	grpcConnection, err := grpc.NewClient(job.LeaderBroker, grpc.WithTransportCredentials(insecure.NewCredentials()), p.grpcDialOption)
	if err != nil {
		return fmt.Errorf("dial broker %s: %v", job.LeaderBroker, err)
	}
	brokerClient := mq_pb.NewSeaweedMessagingClient(grpcConnection)
	stream, err := brokerClient.PublishMessage(context.Background())
	if err != nil {
		return fmt.Errorf("create publish client: %v", err)
	}
	publishClient := &PublishClient{
		SeaweedMessaging_PublishMessageClient: stream,
		Broker:                                job.LeaderBroker,
	}
	if err = publishClient.Send(&mq_pb.PublishMessageRequest{
		Message: &mq_pb.PublishMessageRequest_Init{
			Init: &mq_pb.PublishMessageRequest_InitMessage{
				Topic:          p.config.Topic.ToPbTopic(),
				Partition:      job.Partition,
				AckInterval:    128,
				FollowerBroker: job.FollowerBroker,
				PublisherName:  p.config.PublisherName,
			},
		},
	}); err != nil {
		return fmt.Errorf("send init message: %v", err)
	}
	// process the hello message
	resp, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("recv init response: %v", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("init response error: %v", resp.Error)
	}

	var publishedTsNs int64
	hasMoreData := int32(1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			ackResp, err := publishClient.Recv()
			if err != nil {
				e, _ := status.FromError(err)
				if e.Code() == codes.Unknown && e.Message() == "EOF" {
					log.Printf("publish to %s EOF", publishClient.Broker)
					return
				}
				publishClient.Err = err
				log.Printf("publish1 to %s error: %v\n", publishClient.Broker, err)
				return
			}
			if ackResp.Error != "" {
				publishClient.Err = fmt.Errorf("ack error: %v", ackResp.Error)
				log.Printf("publish2 to %s error: %v\n", publishClient.Broker, ackResp.Error)
				return
			}
			if ackResp.AckSequence > 0 {
				log.Printf("ack %d published %d hasMoreData:%d", ackResp.AckSequence, atomic.LoadInt64(&publishedTsNs), atomic.LoadInt32(&hasMoreData))
			}
			if atomic.LoadInt64(&publishedTsNs) <= ackResp.AckSequence && atomic.LoadInt32(&hasMoreData) == 0 {
				return
			}
		}
	}()

	publishCounter := 0
	for data, hasData := job.inputQueue.Dequeue(); hasData; data, hasData = job.inputQueue.Dequeue() {
		if data.Ctrl != nil && data.Ctrl.IsClose {
			// need to set this before sending to brokers, to avoid timing issue
			atomic.StoreInt32(&hasMoreData, 0)
		}
		if err := publishClient.Send(&mq_pb.PublishMessageRequest{
			Message: &mq_pb.PublishMessageRequest_Data{
				Data: data,
			},
		}); err != nil {
			return fmt.Errorf("send publish data: %v", err)
		}
		publishCounter++
		atomic.StoreInt64(&publishedTsNs, data.TsNs)
	}
	if publishCounter > 0 {
		wg.Wait()
	} else {
		// CloseSend would cancel the context on the server side
		if err := publishClient.CloseSend(); err != nil {
			return fmt.Errorf("close send: %v", err)
		}
	}

	log.Printf("published %d messages to %v for topic partition %+v", publishCounter, job.LeaderBroker, job.Partition)

	return nil
}

func (p *TopicPublisher) doConfigureTopic() (err error) {
	if len(p.config.Brokers) == 0 {
		return fmt.Errorf("topic configuring found no bootstrap brokers")
	}
	var lastErr error
	for _, brokerAddress := range p.config.Brokers {
		err = pb.WithBrokerGrpcClient(false,
			brokerAddress,
			p.grpcDialOption,
			func(client mq_pb.SeaweedMessagingClient) error {
				_, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
					Topic:          p.config.Topic.ToPbTopic(),
					PartitionCount: p.config.PartitionCount,
					RecordType:     p.config.RecordType, // TODO schema upgrade
				})
				return err
			})
		if err == nil {
			lastErr = nil
			return nil
		} else {
			lastErr = fmt.Errorf("%s: %v", brokerAddress, err)
		}
	}

	if lastErr != nil {
		return fmt.Errorf("doConfigureTopic %s: %v", p.config.Topic, err)
	}
	return nil
}

func (p *TopicPublisher) doLookupTopicPartitions() (assignments []*mq_pb.BrokerPartitionAssignment, err error) {
	if len(p.config.Brokers) == 0 {
		return nil, fmt.Errorf("lookup found no bootstrap brokers")
	}
	var lastErr error
	for _, brokerAddress := range p.config.Brokers {
		err := pb.WithBrokerGrpcClient(false,
			brokerAddress,
			p.grpcDialOption,
			func(client mq_pb.SeaweedMessagingClient) error {
				lookupResp, err := client.LookupTopicBrokers(context.Background(),
					&mq_pb.LookupTopicBrokersRequest{
						Topic: p.config.Topic.ToPbTopic(),
					})
				glog.V(0).Infof("lookup topic %s: %v", p.config.Topic, lookupResp)

				if err != nil {
					return err
				}

				if len(lookupResp.BrokerPartitionAssignments) == 0 {
					return fmt.Errorf("no broker partition assignments")
				}

				assignments = lookupResp.BrokerPartitionAssignments

				return nil
			})
		if err == nil {
			return assignments, nil
		} else {
			lastErr = err
		}
	}

	return nil, fmt.Errorf("lookup topic %s: %v", p.config.Topic, lastErr)

}
