package pub_client

import (
	"github.com/rdleal/intervalst/interval"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/buffered_queue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"sync"
)

type PublisherConfiguration struct {
	Topic          topic.Topic
	PartitionCount int32
	Brokers        []string
	PublisherName  string // for debugging
	RecordType     *schema_pb.RecordType
}

type PublishClient struct {
	mq_pb.SeaweedMessaging_PublishMessageClient
	Broker string
	Err    error
}
type TopicPublisher struct {
	partition2Buffer *interval.SearchTree[*buffered_queue.BufferedQueue[*mq_pb.DataMessage], int32]
	grpcDialOption   grpc.DialOption
	sync.Mutex       // protects grpc
	config           *PublisherConfiguration
	jobs             []*EachPartitionPublishJob
}

func NewTopicPublisher(config *PublisherConfiguration) *TopicPublisher {
	tp := &TopicPublisher{
		partition2Buffer: interval.NewSearchTree[*buffered_queue.BufferedQueue[*mq_pb.DataMessage]](func(a, b int32) int {
			return int(a - b)
		}),
		grpcDialOption: grpc.WithTransportCredentials(insecure.NewCredentials()),
		config:         config,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if err := tp.startSchedulerThread(&wg); err != nil {
			log.Println(err)
			return
		}
	}()

	wg.Wait()

	return tp
}

func (p *TopicPublisher) Shutdown() error {

	if inputBuffers, found := p.partition2Buffer.AllIntersections(0, pub_balancer.MaxPartitionCount); found {
		for _, inputBuffer := range inputBuffers {
			inputBuffer.CloseInput()
		}
	}

	for _, job := range p.jobs {
		job.wg.Wait()
	}

	return nil
}
