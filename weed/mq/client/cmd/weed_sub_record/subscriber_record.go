package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"strings"
	"time"
)

var (
	namespace               = flag.String("ns", "test", "namespace")
	t                       = flag.String("topic", "test", "topic")
	seedBrokers             = flag.String("brokers", "localhost:17777", "seed brokers")
	maxPartitionCount       = flag.Int("maxPartitionCount", 3, "max partition count")
	perPartitionConcurrency = flag.Int("perPartitionConcurrency", 1, "per partition concurrency")
	timeAgo                 = flag.Duration("timeAgo", 1*time.Hour, "start time before now. \"300ms\", \"1.5h\" or \"2h45m\". Valid time units are \"ns\", \"us\" (or \"µs\"), \"ms\", \"s\", \"m\", \"h\"")

	clientId = flag.Uint("client_id", uint(util.RandomInt32()), "client id")
)

type MyRecord struct {
	Key    []byte
	Field1 []byte
	Field2 string
	Field3 int32
	Field4 int64
	Field5 float32
	Field6 float64
	Field7 bool
}

func FromSchemaRecordValue(recordValue *schema_pb.RecordValue) *MyRecord {
	return &MyRecord{
		Key:    recordValue.Fields["key"].GetBytesValue(),
		Field1: recordValue.Fields["field1"].GetBytesValue(),
		Field2: recordValue.Fields["field2"].GetStringValue(),
		Field3: recordValue.Fields["field3"].GetInt32Value(),
		Field4: recordValue.Fields["field4"].GetInt64Value(),
		Field5: recordValue.Fields["field5"].GetFloatValue(),
		Field6: recordValue.Fields["field6"].GetDoubleValue(),
		Field7: recordValue.Fields["field7"].GetBoolValue(),
	}
}

func main() {
	flag.Parse()
	util_http.InitGlobalHttpClient()

	subscriberConfig := &sub_client.SubscriberConfiguration{
		ConsumerGroup:           "test",
		ConsumerGroupInstanceId: fmt.Sprintf("client-%d", *clientId),
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
		MaxPartitionCount:       int32(*maxPartitionCount),
		SlidingWindowSize:       int32(*perPartitionConcurrency),
	}

	contentConfig := &sub_client.ContentConfiguration{
		Topic:  topic.NewTopic(*namespace, *t),
		Filter: "",
		// StartTime: time.Now().Add(-*timeAgo),
	}

	brokers := strings.Split(*seedBrokers, ",")
	subscriber := sub_client.NewTopicSubscriber(brokers, subscriberConfig, contentConfig, make(chan sub_client.KeyedOffset, 1024))

	counter := 0
	executors := util.NewLimitedConcurrentExecutor(int(subscriberConfig.SlidingWindowSize))
	subscriber.SetOnDataMessageFn(func(m *mq_pb.SubscribeMessageResponse_Data) {
		executors.Execute(func() {
			counter++
			record := &schema_pb.RecordValue{}
			err := proto.Unmarshal(m.Data.Value, record)
			if err != nil {
				fmt.Printf("unmarshal record value: %v\n", err)
			} else {
				fmt.Printf("%s %d: %v\n", string(m.Data.Key), len(m.Data.Value), record)
			}
		})
	})

	subscriber.SetCompletionFunc(func() {
		glog.V(0).Infof("done received %d messages", counter)
	})

	if err := subscriber.Subscribe(); err != nil {
		fmt.Println(err)
	}

}
