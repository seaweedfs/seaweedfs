package main

import (
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/client/sub_client"
	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"strings"
	"time"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

var (
	namespace   = flag.String("ns", "test", "namespace")
	t           = flag.String("topic", "test", "topic")
	seedBrokers = flag.String("brokers", "localhost:17777", "seed brokers")

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
		ClientId:                fmt.Sprintf("client-%d", *clientId),
		ConsumerGroup:           "test",
		ConsumerGroupInstanceId: fmt.Sprintf("client-%d", *clientId),
		GrpcDialOption:          grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	contentConfig := &sub_client.ContentConfiguration{
		Topic:     topic.NewTopic(*namespace, *t),
		Filter:    "",
		StartTime: time.Unix(1, 1),
	}

	processorConfig := sub_client.ProcessorConfiguration{
		ConcurrentPartitionLimit: 3,
	}

	brokers := strings.Split(*seedBrokers, ",")
	subscriber := sub_client.NewTopicSubscriber(brokers, subscriberConfig, contentConfig, processorConfig)

	counter := 0
	subscriber.SetEachMessageFunc(func(key, value []byte) (bool, error) {
		counter++
		record := &schema_pb.RecordValue{}
		proto.Unmarshal(value, record)
		fmt.Printf("record: %v\n", record)
		return true, nil
	})

	subscriber.SetCompletionFunc(func() {
		glog.V(0).Infof("done received %d messages", counter)
	})

	if err := subscriber.Subscribe(); err != nil {
		fmt.Println(err)
	}

}
