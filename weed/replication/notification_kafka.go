package replication

import (
	"github.com/Shopify/sarama"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func init() {
	NotificationInputs = append(NotificationInputs, &KafkaInput{
	})
}

type KafkaInput struct {
	topic       string
	consumer    sarama.Consumer
	messageChan chan *sarama.ConsumerMessage
}

func (k *KafkaInput) GetName() string {
	return "kafka"
}

func (k *KafkaInput) Initialize(configuration util.Configuration) error {
	glog.V(0).Infof("replication.notification.kafka.hosts: %v\n", configuration.GetStringSlice("hosts"))
	glog.V(0).Infof("replication.notification.kafka.topic: %v\n", configuration.GetString("topic"))
	return k.initialize(
		configuration.GetStringSlice("hosts"),
		configuration.GetString("topic"),
	)
}

func (k *KafkaInput) initialize(hosts []string, topic string) (err error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	k.consumer, err = sarama.NewConsumer(hosts, config)
	k.topic = topic
	k.messageChan = make(chan *sarama.ConsumerMessage, 1)

	partitions, err := k.consumer.Partitions(topic)
	if err != nil {
		panic(err)
	}

	for _, partition := range partitions {
		partitionConsumer, err := k.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		go func() {
			for {
				select {
				case err := <-partitionConsumer.Errors():
					fmt.Println(err)
				case msg := <-partitionConsumer.Messages():
					k.messageChan <- msg
				}
			}
		}()
	}

	return nil
}

func (k *KafkaInput) ReceiveMessage() (key string, message *filer_pb.EventNotification, err error) {

	msg := <-k.messageChan

	key = string(msg.Key)
	message = &filer_pb.EventNotification{}
	err = proto.Unmarshal(msg.Value, message)

	return
}
