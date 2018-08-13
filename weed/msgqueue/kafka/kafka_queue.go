package kafka

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/chrislusf/seaweedfs/weed/msgqueue"
	"github.com/golang/protobuf/proto"
	"github.com/Shopify/sarama"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

func init() {
	msgqueue.MessageQueues = append(msgqueue.MessageQueues, &KafkaQueue{})
}

type KafkaQueue struct {
	topic    string
	producer sarama.AsyncProducer
}

func (k *KafkaQueue) GetName() string {
	return "kafka"
}

func (k *KafkaQueue) Initialize(configuration msgqueue.Configuration) (err error) {
	return k.initialize(
		configuration.GetStringSlice("hosts"),
		configuration.GetString("topic"),
	)
}

func (k *KafkaQueue) initialize(hosts []string, topic string) (err error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	k.producer, err = sarama.NewAsyncProducer(hosts, config)
	go k.handleSuccess()
	go k.handleError()
	return nil
}

func (k *KafkaQueue) SendMessage(key string, message proto.Message) (err error) {
	bytes, err := proto.Marshal(message)
	if err != nil {
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(bytes),
	}

	k.producer.Input() <- msg

	return nil
}

func (k *KafkaQueue) handleSuccess() {
	for {
		pm := <-k.producer.Successes()
		if pm != nil {
			glog.Infof("producer message success, partition:%d offset:%d key:%v valus:%s", pm.Partition, pm.Offset, pm.Key, pm.Value)
		}
	}
}

func (k *KafkaQueue) handleError() {
	for {
		err := <-k.producer.Errors()
		if err != nil {
			glog.Errorf("producer message error, partition:%d offset:%d key:%v valus:%s error(%v)", err.Msg.Partition, err.Msg.Offset, err.Msg.Key, err.Msg.Value, err.Err)
		}
	}
}
