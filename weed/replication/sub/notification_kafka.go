package sub

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	kafkanotif "github.com/seaweedfs/seaweedfs/weed/notification/kafka"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func init() {
	NotificationInputs = append(NotificationInputs, &KafkaInput{})
}

type KafkaInput struct {
	topic       string
	consumer    sarama.Consumer
	messageChan chan *sarama.ConsumerMessage
}

func (k *KafkaInput) GetName() string {
	return "kafka"
}

func (k *KafkaInput) Initialize(configuration util.Configuration, prefix string) error {
	glog.V(0).Infof("replication.notification.kafka.hosts: %v\n", configuration.GetStringSlice(prefix+"hosts"))
	glog.V(0).Infof("replication.notification.kafka.topic: %v\n", configuration.GetString(prefix+"topic"))
	return k.initialize(
		configuration.GetStringSlice(prefix+"hosts"),
		configuration.GetString(prefix+"topic"),
		configuration.GetString(prefix+"offsetFile"),
		configuration.GetInt(prefix+"offsetSaveIntervalSeconds"),
		kafkanotif.SASLTLSConfig{
			SASLEnabled:           configuration.GetBool(prefix + "sasl_enabled"),
			SASLMechanism:         configuration.GetString(prefix + "sasl_mechanism"),
			SASLUsername:          configuration.GetString(prefix + "sasl_username"),
			SASLPassword:          configuration.GetString(prefix + "sasl_password"),
			TLSEnabled:            configuration.GetBool(prefix + "tls_enabled"),
			TLSCACert:             configuration.GetString(prefix + "tls_ca_cert"),
			TLSClientCert:         configuration.GetString(prefix + "tls_client_cert"),
			TLSClientKey:          configuration.GetString(prefix + "tls_client_key"),
			TLSInsecureSkipVerify: configuration.GetBool(prefix + "tls_insecure_skip_verify"),
		},
	)
}

func (k *KafkaInput) initialize(hosts []string, topic string, offsetFile string, offsetSaveIntervalSeconds int, saslTLS kafkanotif.SASLTLSConfig) (err error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	if err = kafkanotif.ConfigureSASLTLS(config, saslTLS); err != nil {
		return fmt.Errorf("kafka consumer security configuration: %w", err)
	}
	k.consumer, err = sarama.NewConsumer(hosts, config)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	glog.V(0).Infof("connected to %v", hosts)

	k.topic = topic
	k.messageChan = make(chan *sarama.ConsumerMessage, 1)

	partitions, err := k.consumer.Partitions(topic)
	if err != nil {
		return fmt.Errorf("get kafka partitions for topic %q: %w", topic, err)
	}

	progress := loadProgress(offsetFile)
	if progress == nil || progress.Topic != topic {
		progress = &KafkaProgress{
			Topic:            topic,
			PartitionOffsets: make(map[int32]int64),
		}
	}
	progress.lastSaveTime = time.Now()
	progress.offsetFile = offsetFile
	progress.offsetSaveIntervalSeconds = offsetSaveIntervalSeconds

	for _, partition := range partitions {
		offset, found := progress.PartitionOffsets[partition]
		if !found {
			offset = sarama.OffsetOldest
		} else {
			offset += 1
		}
		partitionConsumer, err := k.consumer.ConsumePartition(topic, partition, offset)
		if err != nil {
			return fmt.Errorf("consume kafka topic %q partition %d: %w", topic, partition, err)
		}
		go func() {
			for {
				select {
				case err := <-partitionConsumer.Errors():
					fmt.Println(err)
				case msg := <-partitionConsumer.Messages():
					k.messageChan <- msg
					if err := progress.setOffset(msg.Partition, msg.Offset); err != nil {
						glog.Warningf("set kafka offset: %v", err)
					}
				}
			}
		}()
	}

	return nil
}

func (k *KafkaInput) ReceiveMessage() (key string, message *filer_pb.EventNotification, onSuccessFn func(), onFailureFn func(), err error) {

	msg := <-k.messageChan

	key = string(msg.Key)
	message = &filer_pb.EventNotification{}
	err = proto.Unmarshal(msg.Value, message)

	return
}

type KafkaProgress struct {
	Topic                     string          `json:"topic"`
	PartitionOffsets          map[int32]int64 `json:"partitionOffsets"`
	offsetFile                string
	lastSaveTime              time.Time
	offsetSaveIntervalSeconds int
	sync.Mutex
}

func loadProgress(offsetFile string) *KafkaProgress {
	progress := &KafkaProgress{}
	data, err := os.ReadFile(offsetFile)
	if err != nil {
		glog.Warningf("failed to read kafka progress file: %s", offsetFile)
		return nil
	}
	err = json.Unmarshal(data, progress)
	if err != nil {
		glog.Warningf("failed to read kafka progress message: %s", string(data))
		return nil
	}
	return progress
}

func (progress *KafkaProgress) saveProgress() error {
	data, err := json.Marshal(progress)
	if err != nil {
		return fmt.Errorf("failed to marshal progress: %w", err)
	}
	err = util.WriteFile(progress.offsetFile, data, 0640)
	if err != nil {
		return fmt.Errorf("failed to save progress to %s: %v", progress.offsetFile, err)
	}

	progress.lastSaveTime = time.Now()
	return nil
}

func (progress *KafkaProgress) setOffset(partition int32, offset int64) error {
	progress.Lock()
	defer progress.Unlock()

	progress.PartitionOffsets[partition] = offset
	if int(time.Now().Sub(progress.lastSaveTime).Seconds()) > progress.offsetSaveIntervalSeconds {
		return progress.saveProgress()
	}
	return nil
}
