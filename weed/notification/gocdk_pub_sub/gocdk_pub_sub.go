//go:build gocdk
// +build gocdk

// Package gocdk_pub_sub supports the Go CDK (Cloud Development Kit) PubSub API,
// which in turn supports many providers, including Amazon SNS/SQS, Azure Service Bus,
// Google Cloud PubSub, and RabbitMQ.
//
// In the config, select a provider and topic using a URL. See
// https://godoc.org/gocloud.dev/pubsub and its sub-packages for details.
//
// The Go CDK PubSub API does not support administrative operations like topic
// creation. Create the topic using a UI, CLI or provider-specific API before running
// weed.
//
// The Go CDK obtains credentials via environment variables and other
// provider-specific default mechanisms. See the provider's documentation for
// details.
package gocdk_pub_sub

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"
	"gocloud.dev/pubsub/rabbitpubsub"
	"google.golang.org/protobuf/proto"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/util"
	// _ "gocloud.dev/pubsub/azuresb"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/natspubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"
	"os"
)

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &GoCDKPubSub{})
}

func getPath(rawUrl string) string {
	parsedUrl, _ := url.Parse(rawUrl)
	return path.Join(parsedUrl.Host, parsedUrl.Path)
}

type GoCDKPubSub struct {
	topicURL  string
	topic     *pubsub.Topic
	topicLock sync.RWMutex
}

func (k *GoCDKPubSub) GetName() string {
	return "gocdk_pub_sub"
}

func (k *GoCDKPubSub) setTopic(topic *pubsub.Topic) {
	k.topicLock.Lock()
	k.topic = topic
	k.topicLock.Unlock()
	k.doReconnect()
}

func (k *GoCDKPubSub) doReconnect() {
	var conn *amqp.Connection
	k.topicLock.RLock()
	defer k.topicLock.RUnlock()
	if k.topic.As(&conn) {
		go func(c *amqp.Connection) {
			<-c.NotifyClose(make(chan *amqp.Error))
			c.Close()
			k.topicLock.RLock()
			k.topic.Shutdown(context.Background())
			k.topicLock.RUnlock()
			for {
				glog.Info("Try reconnect")
				conn, err := amqp.Dial(os.Getenv("RABBIT_SERVER_URL"))
				if err == nil {
					k.setTopic(rabbitpubsub.OpenTopic(conn, getPath(k.topicURL), nil))
					break
				}
				glog.Error(err)
				time.Sleep(time.Second)
			}
		}(conn)
	}
}

func (k *GoCDKPubSub) Initialize(configuration util.Configuration, prefix string) error {
	k.topicURL = configuration.GetString(prefix + "topic_url")
	glog.V(0).Infof("notification.gocdk_pub_sub.topic_url: %v", k.topicURL)
	topic, err := pubsub.OpenTopic(context.Background(), k.topicURL)
	if err != nil {
		glog.Fatalf("Failed to open topic: %v", err)
	}
	k.setTopic(topic)
	return nil
}

func (k *GoCDKPubSub) SendMessage(key string, message proto.Message) error {
	bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	k.topicLock.RLock()
	defer k.topicLock.RUnlock()
	err = k.topic.Send(context.Background(), &pubsub.Message{
		Body:     bytes,
		Metadata: map[string]string{"key": key},
	})
	if err != nil {
		return fmt.Errorf("send message via Go CDK pubsub %s: %v", k.topicURL, err)
	}
	return nil
}
