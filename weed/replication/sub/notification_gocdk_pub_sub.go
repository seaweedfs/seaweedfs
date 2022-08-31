//go:build gocdk
// +build gocdk

package sub

import (
	"context"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/awssnssqs"
	"gocloud.dev/pubsub/rabbitpubsub"
	"google.golang.org/protobuf/proto"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	// _ "gocloud.dev/pubsub/azuresb"
	_ "gocloud.dev/pubsub/gcppubsub"
	_ "gocloud.dev/pubsub/natspubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"
)

func init() {
	NotificationInputs = append(NotificationInputs, &GoCDKPubSubInput{})
}

func getPath(rawUrl string) string {
	parsedUrl, _ := url.Parse(rawUrl)
	return path.Join(parsedUrl.Host, parsedUrl.Path)
}

func QueueDeclareAndBind(conn *amqp.Connection, exchangeUrl string, queueUrl string) error {
	exchangeName := getPath(exchangeUrl)
	queueName := getPath(queueUrl)
	exchangeNameDLX := "DLX." + exchangeName
	queueNameDLX := "DLX." + queueName
	ch, err := conn.Channel()
	if err != nil {
		glog.Error(err)
		return err
	}
	defer ch.Close()
	if err := ch.ExchangeDeclare(
		exchangeNameDLX, "fanout", true, false, false, false, nil); err != nil {
		glog.Error(err)
		return err
	}
	if err := ch.ExchangeDeclare(
		exchangeName, "fanout", true, false, false, false, nil); err != nil {
		glog.Error(err)
		return err
	}
	if _, err := ch.QueueDeclare(
		queueName, true, false, false, false,
		amqp.Table{"x-dead-letter-exchange": exchangeNameDLX}); err != nil {
		glog.Error(err)
		return err
	}
	if err := ch.QueueBind(queueName, "", exchangeName, false, nil); err != nil {
		glog.Error(err)
		return err
	}
	if _, err := ch.QueueDeclare(
		queueNameDLX, true, false, false, false,
		amqp.Table{"x-dead-letter-exchange": exchangeName, "x-message-ttl": 600000}); err != nil {
		glog.Error(err)
		return err
	}
	if err := ch.QueueBind(queueNameDLX, "", exchangeNameDLX, false, nil); err != nil {
		glog.Error(err)
		return err
	}
	return nil
}

type GoCDKPubSubInput struct {
	sub    *pubsub.Subscription
	subURL string
}

func (k *GoCDKPubSubInput) GetName() string {
	return "gocdk_pub_sub"
}

func (k *GoCDKPubSubInput) Initialize(configuration util.Configuration, prefix string) error {
	topicUrl := configuration.GetString(prefix + "topic_url")
	k.subURL = configuration.GetString(prefix + "sub_url")
	glog.V(0).Infof("notification.gocdk_pub_sub.sub_url: %v", k.subURL)
	sub, err := pubsub.OpenSubscription(context.Background(), k.subURL)
	if err != nil {
		return err
	}
	var conn *amqp.Connection
	if sub.As(&conn) {
		ch, err := conn.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()
		_, err = ch.QueueInspect(getPath(k.subURL))
		if err != nil {
			if strings.HasPrefix(err.Error(), "Exception (404) Reason") {
				if err := QueueDeclareAndBind(conn, topicUrl, k.subURL); err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}
	k.sub = sub
	return nil
}

func (k *GoCDKPubSubInput) ReceiveMessage() (key string, message *filer_pb.EventNotification, onSuccessFn func(), onFailureFn func(), err error) {
	ctx := context.Background()
	msg, err := k.sub.Receive(ctx)
	if err != nil {
		var conn *amqp.Connection
		if k.sub.As(&conn) && conn.IsClosed() {
			conn.Close()
			k.sub.Shutdown(ctx)
			conn, err = amqp.Dial(os.Getenv("RABBIT_SERVER_URL"))
			if err != nil {
				glog.Error(err)
				time.Sleep(time.Second)
				return
			}
			k.sub = rabbitpubsub.OpenSubscription(conn, getPath(k.subURL), nil)
			return
		}
		// This is permanent cached sub err
		glog.Fatal(err)
	}
	onFailureFn = func() {
		if msg.Nackable() {
			isRedelivered := false
			var delivery amqp.Delivery
			if msg.As(&delivery) {
				isRedelivered = delivery.Redelivered
				glog.Warningf("onFailureFn() metadata: %+v, redelivered: %v", msg.Metadata, delivery.Redelivered)
			}
			if isRedelivered {
				if err := delivery.Nack(false, false); err != nil {
					glog.Error(err)
				}
			} else {
				msg.Nack()
			}
		}
	}
	onSuccessFn = func() {
		msg.Ack()
	}
	key = msg.Metadata["key"]
	message = &filer_pb.EventNotification{}
	err = proto.Unmarshal(msg.Body, message)
	if err != nil {
		return "", nil, onSuccessFn, onFailureFn, err
	}
	return key, message, onSuccessFn, onFailureFn, nil
}
