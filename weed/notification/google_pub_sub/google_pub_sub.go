package google_pub_sub

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/notification"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
	"google.golang.org/api/option"
)

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &GooglePubSub{})
}

type GooglePubSub struct {
	topic *pubsub.Topic
}

func (k *GooglePubSub) GetName() string {
	return "google_pub_sub"
}

func (k *GooglePubSub) Initialize(configuration util.Configuration) (err error) {
	glog.V(0).Infof("notification.google_pub_sub.project_id: %v", configuration.GetString("project_id"))
	glog.V(0).Infof("notification.google_pub_sub.topic: %v", configuration.GetString("topic"))
	return k.initialize(
		configuration.GetString("google_application_credentials"),
		configuration.GetString("project_id"),
		configuration.GetString("topic"),
	)
}

func (k *GooglePubSub) initialize(googleApplicationCredentials, projectId, topicName string) (err error) {

	ctx := context.Background()
	// Creates a client.
	if googleApplicationCredentials == "" {
		var found bool
		googleApplicationCredentials, found = os.LookupEnv("GOOGLE_APPLICATION_CREDENTIALS")
		util.LogFatalIf(!found, "need to specific GOOGLE_APPLICATION_CREDENTIALS env variable or google_application_credentials in filer.toml")
	}

	client, err := pubsub.NewClient(ctx, projectId, option.WithCredentialsFile(googleApplicationCredentials))
	util.LogFatalIfError(err, "Failed to create client: %v", err)

	k.topic = client.Topic(topicName)
	exists, err := k.topic.Exists(ctx)
	util.LogFatalIfError(err, "Failed to check topic %s: %v", topicName, err)
	if !exists {
		k.topic, err = client.CreateTopic(ctx, topicName)
		util.LogFatalIfError(err, "Failed to create topic %s: %v", topicName, err)
	}

	return nil
}

func (k *GooglePubSub) SendMessage(key string, message proto.Message) (err error) {

	bytes, err := proto.Marshal(message)
	if err != nil {
		return
	}

	ctx := context.Background()
	result := k.topic.Publish(ctx, &pubsub.Message{
		Data:       bytes,
		Attributes: map[string]string{"key": key},
	})

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("send message to google pub sub %s: %v", k.topic.String(), err)
	}

	return nil
}
