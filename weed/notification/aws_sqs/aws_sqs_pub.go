package aws_sqs

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &AwsSqsPub{})
}

type AwsSqsPub struct {
	svc      *sqs.SQS
	queueUrl string
}

func (k *AwsSqsPub) GetName() string {
	return "aws_sqs"
}

func (k *AwsSqsPub) Initialize(configuration util.Configuration, prefix string) (err error) {
	glog.V(0).Infof("filer.notification.aws_sqs.region: %v", configuration.GetString(prefix+"region"))
	glog.V(0).Infof("filer.notification.aws_sqs.sqs_queue_name: %v", configuration.GetString(prefix+"sqs_queue_name"))
	return k.initialize(
		configuration.GetString(prefix+"aws_access_key_id"),
		configuration.GetString(prefix+"aws_secret_access_key"),
		configuration.GetString(prefix+"region"),
		configuration.GetString(prefix+"sqs_queue_name"),
	)
}

func (k *AwsSqsPub) initialize(awsAccessKeyId, awsSecretAccessKey, region, queueName string) (err error) {

	config := &aws.Config{
		Region: aws.String(region),
	}
	if awsAccessKeyId != "" && awsSecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(awsAccessKeyId, awsSecretAccessKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return fmt.Errorf("create aws session: %v", err)
	}
	k.svc = sqs.New(sess)

	result, err := k.svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {
			return fmt.Errorf("unable to find queue %s", queueName)
		}
		return fmt.Errorf("get queue %s url: %v", queueName, err)
	}

	k.queueUrl = *result.QueueUrl

	return nil
}

func (k *AwsSqsPub) SendMessage(key string, message proto.Message) (err error) {

	text, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("send message marshal %+v: %v", message, err)
	}

	_, err = k.svc.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(10),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"key": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(key),
			},
		},
		MessageBody: aws.String(string(text)),
		QueueUrl:    &k.queueUrl,
	})

	if err != nil {
		return fmt.Errorf("send message to sqs %s: %v", k.queueUrl, err)
	}

	return nil
}
