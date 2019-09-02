package sub

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/joeslay/seaweedfs/weed/glog"
	"github.com/joeslay/seaweedfs/weed/pb/filer_pb"
	"github.com/joeslay/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	NotificationInputs = append(NotificationInputs, &AwsSqsInput{})
}

type AwsSqsInput struct {
	svc      *sqs.SQS
	queueUrl string
}

func (k *AwsSqsInput) GetName() string {
	return "aws_sqs"
}

func (k *AwsSqsInput) Initialize(configuration util.Configuration) error {
	glog.V(0).Infof("replication.notification.aws_sqs.region: %v", configuration.GetString("region"))
	glog.V(0).Infof("replication.notification.aws_sqs.sqs_queue_name: %v", configuration.GetString("sqs_queue_name"))
	return k.initialize(
		configuration.GetString("aws_access_key_id"),
		configuration.GetString("aws_secret_access_key"),
		configuration.GetString("region"),
		configuration.GetString("sqs_queue_name"),
	)
}

func (k *AwsSqsInput) initialize(awsAccessKeyId, aswSecretAccessKey, region, queueName string) (err error) {

	config := &aws.Config{
		Region: aws.String(region),
	}
	if awsAccessKeyId != "" && aswSecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(awsAccessKeyId, aswSecretAccessKey, "")
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

func (k *AwsSqsInput) ReceiveMessage() (key string, message *filer_pb.EventNotification, err error) {

	// receive message
	result, err := k.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &k.queueUrl,
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(20), // 20 seconds
		WaitTimeSeconds:     aws.Int64(20),
	})
	if err != nil {
		err = fmt.Errorf("receive message from sqs %s: %v", k.queueUrl, err)
		return
	}
	if len(result.Messages) == 0 {
		return
	}

	// process the message
	key = *result.Messages[0].Attributes["key"]
	text := *result.Messages[0].Body
	message = &filer_pb.EventNotification{}
	err = proto.UnmarshalText(text, message)

	// delete the message
	_, err = k.svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &k.queueUrl,
		ReceiptHandle: result.Messages[0].ReceiptHandle,
	})

	if err != nil {
		glog.V(1).Infof("delete message from sqs %s: %v", k.queueUrl, err)
	}

	return
}
