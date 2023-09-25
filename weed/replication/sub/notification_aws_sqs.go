package sub

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
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

func (k *AwsSqsInput) Initialize(configuration util.Configuration, prefix string) error {
	glog.V(0).Infof("replication.notification.aws_sqs.region: %v", configuration.GetString(prefix+"region"))
	glog.V(0).Infof("replication.notification.aws_sqs.sqs_queue_name: %v", configuration.GetString(prefix+"sqs_queue_name"))
	return k.initialize(
		configuration.GetString(prefix+"aws_access_key_id"),
		configuration.GetString(prefix+"aws_secret_access_key"),
		configuration.GetString(prefix+"region"),
		configuration.GetString(prefix+"sqs_queue_name"),
	)
}

func (k *AwsSqsInput) initialize(awsAccessKeyId, awsSecretAccessKey, region, queueName string) (err error) {

	config := &aws.Config{
		Region:                        aws.String(region),
		S3DisableContentMD5Validation: aws.Bool(true),
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

func (k *AwsSqsInput) ReceiveMessage() (key string, message *filer_pb.EventNotification, onSuccessFn func(), onFailureFn func(), err error) {

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
	// fmt.Printf("messages: %+v\n", result.Messages[0])
	keyValue := result.Messages[0].MessageAttributes["key"]
	key = *keyValue.StringValue
	text := *result.Messages[0].Body
	message = &filer_pb.EventNotification{}
	err = proto.Unmarshal([]byte(text), message)
	if err != nil {
		err = fmt.Errorf("unmarshal message from sqs %s: %w", k.queueUrl, err)
		return
	}
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
