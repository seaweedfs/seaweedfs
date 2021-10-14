package basic

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	svc *s3.S3
)

func init() {
	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(&aws.Config{
		Region:     aws.String("us-west-2"),
		Endpoint:   aws.String("localhost:8333"),
		DisableSSL: aws.Bool(true),
	})
	if err != nil {
		exitErrorf("create session, %v", err)
	}

	// Create S3 service client
	svc = s3.New(sess)
}

func TestCreateBucket(t *testing.T) {

	input := &s3.CreateBucketInput{
		Bucket: aws.String("theBucket"),
	}

	result, err := svc.CreateBucket(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				fmt.Println(s3.ErrCodeBucketAlreadyExists, aerr.Error())
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				fmt.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	fmt.Println(result)

}

func TestPutObject(t *testing.T) {

	input := &s3.PutObjectInput{
		ACL:    aws.String("authenticated-read"),
		Body:   aws.ReadSeekCloser(strings.NewReader("filetoupload")),
		Bucket: aws.String("theBucket"),
		Key:    aws.String("exampleobject"),
	}

	result, err := svc.PutObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return
	}

	fmt.Println(result)

}

func TestListBucket(t *testing.T) {

	result, err := svc.ListBuckets(nil)
	if err != nil {
		exitErrorf("Unable to list buckets, %v", err)
	}

	fmt.Println("Buckets:")

	for _, b := range result.Buckets {
		fmt.Printf("* %s created on %s\n",
			aws.StringValue(b.Name), aws.TimeValue(b.CreationDate))
	}

}

func TestListObjectV2(t *testing.T) {

	listObj, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:    aws.String(Bucket),
		Prefix:    aws.String("foo"),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		exitErrorf("Unable to list objects, %v", err)
	}
	for _, content := range listObj.Contents {
		fmt.Println(aws.StringValue(content.Key))
	}
	fmt.Printf("list: %s\n", listObj)

}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

const (
	Bucket = "theBucket"
	object = "foo/bar"
	Data   = "<data>"
)

func TestObjectOp(t *testing.T) {
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(Bucket),
	})
	if err != nil {
		exitErrorf("Unable to create bucket, %v", err)
	}

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(object),
		Body:   strings.NewReader(Data),
	})
	if err != nil {
		exitErrorf("Unable to put object, %v", err)
	}

	dest := fmt.Sprintf("%s_bak", object)
	copyObj, err := svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(Bucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", Bucket, object)),
		Key:        aws.String(dest),
	})
	if err != nil {
		exitErrorf("Unable to copy object, %v", err)
	}
	t.Log("copy object result -> ", copyObj.CopyObjectResult)

	getObj, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(dest),
	})
	if err != nil {
		exitErrorf("Unable to get copy object, %v", err)
	}

	data, err := io.ReadAll(getObj.Body)
	if err != nil {
		exitErrorf("Unable to read object data, %v", err)
	}
	if string(data) != Data {
		t.Error("object data -> ", string(data))
	}

	listObj, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(Bucket),
		Prefix: aws.String("foo/"),
	})
	if err != nil {
		exitErrorf("Unable to list objects, %v", err)
	}
	count := 0
	for _, content := range listObj.Contents {
		key := aws.StringValue(content.Key)
		if key == dest {
			count++
		} else if key == object {
			count++
		}
		if count == 2 {
			break
		}
	}
	if count != 2 {
		exitErrorf("Unable to find two objects, %v", listObj.Contents)
	}

	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(object),
	})
	if err != nil {
		exitErrorf("Unable to delete source object, %v", err)
	}

	_, err = svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(dest),
	})
	if err != nil {
		exitErrorf("Unable to delete object, %v", err)
	}

	_, err = svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(Bucket),
	})

	if err != nil {
		exitErrorf("Unable to delete bucket, %v", err)
	}
}
