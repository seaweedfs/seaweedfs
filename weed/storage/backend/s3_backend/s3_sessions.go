package s3_backend

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var (
	s3Sessions   = make(map[string]s3iface.S3API)
	sessionsLock sync.RWMutex
)

func getSession(region string) (s3iface.S3API, bool) {
	sessionsLock.RLock()
	defer sessionsLock.RUnlock()

	sess, found := s3Sessions[region]
	return sess, found
}

func createSession(awsAccessKeyId, awsSecretAccessKey, region, endpoint string, forcePathStyle bool) (s3iface.S3API, error) {

	sessionsLock.Lock()
	defer sessionsLock.Unlock()

	if t, found := s3Sessions[region]; found {
		return t, nil
	}

	config := &aws.Config{
		Region:                        aws.String(region),
		Endpoint:                      aws.String(endpoint),
		S3ForcePathStyle:              aws.Bool(forcePathStyle),
		S3DisableContentMD5Validation: aws.Bool(true),
	}
	if awsAccessKeyId != "" && awsSecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(awsAccessKeyId, awsSecretAccessKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create aws session in region %s: %v", region, err)
	}
	sess.Handlers.Build.PushBack(func(r *request.Request) {
		r.HTTPRequest.Header.Set("User-Agent", "SeaweedFS/"+util.VERSION_NUMBER)
	})

	t := s3.New(sess)

	s3Sessions[region] = t

	return t, nil

}

func deleteFromS3(sess s3iface.S3API, sourceBucket string, sourceKey string) (err error) {
	_, err = sess.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String(sourceKey),
	})
	return err
}
