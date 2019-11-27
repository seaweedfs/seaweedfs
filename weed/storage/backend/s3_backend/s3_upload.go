package s3_backend

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func uploadToS3(sess s3iface.S3API, filename string, destBucket string, destKey string) error {

	//open the file
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file %q, %v", filename, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file %q, %v", filename, err)
	}

	fileSize := info.Size()

	partSize := int64(64 * 1024 * 1024) // The minimum/default allowed part size is 5MB
	for partSize*1000 < fileSize {
		partSize *= 4
	}

	// Create an uploader with the session and custom options
	uploader := s3manager.NewUploaderWithClient(sess, func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = 15 // default is 15
	})

	// Upload the file to S3.
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:               aws.String(destBucket),
		Key:                  aws.String(destKey),
		Body:                 f,
		ACL:                  aws.String("private"),
		ServerSideEncryption: aws.String("AES256"),
		StorageClass:         aws.String("STANDARD_IA"),
	})

	//in case it fails to upload
	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err)
	}
	fmt.Printf("file %s uploaded to %s\n", filename, result.Location)

	return nil
}
