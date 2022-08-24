package s3_backend

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func uploadToS3(sess s3iface.S3API, filename string, destBucket string, destKey string, storageClass string, fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {

	//open the file
	f, err := os.Open(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %q, %v", filename, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %q, %v", filename, err)
	}

	fileSize = info.Size()

	partSize := int64(64 * 1024 * 1024) // The minimum/default allowed part size is 5MB
	for partSize*1000 < fileSize {
		partSize *= 4
	}

	// Create an uploader with the session and custom options
	uploader := s3manager.NewUploaderWithClient(sess, func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = 5
	})

	fileReader := &s3UploadProgressedReader{
		fp:      f,
		size:    fileSize,
		signMap: map[int64]struct{}{},
		fn:      fn,
	}

	// Upload the file to S3.
	var result *s3manager.UploadOutput
	result, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:       aws.String(destBucket),
		Key:          aws.String(destKey),
		Body:         fileReader,
		StorageClass: aws.String(storageClass),
	})

	//in case it fails to upload
	if err != nil {
		return 0, fmt.Errorf("failed to upload file %s: %v", filename, err)
	}
	glog.V(1).Infof("file %s uploaded to %s\n", filename, result.Location)

	return
}

// adapted from https://github.com/aws/aws-sdk-go/pull/1868
// https://github.com/aws/aws-sdk-go/blob/main/example/service/s3/putObjectWithProcess/putObjWithProcess.go
type s3UploadProgressedReader struct {
	fp      *os.File
	size    int64
	read    int64
	signMap map[int64]struct{}
	mux     sync.Mutex
	fn      func(progressed int64, percentage float32) error
}

func (r *s3UploadProgressedReader) Read(p []byte) (int, error) {
	return r.fp.Read(p)
}

func (r *s3UploadProgressedReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.fp.ReadAt(p, off)
	if err != nil {
		return n, err
	}

	r.mux.Lock()
	// Ignore the first signature call
	if _, ok := r.signMap[off]; ok {
		r.read += int64(n)
	} else {
		r.signMap[off] = struct{}{}
	}
	r.mux.Unlock()

	if r.fn != nil {
		read := r.read
		if err := r.fn(read, float32(read*100)/float32(r.size)); err != nil {
			return n, err
		}
	}

	return n, err
}

func (r *s3UploadProgressedReader) Seek(offset int64, whence int) (int64, error) {
	return r.fp.Seek(offset, whence)
}
