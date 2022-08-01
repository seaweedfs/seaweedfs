package s3_backend

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

func downloadFromS3(sess s3iface.S3API, destFileName string, sourceBucket string, sourceKey string,
	fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {

	fileSize, err = getFileSize(sess, sourceBucket, sourceKey)
	if err != nil {
		return
	}

	//open the file
	f, err := os.OpenFile(destFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %q, %v", destFileName, err)
	}
	defer f.Close()

	// Create a downloader with the session and custom options
	downloader := s3manager.NewDownloaderWithClient(sess, func(u *s3manager.Downloader) {
		u.PartSize = int64(64 * 1024 * 1024)
		u.Concurrency = 5
	})

	fileWriter := &s3DownloadProgressedWriter{
		fp:      f,
		size:    fileSize,
		written: 0,
		fn:      fn,
	}

	// Download the file from S3.
	fileSize, err = downloader.Download(fileWriter, &s3.GetObjectInput{
		Bucket: aws.String(sourceBucket),
		Key:    aws.String(sourceKey),
	})
	if err != nil {
		return fileSize, fmt.Errorf("failed to download /buckets/%s%s to %s: %v", sourceBucket, sourceKey, destFileName, err)
	}

	glog.V(1).Infof("downloaded file %s\n", destFileName)

	return
}

// adapted from https://github.com/aws/aws-sdk-go/pull/1868
// and https://petersouter.xyz/s3-download-progress-bar-in-golang/
type s3DownloadProgressedWriter struct {
	size    int64
	written int64
	fn      func(progressed int64, percentage float32) error
	fp      *os.File
}

func (w *s3DownloadProgressedWriter) WriteAt(p []byte, off int64) (int, error) {
	n, err := w.fp.WriteAt(p, off)
	if err != nil {
		return n, err
	}

	// Got the length have read( or means has uploaded), and you can construct your message
	atomic.AddInt64(&w.written, int64(n))

	if w.fn != nil {
		written := w.written
		if err := w.fn(written, float32(written*100)/float32(w.size)); err != nil {
			return n, err
		}
	}

	return n, err
}

func getFileSize(svc s3iface.S3API, bucket string, key string) (filesize int64, error error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	resp, err := svc.HeadObject(params)
	if err != nil {
		return 0, err
	}

	return *resp.ContentLength, nil
}
