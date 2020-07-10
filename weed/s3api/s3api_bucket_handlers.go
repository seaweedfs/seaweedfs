package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   *s3.Owner
	Buckets []*s3.Bucket `xml:"Buckets>Bucket"`
}

func (s3a *S3ApiServer) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {

	var response ListAllMyBucketsResult

	entries, err := s3a.list(s3a.option.BucketsPath, "", "", false, math.MaxInt32)

	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	var buckets []*s3.Bucket
	for _, entry := range entries {
		if entry.IsDirectory {
			buckets = append(buckets, &s3.Bucket{
				Name:         aws.String(entry.Name),
				CreationDate: aws.Time(time.Unix(entry.Attributes.Crtime, 0).UTC()),
			})
		}
	}

	response = ListAllMyBucketsResult{
		Owner: &s3.Owner{
			ID:          aws.String(""),
			DisplayName: aws.String(""),
		},
		Buckets: buckets,
	}

	writeSuccessResponseXML(w, encodeResponse(response))
}

func (s3a *S3ApiServer) PutBucketHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// create the folder for bucket, but lazily create actual collection
	if err := s3a.mkdir(s3a.option.BucketsPath, bucket, nil); err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	writeSuccessResponseEmpty(w)
}

func (s3a *S3ApiServer) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	err := s3a.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		// delete collection
		deleteCollectionRequest := &filer_pb.DeleteCollectionRequest{
			Collection: bucket,
		}

		glog.V(1).Infof("delete collection: %v", deleteCollectionRequest)
		if _, err := client.DeleteCollection(context.Background(), deleteCollectionRequest); err != nil {
			return fmt.Errorf("delete collection %s: %v", bucket, err)
		}

		return nil
	})

	err = s3a.rm(s3a.option.BucketsPath, bucket, false, true)

	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	writeResponse(w, http.StatusNoContent, nil, mimeNone)
}

func (s3a *S3ApiServer) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	err := s3a.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: s3a.option.BucketsPath,
			Name:      bucket,
		}

		glog.V(1).Infof("lookup bucket: %v", request)
		if _, err := filer_pb.LookupEntry(client, request); err != nil {
			if err == filer_pb.ErrNotFound {
				return filer_pb.ErrNotFound
			}
			return fmt.Errorf("lookup bucket %s/%s: %v", s3a.option.BucketsPath, bucket, err)
		}

		return nil
	})

	if err != nil {
		writeErrorResponse(w, ErrNoSuchBucket, r.URL)
		return
	}

	writeSuccessResponseEmpty(w)
}
