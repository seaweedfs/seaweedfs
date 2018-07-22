package s3api

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/gorilla/mux"
	"net/http"
	"os"
	"time"
)

var (
	OS_UID = uint32(os.Getuid())
	OS_GID = uint32(os.Getgid())
)

func (s3a *S3ApiServer) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {

	var response ListAllMyBucketsResponse
	err := s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.ListEntriesRequest{
			Directory: s3a.option.BucketsPath,
		}

		glog.V(4).Infof("read directory: %v", request)
		resp, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return fmt.Errorf("list buckets: %v", err)
		}

		var buckets []ListAllMyBucketsEntry
		for _, entry := range resp.Entries {
			if entry.IsDirectory {
				buckets = append(buckets, ListAllMyBucketsEntry{
					Name:         entry.Name,
					CreationDate: time.Unix(entry.Attributes.Crtime, 0),
				})
			}
		}

		response = ListAllMyBucketsResponse{
			ListAllMyBucketsResponse: ListAllMyBucketsResult{
				Owner: CanonicalUser{
					ID:          "",
					DisplayName: "",
				},
				Buckets: ListAllMyBucketsList{
					Bucket: buckets,
				},
			},
		}

		return nil
	})

	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(response))
}

func (s3a *S3ApiServer) PutBucketHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	err := s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.CreateEntryRequest{
			Directory: s3a.option.BucketsPath,
			Entry: &filer_pb.Entry{
				Name:        bucket,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(0777 | os.ModeDir),
					Uid:      OS_UID,
					Gid:      OS_GID,
				},
			},
		}

		glog.V(1).Infof("create bucket: %v", request)
		if _, err := client.CreateEntry(context.Background(), request); err != nil {
			return fmt.Errorf("mkdir %s/%s: %v", s3a.option.BucketsPath, bucket, err)
		}

		// lazily create collection

		return nil
	})

	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	writeSuccessResponseEmpty(w)
}

func (s3a *S3ApiServer) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	err := s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		ctx := context.Background()

		// delete collection
		deleteCollectionRequest := &filer_pb.DeleteCollectionRequest{
			Collection: bucket,
		}

		glog.V(1).Infof("delete collection: %v", deleteCollectionRequest)
		if _, err := client.DeleteCollection(ctx, deleteCollectionRequest); err != nil {
			return fmt.Errorf("delete collection %s: %v", bucket, err)
		}

		// delete bucket metadata
		request := &filer_pb.DeleteEntryRequest{
			Directory:    s3a.option.BucketsPath,
			Name:         bucket,
			IsDirectory:  true,
			IsDeleteData: false,
			IsRecursive:  true,
		}

		glog.V(1).Infof("delete bucket: %v", request)
		if _, err := client.DeleteEntry(ctx, request); err != nil {
			return fmt.Errorf("delete bucket %s/%s: %v", s3a.option.BucketsPath, bucket, err)
		}

		return nil
	})

	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	writeResponse(w, http.StatusNoContent, nil, mimeNone)
}

func (s3a *S3ApiServer) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	bucket := vars["bucket"]

	err := s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: s3a.option.BucketsPath,
			Name:      bucket,
		}

		glog.V(1).Infof("lookup bucket: %v", request)
		if _, err := client.LookupDirectoryEntry(context.Background(), request); err != nil {
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
