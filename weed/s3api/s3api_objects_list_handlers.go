package s3api

import (
	"github.com/gorilla/mux"
	"net/http"
	"net/url"
	"strconv"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"context"
	"fmt"
	"path/filepath"
	"time"
	"github.com/chrislusf/seaweedfs/weed/filer2"
)

const (
	maxObjectListSizeLimit = 1000 // Limit number of objects in a listObjectsResponse.
)

func (s3a *S3ApiServer) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html

	// collect parameters
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	originalPrefix, marker, delimiter, maxKeys := getListObjectsV1Args(r.URL.Query())

	if maxKeys < 0 {
		writeErrorResponse(w, ErrInvalidMaxKeys, r.URL)
		return
	}
	if delimiter != "" && delimiter != "/" {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
	}

	// convert full path prefix into directory name and prefix for entry name
	dir, prefix := filepath.Split(originalPrefix)

	// check filer
	var response ListBucketResponse
	err := s3a.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.ListEntriesRequest{
			Directory:          fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, bucket, dir),
			Prefix:             prefix,
			Limit:              uint32(maxKeys),
			StartFromFileName:  marker,
			InclusiveStartFrom: false,
		}

		glog.V(4).Infof("read directory: %v", request)
		resp, err := client.ListEntries(context.Background(), request)
		if err != nil {
			return fmt.Errorf("list buckets: %v", err)
		}

		var contents []ListEntry
		var commonPrefixes []PrefixEntry
		for _, entry := range resp.Entries {
			if entry.IsDirectory {
				commonPrefixes = append(commonPrefixes, PrefixEntry{
					Prefix: fmt.Sprintf("%s%s/", dir, entry.Name),
				})
			} else {
				contents = append(contents, ListEntry{
					Key:          fmt.Sprintf("%s%s", dir, entry.Name),
					LastModified: time.Unix(entry.Attributes.Mtime, 0),
					ETag:         "", // TODO add etag
					Size:         int64(filer2.TotalSize(entry.Chunks)),
					Owner: CanonicalUser{
						ID: fmt.Sprintf("%d", entry.Attributes.Uid),
					},
					StorageClass: StorageClass("STANDARD"),
				})
			}
		}

		response = ListBucketResponse{
			ListBucketResponse: ListBucketResult{
				Name:           bucket,
				Prefix:         originalPrefix,
				Marker:         marker, // TODO
				NextMarker:     "",     // TODO
				MaxKeys:        maxKeys,
				Delimiter:      delimiter,
				IsTruncated:    false, // TODO
				Contents:       contents,
				CommonPrefixes: commonPrefixes,
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

func getListObjectsV1Args(values url.Values) (prefix, marker, delimiter string, maxkeys int) {
	prefix = values.Get("prefix")
	marker = values.Get("marker")
	delimiter = values.Get("delimiter")
	if values.Get("max-keys") != "" {
		maxkeys, _ = strconv.Atoi(values.Get("max-keys"))
	} else {
		maxkeys = maxObjectListSizeLimit
	}
	return
}
