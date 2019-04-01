package s3api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/gorilla/mux"
)

const (
	maxObjectListSizeLimit = 1000 // Limit number of objects in a listObjectsResponse.
)

func (s3a *S3ApiServer) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html

	// collect parameters
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	glog.V(4).Infof("read v2: %v", vars)

	originalPrefix, marker, startAfter, delimiter, _, maxKeys := getListObjectsV2Args(r.URL.Query())

	if maxKeys < 0 {
		writeErrorResponse(w, ErrInvalidMaxKeys, r.URL)
		return
	}
	if delimiter != "" && delimiter != "/" {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	if marker == "" {
		marker = startAfter
	}

	ctx := context.Background()

	response, err := s3a.listFilerEntries(ctx, bucket, originalPrefix, maxKeys, marker)

	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(response))
}

func (s3a *S3ApiServer) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html

	// collect parameters
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	ctx := context.Background()

	originalPrefix, marker, delimiter, maxKeys := getListObjectsV1Args(r.URL.Query())

	if maxKeys < 0 {
		writeErrorResponse(w, ErrInvalidMaxKeys, r.URL)
		return
	}
	if delimiter != "" && delimiter != "/" {
		writeErrorResponse(w, ErrNotImplemented, r.URL)
		return
	}

	response, err := s3a.listFilerEntries(ctx, bucket, originalPrefix, maxKeys, marker)

	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(response))
}

func (s3a *S3ApiServer) listFilerEntries(ctx context.Context, bucket, originalPrefix string, maxKeys int, marker string) (response ListBucketResult, err error) {

	// convert full path prefix into directory name and prefix for entry name
	dir, prefix := filepath.Split(originalPrefix)

	// check filer
	err = s3a.withFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.ListEntriesRequest{
			Directory:          fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, bucket, dir),
			Prefix:             prefix,
			Limit:              uint32(maxKeys + 1),
			StartFromFileName:  marker,
			InclusiveStartFrom: false,
		}

		resp, err := client.ListEntries(ctx, request)
		if err != nil {
			return fmt.Errorf("list buckets: %v", err)
		}

		var contents []ListEntry
		var commonPrefixes []PrefixEntry
		var counter int
		var lastEntryName string
		var isTruncated bool
		for _, entry := range resp.Entries {
			counter++
			if counter > maxKeys {
				isTruncated = true
				break
			}
			lastEntryName = entry.Name
			if entry.IsDirectory {
				commonPrefixes = append(commonPrefixes, PrefixEntry{
					Prefix: fmt.Sprintf("%s%s/", dir, entry.Name),
				})
			} else {
				contents = append(contents, ListEntry{
					Key:          fmt.Sprintf("%s%s", dir, entry.Name),
					LastModified: time.Unix(entry.Attributes.Mtime, 0),
					ETag:         "\"" + filer2.ETag(entry.Chunks) + "\"",
					Size:         int64(filer2.TotalSize(entry.Chunks)),
					Owner: CanonicalUser{
						ID:          fmt.Sprintf("%x", entry.Attributes.Uid),
						DisplayName: entry.Attributes.UserName,
					},
					StorageClass: "STANDARD",
				})
			}
		}

		response = ListBucketResult{
			Name:           bucket,
			Prefix:         originalPrefix,
			Marker:         marker,
			NextMarker:     lastEntryName,
			MaxKeys:        maxKeys,
			Delimiter:      "/",
			IsTruncated:    isTruncated,
			Contents:       contents,
			CommonPrefixes: commonPrefixes,
		}

		glog.V(4).Infof("read directory: %v, found: %v, %+v", request, counter, response)

		return nil
	})

	return
}

func getListObjectsV2Args(values url.Values) (prefix, token, startAfter, delimiter string, fetchOwner bool, maxkeys int) {
	prefix = values.Get("prefix")
	token = values.Get("continuation-token")
	startAfter = values.Get("start-after")
	delimiter = values.Get("delimiter")
	if values.Get("max-keys") != "" {
		maxkeys, _ = strconv.Atoi(values.Get("max-keys"))
	} else {
		maxkeys = maxObjectListSizeLimit
	}
	fetchOwner = values.Get("fetch-owner") == "true"
	return
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
