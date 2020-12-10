package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	xhttp "github.com/chrislusf/seaweedfs/weed/s3api/http"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
)

type ListBucketResultV2 struct {
	XMLName               xml.Name      `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string        `xml:"Name"`
	Prefix                string        `xml:"Prefix"`
	MaxKeys               int           `xml:"MaxKeys"`
	Delimiter             string        `xml:"Delimiter,omitempty"`
	IsTruncated           bool          `xml:"IsTruncated"`
	Contents              []ListEntry   `xml:"Contents,omitempty"`
	CommonPrefixes        []PrefixEntry `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     string        `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string        `xml:"NextContinuationToken,omitempty"`
	KeyCount              int           `xml:"KeyCount"`
	StartAfter            string        `xml:"StartAfter,omitempty"`
}

func (s3a *S3ApiServer) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html

	// collect parameters
	bucket, _ := getBucketAndObject(r)

	originalPrefix, continuationToken, startAfter, delimiter, _, maxKeys := getListObjectsV2Args(r.URL.Query())

	if maxKeys < 0 {
		writeErrorResponse(w, s3err.ErrInvalidMaxKeys, r.URL)
		return
	}
	if delimiter != "" && delimiter != "/" {
		writeErrorResponse(w, s3err.ErrNotImplemented, r.URL)
		return
	}

	marker := continuationToken
	if continuationToken == "" {
		marker = startAfter
	}

	response, err := s3a.listFilerEntries(bucket, originalPrefix, maxKeys, marker, delimiter)

	if err != nil {
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}
	responseV2 := &ListBucketResultV2{
		XMLName:               response.XMLName,
		Name:                  response.Name,
		CommonPrefixes:        response.CommonPrefixes,
		Contents:              response.Contents,
		ContinuationToken:     continuationToken,
		Delimiter:             response.Delimiter,
		IsTruncated:           response.IsTruncated,
		KeyCount:              len(response.Contents),
		MaxKeys:               response.MaxKeys,
		NextContinuationToken: response.NextMarker,
		Prefix:                response.Prefix,
		StartAfter:            startAfter,
	}

	writeSuccessResponseXML(w, encodeResponse(responseV2))
}

func (s3a *S3ApiServer) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html

	// collect parameters
	bucket, _ := getBucketAndObject(r)

	originalPrefix, marker, delimiter, maxKeys := getListObjectsV1Args(r.URL.Query())

	if maxKeys < 0 {
		writeErrorResponse(w, s3err.ErrInvalidMaxKeys, r.URL)
		return
	}
	if delimiter != "" && delimiter != "/" {
		writeErrorResponse(w, s3err.ErrNotImplemented, r.URL)
		return
	}

	response, err := s3a.listFilerEntries(bucket, originalPrefix, maxKeys, marker, delimiter)

	if err != nil {
		writeErrorResponse(w, s3err.ErrInternalError, r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(response))
}

func (s3a *S3ApiServer) listFilerEntries(bucket string, originalPrefix string, maxKeys int, marker string, delimiter string) (response ListBucketResult, err error) {
	// convert full path prefix into directory name and prefix for entry name
	reqDir, prefix := filepath.Split(originalPrefix)
	if strings.HasPrefix(reqDir, "/") {
		reqDir = reqDir[1:]
	}
	bucketPrefix := fmt.Sprintf("%s/%s/", s3a.option.BucketsPath, bucket)
	reqDir = fmt.Sprintf("%s%s", bucketPrefix, reqDir)
	if strings.HasSuffix(reqDir, "/") {
		// remove trailing "/"
		reqDir = reqDir[:len(reqDir)-1]
	}

	var contents []ListEntry
	var commonPrefixes []PrefixEntry
	var isTruncated bool
	var doErr error
	var nextMarker string

	// check filer
	err = s3a.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		_, isTruncated, nextMarker, doErr = s3a.doListFilerEntries(client, reqDir, prefix, maxKeys, marker, delimiter, func(dir string, entry *filer_pb.Entry) {
			if entry.IsDirectory {
				if delimiter == "/" {
					commonPrefixes = append(commonPrefixes, PrefixEntry{
						Prefix: fmt.Sprintf("%s/%s/", dir, entry.Name)[len(bucketPrefix):],
					})
				}
			} else {
				storageClass := "STANDARD"
				if v, ok := entry.Extended[xhttp.AmzStorageClass]; ok {
					storageClass = string(v)
				}
				contents = append(contents, ListEntry{
					Key:          fmt.Sprintf("%s/%s", dir, entry.Name)[len(bucketPrefix):],
					LastModified: time.Unix(entry.Attributes.Mtime, 0).UTC(),
					ETag:         "\"" + filer.ETag(entry) + "\"",
					Size:         int64(filer.FileSize(entry)),
					Owner: CanonicalUser{
						ID:          fmt.Sprintf("%x", entry.Attributes.Uid),
						DisplayName: entry.Attributes.UserName,
					},
					StorageClass: StorageClass(storageClass),
				})
			}
		})
		if doErr != nil {
			return doErr
		}

		if !isTruncated {
			nextMarker = ""
		}

		response = ListBucketResult{
			Name:           bucket,
			Prefix:         originalPrefix,
			Marker:         marker,
			NextMarker:     nextMarker,
			MaxKeys:        maxKeys,
			Delimiter:      delimiter,
			IsTruncated:    isTruncated,
			Contents:       contents,
			CommonPrefixes: commonPrefixes,
		}

		return nil
	})

	return
}

func (s3a *S3ApiServer) doListFilerEntries(client filer_pb.SeaweedFilerClient, dir, prefix string, maxKeys int, marker, delimiter string, eachEntryFn func(dir string, entry *filer_pb.Entry)) (counter int, isTruncated bool, nextMarker string, err error) {
	// invariants
	//   prefix and marker should be under dir, marker may contain "/"
	//   maxKeys should be updated for each recursion

	if prefix == "/" && delimiter == "/" {
		return
	}
	if maxKeys <= 0 {
		return
	}

	if strings.Contains(marker, "/") {
		sepIndex := strings.Index(marker, "/")
		subDir, subMarker := marker[0:sepIndex], marker[sepIndex+1:]
		// println("doListFilerEntries dir", dir+"/"+subDir, "subMarker", subMarker, "maxKeys", maxKeys)
		subCounter, subIsTruncated, subNextMarker, subErr := s3a.doListFilerEntries(client, dir+"/"+subDir, "", maxKeys, subMarker, delimiter, eachEntryFn)
		if subErr != nil {
			err = subErr
			return
		}
		isTruncated = isTruncated || subIsTruncated
		maxKeys -= subCounter
		nextMarker = subDir + "/" + subNextMarker
		counter += subCounter
		// finished processing this sub directory
		marker = subDir
	}

	// now marker is also a direct child of dir
	request := &filer_pb.ListEntriesRequest{
		Directory:          dir,
		Prefix:             prefix,
		Limit:              uint32(maxKeys + 1),
		StartFromFileName:  marker,
		InclusiveStartFrom: false,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, listErr := client.ListEntries(ctx, request)
	if listErr != nil {
		err = fmt.Errorf("list entires %+v: %v", request, listErr)
		return
	}

	for {
		resp, recvErr := stream.Recv()
		if recvErr != nil {
			if recvErr == io.EOF {
				break
			} else {
				err = fmt.Errorf("iterating entires %+v: %v", request, recvErr)
				return
			}
		}
		if counter >= maxKeys {
			isTruncated = true
			return
		}
		entry := resp.Entry
		nextMarker = entry.Name
		if entry.IsDirectory {
			// println("ListEntries", dir, "dir:", entry.Name)
			if entry.Name != ".uploads" { // FIXME no need to apply to all directories. this extra also affects maxKeys
				eachEntryFn(dir, entry)
				if delimiter != "/" {
					// println("doListFilerEntries2 dir", dir+"/"+entry.Name, "maxKeys", maxKeys-counter)
					subCounter, subIsTruncated, subNextMarker, subErr := s3a.doListFilerEntries(client, dir+"/"+entry.Name, "", maxKeys-counter, "", delimiter, eachEntryFn)
					if subErr != nil {
						err = fmt.Errorf("doListFilerEntries2: %v", subErr)
						return
					}
					// println("doListFilerEntries2 dir", dir+"/"+entry.Name, "maxKeys", maxKeys-counter, "subCounter", subCounter, "subNextMarker", subNextMarker, "subIsTruncated", subIsTruncated)
					counter += subCounter
					nextMarker = entry.Name + "/" + subNextMarker
					if subIsTruncated {
						isTruncated = true
						return
					}
				} else {
					counter++
				}
			}
		} else {
			// println("ListEntries", dir, "file:", entry.Name)
			eachEntryFn(dir, entry)
			counter++
		}
	}
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
