package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

const cutoffTimeNewEmptyDir = 3

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
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("ListObjectsV2Handler %s", bucket)

	originalPrefix, continuationToken, startAfter, delimiter, _, maxKeys := getListObjectsV2Args(r.URL.Query())

	if maxKeys < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxKeys)
		return
	}
	if delimiter != "" && delimiter != "/" {
		s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
		return
	}

	marker := continuationToken
	if continuationToken == "" {
		marker = startAfter
	}

	response, err := s3a.listFilerEntries(bucket, originalPrefix, maxKeys, marker, delimiter)

	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if len(response.Contents) == 0 {
		if exists, existErr := s3a.exists(s3a.option.BucketsPath, bucket, true); existErr == nil && !exists {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
	}

	responseV2 := &ListBucketResultV2{
		XMLName:               response.XMLName,
		Name:                  response.Name,
		CommonPrefixes:        response.CommonPrefixes,
		Contents:              response.Contents,
		ContinuationToken:     continuationToken,
		Delimiter:             response.Delimiter,
		IsTruncated:           response.IsTruncated,
		KeyCount:              len(response.Contents) + len(response.CommonPrefixes),
		MaxKeys:               response.MaxKeys,
		NextContinuationToken: response.NextMarker,
		Prefix:                response.Prefix,
		StartAfter:            startAfter,
	}

	writeSuccessResponseXML(w, r, responseV2)
}

func (s3a *S3ApiServer) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html

	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("ListObjectsV1Handler %s", bucket)

	originalPrefix, marker, delimiter, maxKeys := getListObjectsV1Args(r.URL.Query())

	if maxKeys < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxKeys)
		return
	}
	if delimiter != "" && delimiter != "/" {
		s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
		return
	}

	response, err := s3a.listFilerEntries(bucket, originalPrefix, maxKeys, marker, delimiter)

	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if len(response.Contents) == 0 {
		if exists, existErr := s3a.exists(s3a.option.BucketsPath, bucket, true); existErr == nil && !exists {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
	}

	writeSuccessResponseXML(w, r, response)
}

func (s3a *S3ApiServer) listFilerEntries(bucket string, originalPrefix string, maxKeys int, originalMarker string, delimiter string) (response ListBucketResult, err error) {
	// convert full path prefix into directory name and prefix for entry name
	requestDir, prefix, marker := normalizePrefixMarker(originalPrefix, originalMarker)
	bucketPrefix := fmt.Sprintf("%s/%s/", s3a.option.BucketsPath, bucket)
	reqDir := bucketPrefix[:len(bucketPrefix)-1]
	if requestDir != "" {
		reqDir = fmt.Sprintf("%s%s", bucketPrefix, requestDir)
	}

	var contents []ListEntry
	var commonPrefixes []PrefixEntry
	var doErr error
	var nextMarker string
	cursor := &ListingCursor{
		maxKeys: maxKeys,
	}

	// check filer
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for {
			empty := true
			nextMarker, doErr = s3a.doListFilerEntries(client, reqDir, prefix, cursor, marker, delimiter, false, func(dir string, entry *filer_pb.Entry) {
				empty = false
				if entry.IsDirectory {
					// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
					if delimiter == "/" { // A response can contain CommonPrefixes only if you specify a delimiter.
						commonPrefixes = append(commonPrefixes, PrefixEntry{
							Prefix: fmt.Sprintf("%s/%s/", dir, entry.Name)[len(bucketPrefix):],
						})
						//All of the keys (up to 1,000) rolled up into a common prefix count as a single return when calculating the number of returns.
						cursor.maxKeys--
					}
					// For s3, one url can be both a file and a directory (actually directory is just a logical concept)
					if entry.IsDirectoryKeyObject() {
						storageClass := "STANDARD"
						if v, ok := entry.Extended[s3_constants.AmzStorageClass]; ok {
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
						cursor.maxKeys--
					}
				} else {
					storageClass := "STANDARD"
					if v, ok := entry.Extended[s3_constants.AmzStorageClass]; ok {
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
					cursor.maxKeys--
				}
			})
			if doErr != nil {
				return doErr
			}

			if cursor.isTruncated {
				if requestDir != "" {
					nextMarker = requestDir + "/" + nextMarker
				}
				break
			} else if empty {
				nextMarker = ""
				break
			} else {
				// start next loop
				marker = nextMarker
			}
		}

		response = ListBucketResult{
			Name:           bucket,
			Prefix:         originalPrefix,
			Marker:         originalMarker,
			NextMarker:     nextMarker,
			MaxKeys:        maxKeys,
			Delimiter:      delimiter,
			IsTruncated:    cursor.isTruncated,
			Contents:       contents,
			CommonPrefixes: commonPrefixes,
		}

		return nil
	})

	return
}

type ListingCursor struct {
	maxKeys     int
	isTruncated bool
}

// the prefix and marker may be in different directories
// normalizePrefixMarker ensures the prefix and marker both starts from the same directory
func normalizePrefixMarker(prefix, marker string) (alignedDir, alignedPrefix, alignedMarker string) {
	// alignedDir should not end with "/"
	// alignedDir, alignedPrefix, alignedMarker should only have "/" in middle
	prefix = strings.TrimLeft(prefix, "/")
	marker = strings.TrimLeft(marker, "/")
	if prefix == "" {
		return "", "", marker
	}
	if marker == "" {
		alignedDir, alignedPrefix = toDirAndName(prefix)
		return
	}
	if !strings.HasPrefix(marker, prefix) {
		// something wrong
		return "", prefix, marker
	}
	if strings.HasPrefix(marker, prefix+"/") {
		alignedDir = prefix
		alignedPrefix = ""
		alignedMarker = marker[len(alignedDir)+1:]
		return
	}

	alignedDir, alignedPrefix = toDirAndName(prefix)
	if alignedDir != "" {
		alignedMarker = marker[len(alignedDir)+1:]
	} else {
		alignedMarker = marker
	}
	return
}
func toDirAndName(dirAndName string) (dir, name string) {
	sepIndex := strings.LastIndex(dirAndName, "/")
	if sepIndex >= 0 {
		dir, name = dirAndName[0:sepIndex], dirAndName[sepIndex+1:]
	} else {
		name = dirAndName
	}
	return
}
func toParentAndDescendants(dirAndName string) (dir, name string) {
	sepIndex := strings.Index(dirAndName, "/")
	if sepIndex >= 0 {
		dir, name = dirAndName[0:sepIndex], dirAndName[sepIndex+1:]
	} else {
		name = dirAndName
	}
	return
}

func (s3a *S3ApiServer) doListFilerEntries(client filer_pb.SeaweedFilerClient, dir, prefix string, cursor *ListingCursor, marker, delimiter string, inclusiveStartFrom bool, eachEntryFn func(dir string, entry *filer_pb.Entry)) (nextMarker string, err error) {
	// invariants
	//   prefix and marker should be under dir, marker may contain "/"
	//   maxKeys should be updated for each recursion

	if prefix == "/" && delimiter == "/" {
		return
	}
	if cursor.maxKeys <= 0 {
		return
	}

	if strings.Contains(marker, "/") {
		subDir, subMarker := toParentAndDescendants(marker)
		// println("doListFilerEntries dir", dir+"/"+subDir, "subMarker", subMarker)
		subNextMarker, subErr := s3a.doListFilerEntries(client, dir+"/"+subDir, "", cursor, subMarker, delimiter, false, eachEntryFn)
		if subErr != nil {
			err = subErr
			return
		}
		nextMarker = subDir + "/" + subNextMarker
		// finished processing this sub directory
		marker = subDir
	}
	if cursor.isTruncated {
		return
	}

	// now marker is also a direct child of dir
	request := &filer_pb.ListEntriesRequest{
		Directory:          dir,
		Prefix:             prefix,
		Limit:              uint32(cursor.maxKeys + 2), // bucket root directory needs to skip additional s3_constants.MultipartUploadsFolder folder
		StartFromFileName:  marker,
		InclusiveStartFrom: inclusiveStartFrom,
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
		if cursor.maxKeys <= 0 {
			cursor.isTruncated = true
			return
		}
		entry := resp.Entry
		nextMarker = entry.Name
		if entry.IsDirectory {
			// println("ListEntries", dir, "dir:", entry.Name)
			if entry.Name == s3_constants.MultipartUploadsFolder { // FIXME no need to apply to all directories. this extra also affects maxKeys
				continue
			}
			if delimiter != "/" {
				eachEntryFn(dir, entry)
				subNextMarker, subErr := s3a.doListFilerEntries(client, dir+"/"+entry.Name, "", cursor, "", delimiter, false, eachEntryFn)
				if subErr != nil {
					err = fmt.Errorf("doListFilerEntries2: %v", subErr)
					return
				}
				// println("doListFilerEntries2 dir", dir+"/"+entry.Name, "subNextMarker", subNextMarker)
				nextMarker = entry.Name + "/" + subNextMarker
				if cursor.isTruncated {
					return
				}
				// println("doListFilerEntries2 nextMarker", nextMarker)
			} else {
				var isEmpty bool
				if !s3a.option.AllowEmptyFolder && !entry.IsDirectoryKeyObject() {
					if isEmpty, err = s3a.ensureDirectoryAllEmpty(client, dir, entry.Name); err != nil {
						glog.Errorf("check empty folder %s: %v", dir, err)
					}
				}
				if !isEmpty {
					eachEntryFn(dir, entry)
				}
			}
		} else {
			eachEntryFn(dir, entry)
			// println("ListEntries", dir, "file:", entry.Name, "maxKeys", cursor.maxKeys)
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

func (s3a *S3ApiServer) ensureDirectoryAllEmpty(filerClient filer_pb.SeaweedFilerClient, parentDir, name string) (isEmpty bool, err error) {
	// println("+ ensureDirectoryAllEmpty", dir, name)
	glog.V(4).Infof("+ isEmpty %s/%s", parentDir, name)
	defer glog.V(4).Infof("- isEmpty %s/%s %v", parentDir, name, isEmpty)
	var fileCounter int
	var subDirs []string
	currentDir := parentDir + "/" + name
	var startFrom string
	var isExhausted bool
	var foundEntry bool
	cutOffTimeAtSec := time.Now().Unix() + cutoffTimeNewEmptyDir
	for fileCounter == 0 && !isExhausted && err == nil {
		err = filer_pb.SeaweedList(filerClient, currentDir, "", func(entry *filer_pb.Entry, isLast bool) error {
			foundEntry = true
			if entry.IsDirectory {
				if entry.Attributes != nil && cutOffTimeAtSec >= entry.Attributes.GetCrtime() {
					fileCounter++
				} else {
					subDirs = append(subDirs, entry.Name)
				}
			} else {
				fileCounter++
			}
			startFrom = entry.Name
			isExhausted = isExhausted || isLast
			glog.V(4).Infof("    * %s/%s isLast: %t", currentDir, startFrom, isLast)
			return nil
		}, startFrom, false, 8)
		if !foundEntry {
			break
		}
	}

	if err != nil {
		return false, err
	}

	if fileCounter > 0 {
		return false, nil
	}

	for _, subDir := range subDirs {
		isSubEmpty, subErr := s3a.ensureDirectoryAllEmpty(filerClient, currentDir, subDir)
		if subErr != nil {
			return false, subErr
		}
		if !isSubEmpty {
			return false, nil
		}
	}

	glog.V(1).Infof("deleting empty folder %s", currentDir)
	if err = doDeleteEntry(filerClient, parentDir, name, true, true); err != nil {
		return
	}

	return true, nil
}
