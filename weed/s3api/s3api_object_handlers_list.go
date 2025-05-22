package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type OptionalString struct {
	string
	set bool
}

func (o OptionalString) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if !o.set {
		return nil
	}
	return e.EncodeElement(o.string, startElement)
}

type ListBucketResultV2 struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	MaxKeys               uint16         `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []ListEntry    `xml:"Contents,omitempty"`
	CommonPrefixes        []PrefixEntry  `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     OptionalString `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	KeyCount              int            `xml:"KeyCount"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
}

func (s3a *S3ApiServer) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/v2-RESTBucketGET.html

	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("ListObjectsV2Handler %s", bucket)

	originalPrefix, startAfter, delimiter, continuationToken, encodingTypeUrl, fetchOwner, maxKeys := getListObjectsV2Args(r.URL.Query())

	if maxKeys < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxKeys)
		return
	}

	marker := continuationToken.string
	if !continuationToken.set {
		marker = startAfter
	}

	response, err := s3a.listFilerEntries(bucket, originalPrefix, maxKeys, marker, delimiter, encodingTypeUrl, fetchOwner)

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
		Name:                  response.Name,
		CommonPrefixes:        response.CommonPrefixes,
		Contents:              response.Contents,
		ContinuationToken:     continuationToken,
		Delimiter:             response.Delimiter,
		IsTruncated:           response.IsTruncated,
		KeyCount:              len(response.Contents) + len(response.CommonPrefixes),
		MaxKeys:               uint16(response.MaxKeys),
		NextContinuationToken: response.NextMarker,
		Prefix:                response.Prefix,
		StartAfter:            startAfter,
	}
	if encodingTypeUrl {
		responseV2.EncodingType = s3.EncodingTypeUrl
	}

	writeSuccessResponseXML(w, r, responseV2)
}

func (s3a *S3ApiServer) ListObjectsV1Handler(w http.ResponseWriter, r *http.Request) {

	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html

	// collect parameters
	bucket, _ := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("ListObjectsV1Handler %s", bucket)

	originalPrefix, marker, delimiter, encodingTypeUrl, maxKeys := getListObjectsV1Args(r.URL.Query())

	if maxKeys < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxKeys)
		return
	}
	response, err := s3a.listFilerEntries(bucket, originalPrefix, uint16(maxKeys), marker, delimiter, encodingTypeUrl, true)

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

func (s3a *S3ApiServer) listFilerEntries(bucket string, originalPrefix string, maxKeys uint16, originalMarker string, delimiter string, encodingTypeUrl bool, fetchOwner bool) (response ListBucketResult, err error) {
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
		maxKeys:               maxKeys,
		prefixEndsOnDelimiter: strings.HasSuffix(originalPrefix, "/") && len(originalMarker) == 0,
	}

	// check filer
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		for {
			empty := true
			nextMarker, doErr = s3a.doListFilerEntries(client, reqDir, prefix, cursor, marker, delimiter, false, func(dir string, entry *filer_pb.Entry) {
				empty = false
				dirName, entryName, prefixName := entryUrlEncode(dir, entry.Name, encodingTypeUrl)
				if entry.IsDirectory {
					if entry.IsDirectoryKeyObject() {
						contents = append(contents, newListEntry(entry, "", dirName, entryName, bucketPrefix, fetchOwner, true, false))
						cursor.maxKeys--
						// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
					} else if delimiter == "/" { // A response can contain CommonPrefixes only if you specify a delimiter.
						commonPrefixes = append(commonPrefixes, PrefixEntry{
							Prefix: fmt.Sprintf("%s/%s/", dirName, prefixName)[len(bucketPrefix):],
						})
						//All of the keys (up to 1,000) rolled up into a common prefix count as a single return when calculating the number of returns.
						cursor.maxKeys--
					}
				} else {
					var delimiterFound bool
					if delimiter != "" {
						// keys that contain the same string between the prefix and the first occurrence of the delimiter are grouped together as a commonPrefix.
						// extract the string between the prefix and the delimiter and add it to the commonPrefixes if it's unique.
						undelimitedPath := fmt.Sprintf("%s/%s", dir, entry.Name)[len(bucketPrefix):]

						// take into account a prefix if supplied while delimiting.
						undelimitedPath = strings.TrimPrefix(undelimitedPath, originalPrefix)

						delimitedPath := strings.SplitN(undelimitedPath, delimiter, 2)

						if len(delimitedPath) == 2 {
							// S3 clients expect the delimited prefix to contain the delimiter and prefix.
							delimitedPrefix := originalPrefix + delimitedPath[0] + delimiter

							for i := range commonPrefixes {
								if commonPrefixes[i].Prefix == delimitedPrefix {
									delimiterFound = true
									break
								}
							}

							if !delimiterFound {
								commonPrefixes = append(commonPrefixes, PrefixEntry{
									Prefix: delimitedPrefix,
								})
								cursor.maxKeys--
								delimiterFound = true
							}
						}
					}
					if !delimiterFound {
						contents = append(contents, newListEntry(entry, "", dirName, entryName, bucketPrefix, fetchOwner, false, false))
						cursor.maxKeys--
					}
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
			} else if empty || strings.HasSuffix(originalPrefix, "/") {
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
			MaxKeys:        int(maxKeys),
			Delimiter:      delimiter,
			IsTruncated:    cursor.isTruncated,
			Contents:       contents,
			CommonPrefixes: commonPrefixes,
		}
		if encodingTypeUrl {
			// Todo used for pass test_bucket_listv2_encoding_basic
			// sort.Slice(response.CommonPrefixes, func(i, j int) bool { return response.CommonPrefixes[i].Prefix < response.CommonPrefixes[j].Prefix })
			response.EncodingType = s3.EncodingTypeUrl
		}
		return nil
	})

	return
}

type ListingCursor struct {
	maxKeys               uint16
	isTruncated           bool
	prefixEndsOnDelimiter bool
}

// the prefix and marker may be in different directories
// normalizePrefixMarker ensures the prefix and marker both starts from the same directory
func normalizePrefixMarker(prefix, marker string) (alignedDir, alignedPrefix, alignedMarker string) {
	// alignedDir should not end with "/"
	// alignedDir, alignedPrefix, alignedMarker should only have "/" in middle
	if len(marker) == 0 {
		prefix = strings.Trim(prefix, "/")
	} else {
		prefix = strings.TrimLeft(prefix, "/")
	}
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
	// glog.V(4).Infof("doListFilerEntries dir: %s, prefix: %s, marker %s, maxKeys: %d, prefixEndsOnDelimiter: %+v", dir, prefix, marker, cursor.maxKeys, cursor.prefixEndsOnDelimiter)
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
		// finished processing this subdirectory
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
	if cursor.prefixEndsOnDelimiter {
		request.Limit = uint32(1)
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
			continue
		}
		entry := resp.Entry
		nextMarker = entry.Name
		if cursor.prefixEndsOnDelimiter {
			if entry.Name == prefix && entry.IsDirectory {
				if delimiter != "/" {
					cursor.prefixEndsOnDelimiter = false
				}
			} else {
				continue
			}
		}
		if entry.IsDirectory {
			// glog.V(4).Infof("List Dir Entries %s, file: %s, maxKeys %d", dir, entry.Name, cursor.maxKeys)
			if entry.Name == s3_constants.MultipartUploadsFolder { // FIXME no need to apply to all directories. this extra also affects maxKeys
				continue
			}
			if delimiter != "/" || cursor.prefixEndsOnDelimiter {
				if cursor.prefixEndsOnDelimiter {
					cursor.prefixEndsOnDelimiter = false
					if entry.IsDirectoryKeyObject() {
						eachEntryFn(dir, entry)
					}
				} else {
					eachEntryFn(dir, entry)
				}
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
				if !s3a.option.AllowEmptyFolder && entry.IsOlderDir() {
					//if isEmpty, err = s3a.ensureDirectoryAllEmpty(client, dir, entry.Name); err != nil {
					//	glog.Errorf("check empty folder %s: %v", dir, err)
					//}
				}
				if !isEmpty {
					eachEntryFn(dir, entry)
				}
			}
		} else {
			eachEntryFn(dir, entry)
			// glog.V(4).Infof("List File Entries %s, file: %s, maxKeys %d", dir, entry.Name, cursor.maxKeys)
		}
		if cursor.prefixEndsOnDelimiter {
			cursor.prefixEndsOnDelimiter = false
		}
	}
	return
}

func getListObjectsV2Args(values url.Values) (prefix, startAfter, delimiter string, token OptionalString, encodingTypeUrl bool, fetchOwner bool, maxkeys uint16) {
	prefix = values.Get("prefix")
	token = OptionalString{set: values.Has("continuation-token"), string: values.Get("continuation-token")}
	startAfter = values.Get("start-after")
	delimiter = values.Get("delimiter")
	encodingTypeUrl = values.Get("encoding-type") == s3.EncodingTypeUrl
	if values.Get("max-keys") != "" {
		if maxKeys, err := strconv.ParseUint(values.Get("max-keys"), 10, 16); err == nil {
			maxkeys = uint16(maxKeys)
		}
	} else {
		maxkeys = maxObjectListSizeLimit
	}
	fetchOwner = values.Get("fetch-owner") == "true"
	return
}

func getListObjectsV1Args(values url.Values) (prefix, marker, delimiter string, encodingTypeUrl bool, maxkeys int16) {
	prefix = values.Get("prefix")
	marker = values.Get("marker")
	delimiter = values.Get("delimiter")
	encodingTypeUrl = values.Get("encoding-type") == "url"
	if values.Get("max-keys") != "" {
		if maxKeys, err := strconv.ParseInt(values.Get("max-keys"), 10, 16); err == nil {
			maxkeys = int16(maxKeys)
		}
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
	for fileCounter == 0 && !isExhausted && err == nil {
		err = filer_pb.SeaweedList(context.Background(), filerClient, currentDir, "", func(entry *filer_pb.Entry, isLast bool) error {
			foundEntry = true
			if entry.IsOlderDir() {
				subDirs = append(subDirs, entry.Name)
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
	if err = doDeleteEntry(filerClient, parentDir, name, true, false); err != nil {
		return
	}

	return true, nil
}
