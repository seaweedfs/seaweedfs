package s3api

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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

	originalPrefix, startAfter, delimiter, continuationToken, encodingTypeUrl, fetchOwner, maxKeys, allowUnordered, errCode := getListObjectsV2Args(r.URL.Query())

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// maxKeys is uint16 here; negative values are rejected during parsing.

	// AWS S3 compatibility: allow-unordered cannot be used with delimiter
	if allowUnordered && delimiter != "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidUnorderedWithDelimiter)
		return
	}

	marker := continuationToken.string
	if !continuationToken.set {
		marker = startAfter
	}

	// Adjust marker if it ends with delimiter to skip all entries with that prefix
	marker = adjustMarkerForDelimiter(marker, delimiter)

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

	originalPrefix, marker, delimiter, encodingTypeUrl, maxKeys, allowUnordered, errCode := getListObjectsV1Args(r.URL.Query())

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	if maxKeys < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxKeys)
		return
	}

	// AWS S3 compatibility: allow-unordered cannot be used with delimiter
	if allowUnordered && delimiter != "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidUnorderedWithDelimiter)
		return
	}

	// Adjust marker if it ends with delimiter to skip all entries with that prefix
	marker = adjustMarkerForDelimiter(marker, delimiter)

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

	// Special case: when maxKeys = 0, return empty results immediately with IsTruncated=false
	if maxKeys == 0 {
		response = ListBucketResult{
			Name:           bucket,
			Prefix:         originalPrefix,
			Marker:         originalMarker,
			NextMarker:     "",
			MaxKeys:        int(maxKeys),
			Delimiter:      delimiter,
			IsTruncated:    false,
			Contents:       contents,
			CommonPrefixes: commonPrefixes,
		}
		if encodingTypeUrl {
			response.EncodingType = s3.EncodingTypeUrl
		}
		return
	}

	// check filer
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		var lastEntryWasCommonPrefix bool
		var lastCommonPrefixName string

		for {
			empty := true

			nextMarker, doErr = s3a.doListFilerEntries(client, reqDir, prefix, cursor, marker, delimiter, false, func(dir string, entry *filer_pb.Entry) {
				empty = false
				dirName, entryName, _ := entryUrlEncode(dir, entry.Name, encodingTypeUrl)
				if entry.IsDirectory {
					// When delimiter is specified, apply delimiter logic to directory key objects too
					if delimiter != "" && entry.IsDirectoryKeyObject() {
						// Apply the same delimiter logic as for regular files
						var delimiterFound bool
						// Use raw dir and entry.Name (not encoded) to ensure consistent handling
						// Encoding will be applied after sorting if encodingTypeUrl is set
						undelimitedPath := fmt.Sprintf("%s/%s/", dir, entry.Name)[len(bucketPrefix):]

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
								lastEntryWasCommonPrefix = true
								lastCommonPrefixName = delimitedPath[0]
							} else {
								// This directory object belongs to an existing CommonPrefix, skip it
								delimiterFound = true
							}
						}

						// If no delimiter found in the directory object name, treat it as a regular key
						if !delimiterFound {
							contents = append(contents, newListEntry(entry, "", dirName, entryName, bucketPrefix, fetchOwner, true, false, s3a.iam))
							cursor.maxKeys--
							lastEntryWasCommonPrefix = false
						}
					} else if entry.IsDirectoryKeyObject() {
						// No delimiter specified, or delimiter doesn't apply - treat as regular key
						contents = append(contents, newListEntry(entry, "", dirName, entryName, bucketPrefix, fetchOwner, true, false, s3a.iam))
						cursor.maxKeys--
						lastEntryWasCommonPrefix = false
						// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
					} else if delimiter == "/" { // A response can contain CommonPrefixes only if you specify a delimiter.
						// Use raw dir and entry.Name (not encoded) to ensure consistent handling
						// Encoding will be applied after sorting if encodingTypeUrl is set
						commonPrefixes = append(commonPrefixes, PrefixEntry{
							Prefix: fmt.Sprintf("%s/%s/", dir, entry.Name)[len(bucketPrefix):],
						})
						//All of the keys (up to 1,000) rolled up into a common prefix count as a single return when calculating the number of returns.
						cursor.maxKeys--
						lastEntryWasCommonPrefix = true
						lastCommonPrefixName = entry.Name
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
								lastEntryWasCommonPrefix = true
								lastCommonPrefixName = delimitedPath[0]
							} else {
								// This object belongs to an existing CommonPrefix, skip it
								// but continue processing to maintain correct flow
								delimiterFound = true
							}
						}
					}
					if !delimiterFound {
						contents = append(contents, newListEntry(entry, "", dirName, entryName, bucketPrefix, fetchOwner, false, false, s3a.iam))
						cursor.maxKeys--
						lastEntryWasCommonPrefix = false
					}
				}
			})
			if doErr != nil {
				return doErr
			}

			// Adjust nextMarker for CommonPrefixes to include trailing slash (AWS S3 compliance)
			if cursor.isTruncated && lastEntryWasCommonPrefix && lastCommonPrefixName != "" {
				// For CommonPrefixes, NextMarker should include the trailing slash
				if requestDir != "" {
					nextMarker = requestDir + "/" + lastCommonPrefixName + "/"
				} else {
					nextMarker = lastCommonPrefixName + "/"
				}
			} else if cursor.isTruncated {
				if requestDir != "" {
					nextMarker = requestDir + "/" + nextMarker
				}
			}

			if cursor.isTruncated {
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
		// Sort CommonPrefixes to match AWS S3 behavior
		// AWS S3 treats the delimiter character specially for sorting common prefixes.
		// For example, with delimiter '/', 'foo/' should come before 'foo+1/' even though '+' (ASCII 43) < '/' (ASCII 47).
		// This custom comparison ensures correct S3-compatible lexicographical ordering.
		sort.Slice(response.CommonPrefixes, func(i, j int) bool {
			return compareWithDelimiter(response.CommonPrefixes[i].Prefix, response.CommonPrefixes[j].Prefix, delimiter)
		})

		// URL-encode CommonPrefixes AFTER sorting (if EncodingType=url)
		// This ensures proper sort order (on decoded values) and correct encoding in response
		if encodingTypeUrl {
			response.EncodingType = s3.EncodingTypeUrl
			for i := range response.CommonPrefixes {
				response.CommonPrefixes[i].Prefix = urlPathEscape(response.CommonPrefixes[i].Prefix)
			}
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
	// When listing at bucket root with delimiter '/', prefix can be "/" after normalization.
	// Returning early here would incorrectly hide all top-level entries (folders like "Veeam/").
	if cursor.maxKeys <= 0 {
		return // Don't set isTruncated here - let caller decide based on whether more entries exist
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

	// Track .versions directories found in this directory for later processing
	// Store the full entry to avoid additional getEntry calls (N+1 query optimization)
	var versionsDirs []*filer_pb.Entry

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
		entry := resp.Entry

		if cursor.maxKeys <= 0 {
			cursor.isTruncated = true
			continue
		}

		// Set nextMarker only when we have quota to process this entry
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

			// Skip .versions directories in regular list operations but track them for logical object creation
			// Store the full entry to avoid additional getEntry calls later
			if strings.HasSuffix(entry.Name, s3_constants.VersionsFolder) {
				glog.V(4).Infof("Found .versions directory: %s", entry.Name)
				versionsDirs = append(versionsDirs, entry)
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
					err = fmt.Errorf("doListFilerEntries2: %w", subErr)
					return
				}
				// println("doListFilerEntries2 dir", dir+"/"+entry.Name, "subNextMarker", subNextMarker)
				nextMarker = entry.Name + "/" + subNextMarker
				if cursor.isTruncated {
					return
				}
				// println("doListFilerEntries2 nextMarker", nextMarker)
			} else {
				eachEntryFn(dir, entry)
			}
		} else {
			eachEntryFn(dir, entry)
			// glog.V(4).Infof("List File Entries %s, file: %s, maxKeys %d", dir, entry.Name, cursor.maxKeys)
		}
		if cursor.prefixEndsOnDelimiter {
			cursor.prefixEndsOnDelimiter = false
		}
	}

	// After processing all regular entries, handle versioned objects
	// Create logical entries for objects that have .versions directories
	// OPTIMIZATION: Use the already-fetched .versions directory entry to avoid N+1 queries
	for _, versionsDir := range versionsDirs {
		if cursor.maxKeys <= 0 {
			cursor.isTruncated = true
			break
		}

		// Update nextMarker to ensure pagination advances past this .versions directory
		// This is critical to prevent infinite loops when results are truncated
		nextMarker = versionsDir.Name

		// Extract object name from .versions directory name (remove .versions suffix)
		baseObjectName := strings.TrimSuffix(versionsDir.Name, s3_constants.VersionsFolder)

		// Construct full object path relative to bucket
		// dir is something like "/buckets/sea-test-1/Veeam/Backup/vbr/Config"
		// we need to get the path relative to bucket: "Veeam/Backup/vbr/Config/Owner"
		bucketPath := strings.TrimPrefix(dir, s3a.option.BucketsPath+"/")
		bucketName := strings.Split(bucketPath, "/")[0]

		// Remove bucket name from path to get directory within bucket
		bucketRelativePath := strings.Join(strings.Split(bucketPath, "/")[1:], "/")

		var fullObjectPath string
		if bucketRelativePath == "" {
			// Object is at bucket root
			fullObjectPath = baseObjectName
		} else {
			// Object is in subdirectory
			fullObjectPath = bucketRelativePath + "/" + baseObjectName
		}

		glog.V(4).Infof("Processing versioned object: baseObjectName=%s, bucketRelativePath=%s, fullObjectPath=%s",
			baseObjectName, bucketRelativePath, fullObjectPath)

		// OPTIMIZATION: Use metadata from the already-fetched .versions directory entry
		// This avoids additional getEntry calls which cause high "find" usage
		if latestVersionEntry, err := s3a.getLatestVersionEntryFromDirectoryEntry(bucketName, fullObjectPath, versionsDir); err == nil {
			glog.V(4).Infof("Creating logical entry for versioned object: %s", fullObjectPath)
			eachEntryFn(dir, latestVersionEntry)
		} else if errors.Is(err, ErrDeleteMarker) {
			// Expected: latest version is a delete marker, object should not appear in list
			glog.V(4).Infof("Skipping versioned object %s: delete marker", fullObjectPath)
		} else {
			// Unexpected failure: missing metadata, fetch error, etc.
			glog.V(3).Infof("Skipping versioned object %s due to error: %v", fullObjectPath, err)
		}
	}

	return
}

func getListObjectsV2Args(values url.Values) (prefix, startAfter, delimiter string, token OptionalString, encodingTypeUrl bool, fetchOwner bool, maxkeys uint16, allowUnordered bool, errCode s3err.ErrorCode) {
	prefix = values.Get("prefix")
	token = OptionalString{set: values.Has("continuation-token"), string: values.Get("continuation-token")}
	startAfter = values.Get("start-after")
	delimiter = values.Get("delimiter")
	encodingTypeUrl = values.Get("encoding-type") == s3.EncodingTypeUrl
	if values.Get("max-keys") != "" {
		if maxKeys, err := strconv.ParseUint(values.Get("max-keys"), 10, 16); err == nil {
			maxkeys = uint16(maxKeys)
		} else {
			// Invalid max-keys value (non-numeric)
			errCode = s3err.ErrInvalidMaxKeys
			return
		}
	} else {
		maxkeys = maxObjectListSizeLimit
	}
	fetchOwner = values.Get("fetch-owner") == "true"
	allowUnordered = values.Get("allow-unordered") == "true"
	errCode = s3err.ErrNone
	return
}

func getListObjectsV1Args(values url.Values) (prefix, marker, delimiter string, encodingTypeUrl bool, maxkeys int16, allowUnordered bool, errCode s3err.ErrorCode) {
	prefix = values.Get("prefix")
	marker = values.Get("marker")
	delimiter = values.Get("delimiter")
	encodingTypeUrl = values.Get("encoding-type") == "url"
	if values.Get("max-keys") != "" {
		if maxKeys, err := strconv.ParseInt(values.Get("max-keys"), 10, 16); err == nil {
			maxkeys = int16(maxKeys)
		} else {
			// Invalid max-keys value (non-numeric)
			errCode = s3err.ErrInvalidMaxKeys
			return
		}
	} else {
		maxkeys = maxObjectListSizeLimit
	}
	allowUnordered = values.Get("allow-unordered") == "true"
	errCode = s3err.ErrNone
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

// compareWithDelimiter compares two strings for sorting, treating the delimiter character
// as having lower precedence than other characters to match AWS S3 behavior.
// For example, with delimiter '/', 'foo/' should come before 'foo+1/' even though '+' < '/' in ASCII.
// Note: This function assumes delimiter is a single character. Multi-character delimiters will fall back to standard comparison.
func compareWithDelimiter(a, b, delimiter string) bool {
	if delimiter == "" {
		return a < b
	}

	// Multi-character delimiters are not supported by AWS S3 in practice,
	// but if encountered, fall back to standard byte-wise comparison
	if len(delimiter) != 1 {
		return a < b
	}

	delimByte := delimiter[0]
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	// Compare character by character
	for i := 0; i < minLen; i++ {
		charA := a[i]
		charB := b[i]

		if charA == charB {
			continue
		}

		// Check if either character is the delimiter
		isDelimA := charA == delimByte
		isDelimB := charB == delimByte

		if isDelimA && !isDelimB {
			// Delimiter in 'a' should come first
			return true
		}
		if !isDelimA && isDelimB {
			// Delimiter in 'b' should come first
			return false
		}

		// Neither or both are delimiters, use normal comparison
		return charA < charB
	}

	// If we get here, one string is a prefix of the other
	return len(a) < len(b)
}

// adjustMarkerForDelimiter handles delimiter-ending markers by incrementing them to skip entries with that prefix.
// For example, when continuation token is "boo/", this returns "boo~" to skip all "boo/*" entries
// but still finds any "bop" or later entries. We add a high ASCII character rather than incrementing
// the last character to avoid skipping potential directory entries.
// This is essential for correct S3 list operations with delimiters and CommonPrefixes.
func adjustMarkerForDelimiter(marker, delimiter string) string {
	if delimiter == "" || !strings.HasSuffix(marker, delimiter) {
		return marker
	}

	// Remove the trailing delimiter and append a high ASCII character
	// This ensures we skip all entries under the prefix but don't skip
	// potential directory entries that start with a similar prefix
	prefix := strings.TrimSuffix(marker, delimiter)
	if len(prefix) == 0 {
		return marker
	}

	// Use tilde (~) which has ASCII value 126, higher than most printable characters
	// This skips "prefix/*" entries but still finds "prefix" + any higher character
	return prefix + "~"
}
