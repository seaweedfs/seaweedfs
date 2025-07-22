package s3api

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

// CustomListEntry handles the Owner field properly for fetchOwner parameter
// When fetchOwner is false, Owner should be completely omitted from the XML response
type CustomListEntry struct {
	Key          string         `xml:"Key"`
	LastModified time.Time      `xml:"LastModified"`
	ETag         string         `xml:"ETag"`
	Size         int64          `xml:"Size"`
	Owner        *CanonicalUser `xml:"Owner,omitempty"`
	StorageClass StorageClass   `xml:"StorageClass"`
}

func (t *CustomListEntry) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T CustomListEntry
	var layout struct {
		*T
		LastModified *xsdDateTime `xml:"LastModified"`
	}
	layout.T = (*T)(t)
	layout.LastModified = (*xsdDateTime)(&layout.T.LastModified)
	return e.EncodeElement(layout, start)
}
func (t *CustomListEntry) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T CustomListEntry
	var overlay struct {
		*T
		LastModified *xsdDateTime `xml:"LastModified"`
	}
	overlay.T = (*T)(t)
	overlay.LastModified = (*xsdDateTime)(&overlay.T.LastModified)
	return d.DecodeElement(&overlay, &start)
}

// CustomVersionEntry handles the Owner field properly for fetchOwner parameter
type CustomVersionEntry struct {
	Key          string         `xml:"Key"`
	VersionId    string         `xml:"VersionId"`
	IsLatest     bool           `xml:"IsLatest"`
	LastModified time.Time      `xml:"LastModified"`
	ETag         string         `xml:"ETag"`
	Size         int64          `xml:"Size"`
	Owner        *CanonicalUser `xml:"Owner,omitempty"`
	StorageClass StorageClass   `xml:"StorageClass"`
}

func (t *CustomVersionEntry) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	type T CustomVersionEntry
	var layout struct {
		*T
		LastModified *xsdDateTime `xml:"LastModified"`
	}
	layout.T = (*T)(t)
	layout.LastModified = (*xsdDateTime)(&layout.T.LastModified)
	return e.EncodeElement(layout, start)
}
func (t *CustomVersionEntry) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type T CustomVersionEntry
	var overlay struct {
		*T
		LastModified *xsdDateTime `xml:"LastModified"`
	}
	overlay.T = (*T)(t)
	overlay.LastModified = (*xsdDateTime)(&overlay.T.LastModified)
	return d.DecodeElement(&overlay, &start)
}

// CustomListBucketResult uses CustomListEntry for proper Owner handling
type CustomListBucketResult struct {
	Metadata       []MetadataEntry   `xml:"Metadata,omitempty"`
	Name           string            `xml:"Name"`
	Prefix         string            `xml:"Prefix"`
	Marker         string            `xml:"Marker"`
	NextMarker     string            `xml:"NextMarker,omitempty"`
	MaxKeys        int               `xml:"MaxKeys"`
	Delimiter      string            `xml:"Delimiter,omitempty"`
	IsTruncated    bool              `xml:"IsTruncated"`
	Contents       []CustomListEntry `xml:"Contents,omitempty"`
	CommonPrefixes []PrefixEntry     `xml:"CommonPrefixes,omitempty"`
	EncodingType   string            `xml:"EncodingType"`
}

// CustomListBucketResultV2 uses CustomListEntry for proper Owner handling
type CustomListBucketResultV2 struct {
	XMLName               xml.Name          `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	MaxKeys               uint16            `xml:"MaxKeys"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	IsTruncated           bool              `xml:"IsTruncated"`
	Contents              []CustomListEntry `xml:"Contents,omitempty"`
	CommonPrefixes        []PrefixEntry     `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     OptionalString    `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
	KeyCount              int               `xml:"KeyCount"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
}

// newCustomListEntry creates a CustomListEntry with proper Owner handling
func newCustomListEntry(entry *filer_pb.Entry, key string, dir string, name string, bucketPrefix string, fetchOwner bool, isDirectory bool, encodingTypeUrl bool) (listEntry CustomListEntry) {
	storageClass := "STANDARD"
	if v, ok := entry.Extended[s3_constants.AmzStorageClass]; ok {
		storageClass = string(v)
	}
	keyFormat := "%s/%s"
	if isDirectory {
		keyFormat += "/"
	}
	if key == "" {
		key = fmt.Sprintf(keyFormat, dir, name)[len(bucketPrefix):]
	}
	if encodingTypeUrl {
		key = urlPathEscape(key)
	}
	listEntry = CustomListEntry{
		Key:          key,
		LastModified: time.Unix(entry.Attributes.Mtime, 0).UTC(),
		ETag:         "\"" + filer.ETag(entry) + "\"",
		Size:         int64(filer.FileSize(entry)),
		StorageClass: StorageClass(storageClass),
	}
	if fetchOwner {
		// Extract owner from S3 metadata (Extended attributes) instead of file system attributes
		var ownerID, displayName string
		if entry.Extended != nil {
			if ownerBytes, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
				ownerID = string(ownerBytes)
			}
		}

		// Fallback to anonymous if no S3 owner found
		if ownerID == "" {
			ownerID = s3_constants.AccountAnonymousId
			displayName = "anonymous"
		} else {
			// Use the ownerID as displayName if no better option is available
			displayName = ownerID

			// Additional fallback to file system username if available and different from ownerID
			if entry.Attributes.UserName != "" && entry.Attributes.UserName != ownerID {
				displayName = entry.Attributes.UserName
			}
		}

		listEntry.Owner = &CanonicalUser{
			ID:          ownerID,
			DisplayName: displayName,
		}
	}
	// If fetchOwner is false, Owner remains nil and will be omitted from XML
	return listEntry
}

// listFilerEntriesCustom is a version of listFilerEntries that uses CustomListEntry for proper Owner field handling
func (s3a *S3ApiServer) listFilerEntriesCustom(bucket string, originalPrefix string, maxKeys uint16, originalMarker string, delimiter string, encodingTypeUrl bool, fetchOwner bool) (response CustomListBucketResult, err error) {
	// convert full path prefix into directory name and prefix for entry name
	requestDir, prefix, marker := normalizePrefixMarker(originalPrefix, originalMarker)
	bucketPrefix := fmt.Sprintf("%s/%s/", s3a.option.BucketsPath, bucket)
	reqDir := bucketPrefix[:len(bucketPrefix)-1]
	if requestDir != "" {
		reqDir = fmt.Sprintf("%s%s", bucketPrefix, requestDir)
	}

	var contents []CustomListEntry
	var commonPrefixes []PrefixEntry
	var doErr error
	var nextMarker string
	cursor := &ListingCursor{
		maxKeys:               maxKeys,
		prefixEndsOnDelimiter: strings.HasSuffix(originalPrefix, "/") && len(originalMarker) == 0,
	}

	// Special case: when maxKeys = 0, return empty results immediately with IsTruncated=false
	if maxKeys == 0 {
		response = CustomListBucketResult{
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
				dirName, entryName, prefixName := entryUrlEncode(dir, entry.Name, encodingTypeUrl)
				if entry.IsDirectory {
					if entry.IsDirectoryKeyObject() {
						contents = append(contents, newCustomListEntry(entry, "", dirName, entryName, bucketPrefix, fetchOwner, true, false))
						cursor.maxKeys--
						lastEntryWasCommonPrefix = false
						// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
					} else if delimiter == "/" { // A response can contain CommonPrefixes only if you specify a delimiter.
						commonPrefixes = append(commonPrefixes, PrefixEntry{
							Prefix: fmt.Sprintf("%s/%s/", dirName, prefixName)[len(bucketPrefix):],
						})
						//All of the keys (up to 1,000) rolled up into a common prefix count as a single return when calculating the number of returns.
						cursor.maxKeys--
						lastEntryWasCommonPrefix = true
						lastCommonPrefixName = entry.Name
					}
				} else {
					// Handle objects that end with the delimiter when delimiter is specified
					if delimiter != "" && strings.HasSuffix(entry.Name, delimiter) {
						// Objects ending with delimiter should be grouped as common prefixes
						commonPrefixes = append(commonPrefixes, PrefixEntry{
							Prefix: fmt.Sprintf("%s/%s", dirName, prefixName)[len(bucketPrefix):],
						})
						cursor.maxKeys--
						lastEntryWasCommonPrefix = true
						lastCommonPrefixName = entry.Name
					} else {
						// Regular objects (not ending with delimiter)
						contents = append(contents, newCustomListEntry(entry, "", dirName, entryName, bucketPrefix, fetchOwner, false, false))
						cursor.maxKeys--
						lastEntryWasCommonPrefix = false
					}
				}
				if lastEntryWasCommonPrefix {
					nextMarker = lastCommonPrefixName
				} else {
					nextMarker = entry.Name
				}
			})

			if doErr != nil {
				err = doErr
				return err
			}

			if cursor.isTruncated {
				response = CustomListBucketResult{
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
					response.EncodingType = s3.EncodingTypeUrl
				}
				return nil
			}

			if empty {
				break
			}
			marker = nextMarker
		}

		response = CustomListBucketResult{
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
		return nil
	})
	return
}
