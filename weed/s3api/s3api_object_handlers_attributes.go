package s3api

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

const maxPartsListDefault = 1000

// GetObjectAttributesResponse is the XML response for GetObjectAttributes.
type GetObjectAttributesResponse struct {
	XMLName      xml.Name                  `xml:"GetObjectAttributesResponse"`
	ETag         string                    `xml:"ETag,omitempty"`
	Checksum     *ObjectAttributesChecksum `xml:"Checksum,omitempty"`
	ObjectParts  *ObjectAttributesParts    `xml:"ObjectParts,omitempty"`
	StorageClass string                    `xml:"StorageClass,omitempty"`
	ObjectSize   *int64                    `xml:"ObjectSize,omitempty"`
}

// ObjectAttributesChecksum holds checksum info for GetObjectAttributes.
type ObjectAttributesChecksum struct {
	ChecksumCRC32  string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1   string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256 string `xml:"ChecksumSHA256,omitempty"`
}

// ObjectAttributesParts holds parts info for GetObjectAttributes.
type ObjectAttributesParts struct {
	IsTruncated          bool                    `xml:"IsTruncated"`
	MaxParts             int                     `xml:"MaxParts"`
	NextPartNumberMarker int                     `xml:"NextPartNumberMarker"`
	PartNumberMarker     int                     `xml:"PartNumberMarker"`
	TotalPartsCount      int                     `xml:"PartsCount"`
	Parts                []*ObjectAttributesPart `xml:"Part"`
}

// ObjectAttributesPart holds info for a single part.
type ObjectAttributesPart struct {
	PartNumber int   `xml:"PartNumber"`
	Size       int64 `xml:"Size"`
}

// parseObjectAttributes parses the X-Amz-Object-Attributes header.
func parseObjectAttributes(h http.Header) map[string]struct{} {
	attrs := make(map[string]struct{})
	for _, headerVal := range h.Values(s3_constants.AmzObjectAttributes) {
		for _, v := range strings.Split(strings.TrimSpace(headerVal), ",") {
			v = strings.TrimSpace(v)
			if v != "" {
				attrs[v] = struct{}{}
			}
		}
	}
	return attrs
}

// validateObjectAttributes checks that all requested attributes are valid.
func validateObjectAttributes(attrs map[string]struct{}) bool {
	for attr := range attrs {
		switch attr {
		case "ETag", "Checksum", "StorageClass", "ObjectSize", "ObjectParts":
		default:
			return false
		}
	}
	return true
}

func (s3a *S3ApiServer) GetObjectAttributesHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)
	glog.V(3).Infof("GetObjectAttributesHandler %s %s", bucket, object)

	// Parse and validate requested attributes
	requestedAttrs := parseObjectAttributes(r.Header)
	if len(requestedAttrs) == 0 || !validateObjectAttributes(requestedAttrs) {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidAttributeName)
		return
	}

	// Parse optional pagination headers for ObjectParts
	maxParts := maxPartsListDefault
	if v := r.Header.Get(s3_constants.AmzMaxParts); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed < 0 {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxParts)
			return
		}
		maxParts = parsed
		if maxParts > maxPartsListDefault {
			maxParts = maxPartsListDefault
		}
	}
	partNumberMarker := 0
	if v := r.Header.Get(s3_constants.AmzPartNumberMarker); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed < 0 {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPartNumberMarker)
			return
		}
		partNumberMarker = parsed
	}

	// Check for specific version ID
	versionId := r.URL.Query().Get("versionId")

	var (
		entry *filer_pb.Entry
		err   error
	)

	versioningConfigured, err := s3a.isVersioningConfigured(bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("Error checking versioning for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	if versioningConfigured {
		var targetVersionId string

		if versionId != "" {
			entry, err = s3a.getSpecificObjectVersion(bucket, object, versionId)
			if err != nil {
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
			targetVersionId = versionId
		} else {
			bucketDir := s3a.bucketDir(bucket)
			normalizedObject := s3_constants.NormalizeObjectKey(object)
			versionsDir := normalizedObject + s3_constants.VersionsFolder

			versionsEntry, versionsErr := s3a.getEntry(bucketDir, versionsDir)
			if versionsErr == nil && versionsEntry != nil {
				entry, err = s3a.getLatestObjectVersion(bucket, object)
				if err != nil {
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			} else if errors.Is(versionsErr, filer_pb.ErrNotFound) {
				regularEntry, regularErr := s3a.getEntry(bucketDir, normalizedObject)
				if regularErr == nil && regularEntry != nil {
					entry = regularEntry
					targetVersionId = "null"
				} else {
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			} else {
				entry, err = s3a.getLatestObjectVersion(bucket, object)
				if err != nil {
					s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
					return
				}
			}

			if targetVersionId == "" {
				if entry.Extended != nil {
					if versionIdBytes, exists := entry.Extended[s3_constants.ExtVersionIdKey]; exists {
						targetVersionId = string(versionIdBytes)
					}
				}
				if targetVersionId == "" {
					targetVersionId = "null"
				}
			}
		}

		// Check for delete marker
		if entry.Extended != nil {
			if deleteMarker, exists := entry.Extended[s3_constants.ExtDeleteMarkerKey]; exists && string(deleteMarker) == "true" {
				w.Header().Set(s3_constants.AmzDeleteMarker, "true")
				s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
				return
			}
		}

		w.Header().Set("x-amz-version-id", targetVersionId)
	} else {
		entry, err = s3a.fetchObjectEntry(bucket, object)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
			return
		}
		if entry == nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchKey)
			return
		}
	}

	// Evaluate conditional headers against the resolved entry (after version resolution)
	// This ensures conditions are checked against the correct version, not always the latest
	if s3a.hasConditionalHeaders(r) {
		headers, errCode := parseConditionalHeaders(r)
		if errCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, errCode)
			return
		}
		result := s3a.validateConditionalHeadersForReads(r, headers, entry, bucket, object)
		if result.ErrorCode != s3err.ErrNone {
			glog.V(3).Infof("GetObjectAttributesHandler: Conditional header check failed for %s/%s with error %v", bucket, object, result.ErrorCode)
			if result.ErrorCode == s3err.ErrNotModified && result.ETag != "" {
				w.Header().Set("ETag", result.ETag)
			}
			s3err.WriteErrorResponse(w, r, result.ErrorCode)
			return
		}
	}

	if entry == nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Build response with only requested attributes
	resp := &GetObjectAttributesResponse{}

	if _, ok := requestedAttrs["ETag"]; ok {
		etag := s3a.getObjectETag(entry)
		resp.ETag = strings.Trim(etag, `"`)
	}

	if _, ok := requestedAttrs["StorageClass"]; ok {
		storageClass := "STANDARD"
		if entry.Extended != nil {
			if sc, exists := entry.Extended[s3_constants.AmzStorageClass]; exists && len(sc) > 0 {
				storageClass = string(sc)
			}
		}
		resp.StorageClass = storageClass
	}

	// Checksum: accepted in validation so clients don't get a 400, but SeaweedFS
	// does not yet store S3 checksums (CRC32, CRC32C, SHA1, SHA256), so
	// resp.Checksum is intentionally left nil. When checksum storage is added,
	// populate resp.Checksum here.

	if _, ok := requestedAttrs["ObjectSize"]; ok {
		var size int64
		if entry.Attributes != nil {
			size = int64(entry.Attributes.FileSize)
		}
		resp.ObjectSize = &size
	}

	if _, ok := requestedAttrs["ObjectParts"]; ok {
		resp.ObjectParts = s3a.buildObjectAttributesParts(entry, maxParts, partNumberMarker)
	}

	// Set Last-Modified, remove Content-Type
	if entry.Attributes != nil && entry.Attributes.Mtime > 0 {
		w.Header().Set("Last-Modified", time.Unix(entry.Attributes.Mtime, 0).UTC().Format(http.TimeFormat))
	}
	w.Header().Del("Content-Type")

	writeSuccessResponseXML(w, r, resp)
}

// buildObjectAttributesParts builds the ObjectParts section from multipart metadata.
func (s3a *S3ApiServer) buildObjectAttributesParts(entry *filer_pb.Entry, maxParts, partNumberMarker int) *ObjectAttributesParts {
	if entry.Extended == nil {
		return nil
	}

	boundariesJSON, exists := entry.Extended[s3_constants.SeaweedFSMultipartPartBoundaries]
	if !exists {
		return nil
	}

	var boundaries []PartBoundaryInfo
	if err := json.Unmarshal(boundariesJSON, &boundaries); err != nil {
		glog.Warningf("GetObjectAttributes: failed to unmarshal part boundaries: %v", err)
		return nil
	}

	if len(boundaries) == 0 {
		return nil
	}

	parts := &ObjectAttributesParts{
		PartNumberMarker: partNumberMarker,
		MaxParts:         maxParts,
		TotalPartsCount:  len(boundaries),
	}

	chunks := entry.GetChunks()
	for _, b := range boundaries {
		if b.PartNumber <= partNumberMarker {
			continue
		}
		if len(parts.Parts) >= maxParts {
			parts.IsTruncated = true
			break
		}

		var partSize int64
		if b.StartChunk >= 0 && b.EndChunk >= 0 && b.StartChunk < len(chunks) && b.EndChunk <= len(chunks) && b.StartChunk < b.EndChunk {
			for ci := b.StartChunk; ci < b.EndChunk; ci++ {
				partSize += int64(chunks[ci].Size)
			}
		}

		parts.NextPartNumberMarker = b.PartNumber
		parts.Parts = append(parts.Parts, &ObjectAttributesPart{
			PartNumber: b.PartNumber,
			Size:       partSize,
		})
	}

	return parts
}
