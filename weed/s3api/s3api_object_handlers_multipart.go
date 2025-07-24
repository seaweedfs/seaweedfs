package s3api

import (
	"crypto/sha1"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/uuid"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
)

const (
	maxObjectListSizeLimit = 1000  // Limit number of objects in a listObjectsResponse.
	maxUploadsList         = 10000 // Limit number of uploads in a listUploadsResponse.
	maxPartsList           = 10000 // Limit number of parts in a listPartsResponse.
	globalMaxPartID        = 100000
)

// NewMultipartUploadHandler - New multipart upload.
func (s3a *S3ApiServer) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before creating multipart upload
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Check if versioning is enabled for the bucket (needed for object lock)
	versioningEnabled, err := s3a.isVersioningEnabled(bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchBucket)
			return
		}
		glog.Errorf("Error checking versioning status for bucket %s: %v", bucket, err)
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	// Validate object lock headers before processing
	if err := s3a.validateObjectLockHeaders(r, versioningEnabled); err != nil {
		glog.V(2).Infof("NewMultipartUploadHandler: object lock header validation failed for bucket %s, object %s: %v", bucket, object, err)
		s3err.WriteErrorResponse(w, r, mapValidationErrorToS3Error(err))
		return
	}

	createMultipartUploadInput := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      objectKey(aws.String(object)),
		Metadata: make(map[string]*string),
	}

	metadata := weed_server.SaveAmzMetaData(r, nil, false)
	for k, v := range metadata {
		createMultipartUploadInput.Metadata[k] = aws.String(string(v))
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "" {
		createMultipartUploadInput.ContentType = &contentType
	}
	response, errCode := s3a.createMultipartUpload(r, createMultipartUploadInput)

	glog.V(3).Info("NewMultipartUploadHandler", string(s3err.EncodeXMLResponse(response)), errCode)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	writeSuccessResponseXML(w, r, response)

}

// CompleteMultipartUploadHandler - Completes multipart upload.
func (s3a *S3ApiServer) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html

	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before completing multipart upload
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	parts := &CompleteMultipartUpload{}
	if err := xmlDecoder(r.Body, parts, r.ContentLength); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrMalformedXML)
		return
	}

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())
	err := s3a.checkUploadId(object, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	response, errCode := s3a.completeMultipartUpload(r, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      objectKey(aws.String(object)),
		UploadId: aws.String(uploadID),
	}, parts)

	glog.V(3).Info("CompleteMultipartUploadHandler", string(s3err.EncodeXMLResponse(response)), errCode)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// Set version ID in HTTP header if present
	if response.VersionId != nil {
		w.Header().Set("x-amz-version-id", *response.VersionId)
	}

	stats_collect.RecordBucketActiveTime(bucket)
	stats_collect.S3UploadedObjectsCounter.WithLabelValues(bucket).Inc()

	writeSuccessResponseXML(w, r, response)

}

// AbortMultipartUploadHandler - Aborts multipart upload.
func (s3a *S3ApiServer) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before aborting multipart upload
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())
	err := s3a.checkUploadId(object, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	response, errCode := s3a.abortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      objectKey(aws.String(object)),
		UploadId: aws.String(uploadID),
	})

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	glog.V(3).Info("AbortMultipartUploadHandler", string(s3err.EncodeXMLResponse(response)))

	//https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
	s3err.WriteEmptyResponse(w, r, http.StatusNoContent)
	s3err.PostLog(r, http.StatusNoContent, s3err.ErrNone)

}

// ListMultipartUploadsHandler - Lists multipart uploads.
func (s3a *S3ApiServer) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before listing multipart uploads
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, encodingType := getBucketMultipartResources(r.URL.Query())
	if maxUploads < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxUploads)
		return
	}
	if keyMarker != "" {
		// Marker not common with prefix is not implemented.
		if !strings.HasPrefix(keyMarker, prefix) {
			s3err.WriteErrorResponse(w, r, s3err.ErrNotImplemented)
			return
		}
	}

	response, errCode := s3a.listMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket:         aws.String(bucket),
		Delimiter:      aws.String(delimiter),
		EncodingType:   aws.String(encodingType),
		KeyMarker:      aws.String(keyMarker),
		MaxUploads:     aws.Int64(int64(maxUploads)),
		Prefix:         aws.String(prefix),
		UploadIdMarker: aws.String(uploadIDMarker),
	})

	glog.V(3).Infof("ListMultipartUploadsHandler %s errCode=%d", string(s3err.EncodeXMLResponse(response)), errCode)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// TODO handle encodingType

	writeSuccessResponseXML(w, r, response)
}

// ListObjectPartsHandler - Lists object parts in a multipart upload.
func (s3a *S3ApiServer) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before listing object parts
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	uploadID, partNumberMarker, maxParts, _ := getObjectResources(r.URL.Query())
	if partNumberMarker < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPartNumberMarker)
		return
	}
	if maxParts < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxParts)
		return
	}

	err := s3a.checkUploadId(object, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	response, errCode := s3a.listObjectParts(&s3.ListPartsInput{
		Bucket:           aws.String(bucket),
		Key:              objectKey(aws.String(object)),
		MaxParts:         aws.Int64(int64(maxParts)),
		PartNumberMarker: aws.Int64(int64(partNumberMarker)),
		UploadId:         aws.String(uploadID),
	})

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	glog.V(3).Infof("ListObjectPartsHandler %s count=%d", string(s3err.EncodeXMLResponse(response)), len(response.Part))

	writeSuccessResponseXML(w, r, response)

}

// PutObjectPartHandler - Put an object part in a multipart upload.
func (s3a *S3ApiServer) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := s3_constants.GetBucketAndObject(r)

	// Check if bucket exists before putting object part
	if err := s3a.checkBucket(r, bucket); err != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, err)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	err := s3a.checkUploadId(object, uploadID)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrNoSuchUpload)
		return
	}

	partIDString := r.URL.Query().Get("partNumber")
	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		return
	}
	if partID > globalMaxPartID {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxParts)
		return
	}

	dataReader, s3ErrCode := getRequestDataReader(s3a, r)
	if s3ErrCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, s3ErrCode)
		return
	}
	defer dataReader.Close()

	glog.V(2).Infof("PutObjectPartHandler %s %s %04d", bucket, uploadID, partID)

	uploadUrl := s3a.genPartUploadUrl(bucket, uploadID, partID)

	if partID == 1 && r.Header.Get("Content-Type") == "" {
		dataReader = mimeDetect(r, dataReader)
	}
	destination := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, bucket, object)

	etag, errCode := s3a.putToFiler(r, uploadUrl, dataReader, destination, bucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	setEtag(w, etag)

	writeSuccessResponseEmpty(w, r)

}

func (s3a *S3ApiServer) genUploadsFolder(bucket string) string {
	return fmt.Sprintf("%s/%s/%s", s3a.option.BucketsPath, bucket, s3_constants.MultipartUploadsFolder)
}

func (s3a *S3ApiServer) genPartUploadUrl(bucket, uploadID string, partID int) string {
	return fmt.Sprintf("http://%s%s/%s/%04d_%s.part",
		s3a.option.Filer.ToHttpAddress(), s3a.genUploadsFolder(bucket), uploadID, partID, uuid.NewString())
}

// Generate uploadID hash string from object
func (s3a *S3ApiServer) generateUploadID(object string) string {

	object = strings.TrimPrefix(object, "/")
	h := sha1.New()
	h.Write([]byte(object))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Check object name and uploadID when processing  multipart uploading
func (s3a *S3ApiServer) checkUploadId(object string, id string) error {

	hash := s3a.generateUploadID(object)

	if !strings.HasPrefix(id, hash) {
		glog.Errorf("object %s and uploadID %s are not matched", object, id)
		return fmt.Errorf("object %s and uploadID %s are not matched", object, id)
	}
	return nil
}

// Parse bucket url queries for ?uploads
func getBucketMultipartResources(values url.Values) (prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int, encodingType string) {
	prefix = values.Get("prefix")
	keyMarker = values.Get("key-marker")
	uploadIDMarker = values.Get("upload-id-marker")
	delimiter = values.Get("delimiter")
	if values.Get("max-uploads") != "" {
		maxUploads, _ = strconv.Atoi(values.Get("max-uploads"))
	} else {
		maxUploads = maxUploadsList
	}
	encodingType = values.Get("encoding-type")
	return
}

// Parse object url queries
func getObjectResources(values url.Values) (uploadID string, partNumberMarker, maxParts int, encodingType string) {
	uploadID = values.Get("uploadId")
	partNumberMarker, _ = strconv.Atoi(values.Get("part-number-marker"))
	if values.Get("max-parts") != "" {
		maxParts, _ = strconv.Atoi(values.Get("max-parts"))
	} else {
		maxParts = maxPartsList
	}
	encodingType = values.Get("encoding-type")
	return
}

func xmlDecoder(body io.Reader, v interface{}, size int64) error {
	var lbody io.Reader
	if size > 0 {
		lbody = io.LimitReader(body, size)
	} else {
		lbody = body
	}
	d := xml.NewDecoder(lbody)
	d.CharsetReader = func(label string, input io.Reader) (io.Reader, error) {
		return input, nil
	}
	return d.Decode(v)
}

type CompleteMultipartUpload struct {
	Parts []CompletedPart `xml:"Part"`
}
type CompletedPart struct {
	ETag       string
	PartNumber int
}
