package s3api

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	xhttp "github.com/chrislusf/seaweedfs/weed/s3api/http"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	weed_server "github.com/chrislusf/seaweedfs/weed/server"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	maxObjectListSizeLimit = 10000 // Limit number of objects in a listObjectsResponse.
	maxUploadsList         = 10000 // Limit number of uploads in a listUploadsResponse.
	maxPartsList           = 10000 // Limit number of parts in a listPartsResponse.
	globalMaxPartID        = 100000
)

// NewMultipartUploadHandler - New multipart upload.
func (s3a *S3ApiServer) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := xhttp.GetBucketAndObject(r)

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
	response, errCode := s3a.createMultipartUpload(createMultipartUploadInput)

	glog.V(2).Info("NewMultipartUploadHandler", string(s3err.EncodeXMLResponse(response)), errCode)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	writeSuccessResponseXML(w, r, response)

}

// CompleteMultipartUploadHandler - Completes multipart upload.
func (s3a *S3ApiServer) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := xhttp.GetBucketAndObject(r)

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())

	response, errCode := s3a.completeMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      objectKey(aws.String(object)),
		UploadId: aws.String(uploadID),
	})

	glog.V(2).Info("CompleteMultipartUploadHandler", string(s3err.EncodeXMLResponse(response)), errCode)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	writeSuccessResponseXML(w, r, response)

}

// AbortMultipartUploadHandler - Aborts multipart upload.
func (s3a *S3ApiServer) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := xhttp.GetBucketAndObject(r)

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())

	response, errCode := s3a.abortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      objectKey(aws.String(object)),
		UploadId: aws.String(uploadID),
	})

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	glog.V(2).Info("AbortMultipartUploadHandler", string(s3err.EncodeXMLResponse(response)))

	writeSuccessResponseXML(w, r, response)

}

// ListMultipartUploadsHandler - Lists multipart uploads.
func (s3a *S3ApiServer) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := xhttp.GetBucketAndObject(r)

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

	glog.V(2).Infof("ListMultipartUploadsHandler %s errCode=%d", string(s3err.EncodeXMLResponse(response)), errCode)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	// TODO handle encodingType

	writeSuccessResponseXML(w, r, response)
}

// ListObjectPartsHandler - Lists object parts in a multipart upload.
func (s3a *S3ApiServer) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object := xhttp.GetBucketAndObject(r)

	uploadID, partNumberMarker, maxParts, _ := getObjectResources(r.URL.Query())
	if partNumberMarker < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPartNumberMarker)
		return
	}
	if maxParts < 0 {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxParts)
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

	glog.V(2).Infof("ListObjectPartsHandler %s count=%d", string(s3err.EncodeXMLResponse(response)), len(response.Part))

	writeSuccessResponseXML(w, r, response)

}

// PutObjectPartHandler - Put an object part in a multipart upload.
func (s3a *S3ApiServer) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _ := xhttp.GetBucketAndObject(r)

	uploadID := r.URL.Query().Get("uploadId")
	exists, err := s3a.exists(s3a.genUploadsFolder(bucket), uploadID, true)
	if !exists {
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

	dataReader := r.Body
	if s3a.iam.isEnabled() {
		rAuthType := getRequestAuthType(r)
		var s3ErrCode s3err.ErrorCode
		switch rAuthType {
		case authTypeStreamingSigned:
			dataReader, s3ErrCode = s3a.iam.newSignV4ChunkedReader(r)
		case authTypeSignedV2, authTypePresignedV2:
			_, s3ErrCode = s3a.iam.isReqAuthenticatedV2(r)
		case authTypePresigned, authTypeSigned:
			_, s3ErrCode = s3a.iam.reqSignatureV4Verify(r)
		}
		if s3ErrCode != s3err.ErrNone {
			s3err.WriteErrorResponse(w, r, s3ErrCode)
			return
		}
	}
	defer dataReader.Close()

	glog.V(2).Infof("PutObjectPartHandler %s %s %04d", bucket, uploadID, partID)

	uploadUrl := fmt.Sprintf("http://%s%s/%s/%04d.part?collection=%s",
		s3a.option.Filer.ToHttpAddress(), s3a.genUploadsFolder(bucket), uploadID, partID, bucket)

	if partID == 1 && r.Header.Get("Content-Type") == "" {
		dataReader = mimeDetect(r, dataReader)
	}

	etag, errCode := s3a.putToFiler(r, uploadUrl, dataReader)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	setEtag(w, etag)

	writeSuccessResponseEmpty(w, r)

}

func (s3a *S3ApiServer) genUploadsFolder(bucket string) string {
	return fmt.Sprintf("%s/%s/.uploads", s3a.option.BucketsPath, bucket)
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

type byCompletedPartNumber []*s3.CompletedPart

func (a byCompletedPartNumber) Len() int           { return len(a) }
func (a byCompletedPartNumber) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byCompletedPartNumber) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }
