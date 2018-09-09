package s3api

import (
	"net/http"
	"github.com/gorilla/mux"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"net/url"
	"strconv"
	"io/ioutil"
	"encoding/xml"
	"sort"
	"strings"
)

const (
	maxObjectList   = 1000 // Limit number of objects in a listObjectsResponse.
	maxUploadsList  = 1000 // Limit number of uploads in a listUploadsResponse.
	maxPartsList    = 1000 // Limit number of parts in a listPartsResponse.
	globalMaxPartID = 10000
)

// NewMultipartUploadHandler - New multipart upload.
func (s3a *S3ApiServer) NewMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	var object, bucket string
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = vars["object"]

	response, errCode := s3a.createMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(object),
	})

	if errCode != ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(response))

}

// CompleteMultipartUploadHandler - Completes multipart upload.
func (s3a *S3ApiServer) CompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())

	completeMultipartBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeErrorResponse(w, ErrInternalError, r.URL)
		return
	}
	completedMultipartUpload := &s3.CompletedMultipartUpload{}
	if err = xml.Unmarshal(completeMultipartBytes, completedMultipartUpload); err != nil {
		writeErrorResponse(w, ErrMalformedXML, r.URL)
		return
	}
	if len(completedMultipartUpload.Parts) == 0 {
		writeErrorResponse(w, ErrMalformedXML, r.URL)
		return
	}
	if !sort.IsSorted(byCompletedPartNumber(completedMultipartUpload.Parts)) {
		writeErrorResponse(w, ErrInvalidPartOrder, r.URL)
		return
	}

	response, errCode := s3a.completeMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(object),
		MultipartUpload: completedMultipartUpload,
		UploadId:        aws.String(uploadID),
	})

	if errCode != ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(response))

}

// AbortMultipartUploadHandler - Aborts multipart upload.
func (s3a *S3ApiServer) AbortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	// Get upload id.
	uploadID, _, _, _ := getObjectResources(r.URL.Query())

	response, errCode := s3a.abortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(object),
		UploadId: aws.String(uploadID),
	})

	if errCode != ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(response))

}

// ListMultipartUploadsHandler - Lists multipart uploads.
func (s3a *S3ApiServer) ListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	prefix, keyMarker, uploadIDMarker, delimiter, maxUploads, encodingType := getBucketMultipartResources(r.URL.Query())
	if maxUploads < 0 {
		writeErrorResponse(w, ErrInvalidMaxUploads, r.URL)
		return
	}
	if keyMarker != "" {
		// Marker not common with prefix is not implemented.
		if !strings.HasPrefix(keyMarker, prefix) {
			writeErrorResponse(w, ErrNotImplemented, r.URL)
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

	if errCode != ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	// TODO handle encodingType

	writeSuccessResponseXML(w, encodeResponse(response))
}

// ListObjectPartsHandler - Lists object parts in a multipart upload.
func (s3a *S3ApiServer) ListObjectPartsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	object := vars["object"]

	uploadID, partNumberMarker, maxParts, _ := getObjectResources(r.URL.Query())
	if partNumberMarker < 0 {
		writeErrorResponse(w, ErrInvalidPartNumberMarker, r.URL)
		return
	}
	if maxParts < 0 {
		writeErrorResponse(w, ErrInvalidMaxParts, r.URL)
		return
	}

	response, errCode := s3a.listObjectParts(&s3.ListPartsInput{
		Bucket:           aws.String(bucket),
		Key:              aws.String(object),
		MaxParts:         aws.Int64(int64(maxParts)),
		PartNumberMarker: aws.Int64(int64(partNumberMarker)),
		UploadId:         aws.String(uploadID),
	})

	if errCode != ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	writeSuccessResponseXML(w, encodeResponse(response))

}

// PutObjectPartHandler - Put an object part in a multipart upload.
func (s3a *S3ApiServer) PutObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	rAuthType := getRequestAuthType(r)

	uploadID := r.URL.Query().Get("uploadId")
	exists, err := s3a.exists(s3a.genUploadsFolder(bucket), uploadID, true)
	if !exists {
		writeErrorResponse(w, ErrNoSuchUpload, r.URL)
		return
	}

	partIDString := r.URL.Query().Get("partNumber")
	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(w, ErrInvalidPart, r.URL)
		return
	}
	if partID > globalMaxPartID {
		writeErrorResponse(w, ErrInvalidMaxParts, r.URL)
		return
	}

	dataReader := r.Body
	if rAuthType == authTypeStreamingSigned {
		dataReader = newSignV4ChunkedReader(r)
	}

	uploadUrl := fmt.Sprintf("http://%s%s/%s/%04d.part",
		s3a.option.Filer, s3a.genUploadsFolder(bucket), uploadID, partID-1)

	etag, errCode := s3a.putToFiler(r, uploadUrl, dataReader)

	if errCode != ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	setEtag(w, etag)

	writeSuccessResponseEmpty(w)

}

func (s3a *S3ApiServer) genUploadsFolder(bucket string) string {
	return fmt.Sprintf("%s/%s/_uploads", s3a.option.BucketsPath, bucket)
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
