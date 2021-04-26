package s3api

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	weed_server "github.com/chrislusf/seaweedfs/weed/server"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/util"
)

func (s3a *S3ApiServer) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {

	dstBucket, dstObject := getBucketAndObject(r)

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject := pathToBucketAndObject(cpSrcPath)

	if (srcBucket == dstBucket && srcObject == dstObject || cpSrcPath == "") && isReplace(r) {
		fullPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject))
		dir, name := fullPath.DirAndName()
		entry, err := s3a.getEntry(dir, name)
		if err != nil {
			writeErrorResponse(w, s3err.ErrInvalidCopySource, r.URL)
		}
		entry.Extended = weed_server.SaveAmzMetaData(r, entry.Extended, isReplace(r))
		err = s3a.touch(dir, name, entry)
		if err != nil {
			writeErrorResponse(w, s3err.ErrInvalidCopySource, r.URL)
		}
		writeSuccessResponseXML(w, encodeResponse(CopyObjectResult{
			ETag:         fmt.Sprintf("%x", entry.Attributes.Md5),
			LastModified: time.Now().UTC(),
		}))
		return
	}

	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		writeErrorResponse(w, s3err.ErrInvalidCopySource, r.URL)
		return
	}

	if srcBucket == dstBucket && srcObject == dstObject {
		writeErrorResponse(w, s3err.ErrInvalidCopyDest, r.URL)
		return
	}

	dstUrl := fmt.Sprintf("http://%s%s/%s%s?collection=%s",
		s3a.option.Filer, s3a.option.BucketsPath, dstBucket, dstObject, dstBucket)
	srcUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, srcBucket, srcObject)

	_, _, resp, err := util.DownloadFile(srcUrl)
	if err != nil {
		writeErrorResponse(w, s3err.ErrInvalidCopySource, r.URL)
		return
	}
	defer util.CloseResponse(resp)

	glog.V(2).Infof("copy from %s to %s", srcUrl, dstUrl)
	etag, errCode := s3a.putToFiler(r, dstUrl, resp.Body)

	if errCode != s3err.ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	setEtag(w, etag)

	response := CopyObjectResult{
		ETag:         etag,
		LastModified: time.Now().UTC(),
	}

	writeSuccessResponseXML(w, encodeResponse(response))

}

func pathToBucketAndObject(path string) (bucket, object string) {
	path = strings.TrimPrefix(path, "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 2 {
		return parts[0], "/" + parts[1]
	}
	return parts[0], "/"
}

type CopyPartResult struct {
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
}

func (s3a *S3ApiServer) CopyObjectPartHandler(w http.ResponseWriter, r *http.Request) {
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/CopyingObjctsUsingRESTMPUapi.html
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html
	dstBucket, _ := getBucketAndObject(r)

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject := pathToBucketAndObject(cpSrcPath)
	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		writeErrorResponse(w, s3err.ErrInvalidCopySource, r.URL)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		writeErrorResponse(w, s3err.ErrInvalidPart, r.URL)
		return
	}

	// check partID with maximum part ID for multipart objects
	if partID > globalMaxPartID {
		writeErrorResponse(w, s3err.ErrInvalidMaxParts, r.URL)
		return
	}

	rangeHeader := r.Header.Get("x-amz-copy-source-range")

	dstUrl := fmt.Sprintf("http://%s%s/%s/%04d.part?collection=%s",
		s3a.option.Filer, s3a.genUploadsFolder(dstBucket), uploadID, partID, dstBucket)
	srcUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer, s3a.option.BucketsPath, srcBucket, srcObject)

	dataReader, err := util.ReadUrlAsReaderCloser(srcUrl, rangeHeader)
	if err != nil {
		writeErrorResponse(w, s3err.ErrInvalidCopySource, r.URL)
		return
	}
	defer dataReader.Close()

	glog.V(2).Infof("copy from %s to %s", srcUrl, dstUrl)
	etag, errCode := s3a.putToFiler(r, dstUrl, dataReader)

	if errCode != s3err.ErrNone {
		writeErrorResponse(w, errCode, r.URL)
		return
	}

	setEtag(w, etag)

	response := CopyPartResult{
		ETag:         etag,
		LastModified: time.Now().UTC(),
	}

	writeSuccessResponseXML(w, encodeResponse(response))

}

func isReplace(r *http.Request) bool {
	return r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE"
}
