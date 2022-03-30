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
	"time"

	"github.com/chrislusf/seaweedfs/weed/util"
)

func (s3a *S3ApiServer) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {

	dstBucket, dstObject := xhttp.GetBucketAndObject(r)
	errCode := s3a.checkBucketInCache(r, dstBucket)
	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopyDest)
		return
	}

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject := pathToBucketAndObject(cpSrcPath)

	glog.V(3).Infof("CopyObjectHandler %s %s => %s %s", srcBucket, srcObject, dstBucket, dstObject)

	if (srcBucket == dstBucket && srcObject == dstObject || cpSrcPath == "") && isReplace(r) {
		fullPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject))
		dir, name := fullPath.DirAndName()
		entry, err := s3a.getEntry(dir, name)
		if err != nil || entry.IsDirectory {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
			return
		}
		entry.Extended = weed_server.SaveAmzMetaData(r, entry.Extended, isReplace(r))
		err = s3a.touch(dir, name, entry)
		if err != nil {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
			return
		}
		writeSuccessResponseXML(w, r, CopyObjectResult{
			ETag:         fmt.Sprintf("%x", entry.Attributes.Md5),
			LastModified: time.Now().UTC(),
		})
		return
	}

	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}
	srcPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, srcBucket, srcObject))
	dir, name := srcPath.DirAndName()
	if entry, err := s3a.getEntry(dir, name); err != nil || entry.IsDirectory {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	if srcBucket == dstBucket && srcObject == dstObject {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopyDest)
		return
	}

	dstUrl := fmt.Sprintf("http://%s%s/%s%s?collection=%s",
		s3a.option.Filer.ToHttpAddress(), s3a.option.BucketsPath, dstBucket, urlPathEscape(dstObject), dstBucket)
	srcUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer.ToHttpAddress(), s3a.option.BucketsPath, srcBucket, urlPathEscape(srcObject))

	_, _, resp, err := util.DownloadFile(srcUrl, s3a.maybeGetFilerJwtAuthorizationToken(false))
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}
	defer util.CloseResponse(resp)

	glog.V(2).Infof("copy from %s to %s", srcUrl, dstUrl)
	etag, errCode := s3a.putToFiler(r, dstUrl, resp.Body)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	setEtag(w, etag)

	response := CopyObjectResult{
		ETag:         etag,
		LastModified: time.Now().UTC(),
	}

	writeSuccessResponseXML(w, r, response)

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
	dstBucket, _ := xhttp.GetBucketAndObject(r)

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject := pathToBucketAndObject(cpSrcPath)
	// If source object is empty or bucket is empty, reply back invalid copy source.
	if srcObject == "" || srcBucket == "" {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}

	uploadID := r.URL.Query().Get("uploadId")
	partIDString := r.URL.Query().Get("partNumber")

	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidPart)
		return
	}

	glog.V(3).Infof("CopyObjectPartHandler %s %s => %s part %d", srcBucket, srcObject, dstBucket, partID)

	// check partID with maximum part ID for multipart objects
	if partID > globalMaxPartID {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidMaxParts)
		return
	}

	rangeHeader := r.Header.Get("x-amz-copy-source-range")

	dstUrl := fmt.Sprintf("http://%s%s/%s/%04d.part?collection=%s",
		s3a.option.Filer.ToHttpAddress(), s3a.genUploadsFolder(dstBucket), uploadID, partID, dstBucket)
	srcUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer.ToHttpAddress(), s3a.option.BucketsPath, srcBucket, urlPathEscape(srcObject))

	dataReader, err := util.ReadUrlAsReaderCloser(srcUrl, s3a.maybeGetFilerJwtAuthorizationToken(false), rangeHeader)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}
	defer dataReader.Close()

	glog.V(2).Infof("copy from %s to %s", srcUrl, dstUrl)
	etag, errCode := s3a.putToFiler(r, dstUrl, dataReader)

	if errCode != s3err.ErrNone {
		s3err.WriteErrorResponse(w, r, errCode)
		return
	}

	setEtag(w, etag)

	response := CopyPartResult{
		ETag:         etag,
		LastModified: time.Now().UTC(),
	}

	writeSuccessResponseXML(w, r, response)

}

func isReplace(r *http.Request) bool {
	return r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE"
}
