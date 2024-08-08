package s3api

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"modernc.org/strutil"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

const (
	DirectiveCopy    = "COPY"
	DirectiveReplace = "REPLACE"
)

func (s3a *S3ApiServer) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {

	dstBucket, dstObject := s3_constants.GetBucketAndObject(r)

	// Copy source path.
	cpSrcPath, err := url.QueryUnescape(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		// Save unescaped string as is.
		cpSrcPath = r.Header.Get("X-Amz-Copy-Source")
	}

	srcBucket, srcObject := pathToBucketAndObject(cpSrcPath)

	glog.V(3).Infof("CopyObjectHandler %s %s => %s %s", srcBucket, srcObject, dstBucket, dstObject)

	replaceMeta, replaceTagging := replaceDirective(r.Header)

	if (srcBucket == dstBucket && srcObject == dstObject || cpSrcPath == "") && (replaceMeta || replaceTagging) {
		fullPath := util.FullPath(fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject))
		dir, name := fullPath.DirAndName()
		entry, err := s3a.getEntry(dir, name)
		if err != nil || entry.IsDirectory {
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
			return
		}
		entry.Extended, err = processMetadataBytes(r.Header, entry.Extended, replaceMeta, replaceTagging)
		entry.Attributes.Mtime = time.Now().Unix()
		if err != nil {
			glog.Errorf("CopyObjectHandler ValidateTags error %s: %v", r.URL, err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidTag)
			return
		}
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

	dstUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer.ToHttpAddress(), s3a.option.BucketsPath, dstBucket, urlEscapeObject(dstObject))
	srcUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer.ToHttpAddress(), s3a.option.BucketsPath, srcBucket, urlEscapeObject(srcObject))

	_, _, resp, err := util_http.DownloadFile(srcUrl, s3a.maybeGetFilerJwtAuthorizationToken(false))
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}
	defer util_http.CloseResponse(resp)

	tagErr := processMetadata(r.Header, resp.Header, replaceMeta, replaceTagging, s3a.getTags, dir, name)
	if tagErr != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}
	glog.V(2).Infof("copy from %s to %s", srcUrl, dstUrl)
	destination := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject)
	etag, errCode := s3a.putToFiler(r, dstUrl, resp.Body, destination, dstBucket)

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
	dstBucket, dstObject := s3_constants.GetBucketAndObject(r)

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

	dstUrl := s3a.genPartUploadUrl(dstBucket, uploadID, partID)
	srcUrl := fmt.Sprintf("http://%s%s/%s%s",
		s3a.option.Filer.ToHttpAddress(), s3a.option.BucketsPath, srcBucket, urlEscapeObject(srcObject))

	resp, dataReader, err := util_http.ReadUrlAsReaderCloser(srcUrl, s3a.maybeGetFilerJwtAuthorizationToken(false), rangeHeader)
	if err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidCopySource)
		return
	}
	defer util_http.CloseResponse(resp)
	defer dataReader.Close()

	glog.V(2).Infof("copy from %s to %s", srcUrl, dstUrl)
	destination := fmt.Sprintf("%s/%s%s", s3a.option.BucketsPath, dstBucket, dstObject)
	etag, errCode := s3a.putToFiler(r, dstUrl, dataReader, destination, dstBucket)

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

func replaceDirective(reqHeader http.Header) (replaceMeta, replaceTagging bool) {
	return reqHeader.Get(s3_constants.AmzUserMetaDirective) == DirectiveReplace, reqHeader.Get(s3_constants.AmzObjectTaggingDirective) == DirectiveReplace
}

func processMetadata(reqHeader, existing http.Header, replaceMeta, replaceTagging bool, getTags func(parentDirectoryPath string, entryName string) (tags map[string]string, err error), dir, name string) (err error) {
	if sc := reqHeader.Get(s3_constants.AmzStorageClass); len(sc) == 0 {
		if sc := existing[s3_constants.AmzStorageClass]; len(sc) > 0 {
			reqHeader[s3_constants.AmzStorageClass] = sc
		}
	}

	if !replaceMeta {
		for header, _ := range reqHeader {
			if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
				delete(reqHeader, header)
			}
		}
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
				reqHeader[k] = v
			}
		}
	}

	if !replaceTagging {
		for header, _ := range reqHeader {
			if strings.HasPrefix(header, s3_constants.AmzObjectTagging) {
				delete(reqHeader, header)
			}
		}

		found := false
		for k, _ := range existing {
			if strings.HasPrefix(k, s3_constants.AmzObjectTaggingPrefix) {
				found = true
				break
			}
		}

		if found {
			tags, err := getTags(dir, name)
			if err != nil {
				return err
			}

			var tagArr []string
			for k, v := range tags {
				tagArr = append(tagArr, fmt.Sprintf("%s=%s", k, v))
			}
			tagStr := strutil.JoinFields(tagArr, "&")
			reqHeader.Set(s3_constants.AmzObjectTagging, tagStr)
		}
	}
	return
}

func processMetadataBytes(reqHeader http.Header, existing map[string][]byte, replaceMeta, replaceTagging bool) (metadata map[string][]byte, err error) {
	metadata = make(map[string][]byte)

	if sc := existing[s3_constants.AmzStorageClass]; len(sc) > 0 {
		metadata[s3_constants.AmzStorageClass] = sc
	}
	if sc := reqHeader.Get(s3_constants.AmzStorageClass); len(sc) > 0 {
		metadata[s3_constants.AmzStorageClass] = []byte(sc)
	}

	if replaceMeta {
		for header, values := range reqHeader {
			if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
				for _, value := range values {
					metadata[header] = []byte(value)
				}
			}
		}
	} else {
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
				metadata[k] = v
			}
		}
	}
	if replaceTagging {
		if tags := reqHeader.Get(s3_constants.AmzObjectTagging); tags != "" {
			parsedTags, err := parseTagsHeader(tags)
			if err != nil {
				return nil, err
			}
			err = ValidateTags(parsedTags)
			if err != nil {
				return nil, err
			}
			for k, v := range parsedTags {
				metadata[s3_constants.AmzObjectTagging+"-"+k] = []byte(v)
			}
		}
	} else {
		for k, v := range existing {
			if strings.HasPrefix(k, s3_constants.AmzObjectTagging) {
				metadata[k] = v
			}
		}
		delete(metadata, s3_constants.AmzTagCount)
	}

	return
}
