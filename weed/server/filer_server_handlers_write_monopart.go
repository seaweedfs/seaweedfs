package weed_server

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func checkContentMD5(w http.ResponseWriter, r *http.Request) (err error) {
	if contentMD5 := r.Header.Get("Content-MD5"); contentMD5 != "" {
		buf, _ := ioutil.ReadAll(r.Body)
		//checkMD5
		sum := md5.Sum(buf)
		fileDataMD5 := base64.StdEncoding.EncodeToString(sum[0:len(sum)])
		if strings.ToLower(fileDataMD5) != strings.ToLower(contentMD5) {
			glog.V(0).Infof("fileDataMD5 [%s] is not equal to Content-MD5 [%s]", fileDataMD5, contentMD5)
			err = fmt.Errorf("MD5 check failed")
			writeJsonError(w, r, http.StatusNotAcceptable, err)
			return
		}
		//reconstruct http request body for following new request to volume server
		r.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
	}
	return
}

func (fs *FilerServer) monolithicUploadAnalyzer(w http.ResponseWriter, r *http.Request, replication, collection string) (fileId, urlLocation string, err error) {
	/*
		Amazon S3 ref link:[http://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html]
		There is a long way to provide a completely compatibility against all Amazon S3 API, I just made
		a simple data stream adapter between S3 PUT API and seaweedfs's volume storage Write API
		1. The request url format should be http://$host:$port/$bucketName/$objectName
		2. bucketName will be mapped to seaweedfs's collection name
		3. You could customize and make your enhancement.
	*/
	lastPos := strings.LastIndex(r.URL.Path, "/")
	if lastPos == -1 || lastPos == 0 || lastPos == len(r.URL.Path)-1 {
		glog.V(0).Infoln("URL Path [%s] is invalid, could not retrieve file name", r.URL.Path)
		err = fmt.Errorf("URL Path is invalid")
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	if err = checkContentMD5(w, r); err != nil {
		return
	}

	fileName := r.URL.Path[lastPos+1:]
	if err = multipartHttpBodyBuilder(w, r, fileName); err != nil {
		return
	}

	secondPos := strings.Index(r.URL.Path[1:], "/") + 1
	collection = r.URL.Path[1:secondPos]
	path := r.URL.Path

	if fileId, urlLocation, err = fs.queryFileInfoByPath(w, r, path); err == nil && fileId == "" {
		fileId, urlLocation, err = fs.assignNewFileInfo(w, r, replication, collection)
	}
	return
}
