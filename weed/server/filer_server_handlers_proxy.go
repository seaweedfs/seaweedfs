package weed_server

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/mem"
	"io"
	"math/rand"
	"net/http"
)

var (
	client *http.Client
)

func init() {
	client = &http.Client{Transport: &http.Transport{
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024,
	}}
}

func (fs *FilerServer) proxyToVolumeServer(w http.ResponseWriter, r *http.Request, fileId string) {

	urlStrings, err := fs.filer.MasterClient.GetLookupFileIdFunction()(fileId)
	if err != nil {
		glog.Errorf("locate %s: %v", fileId, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if len(urlStrings) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	proxyReq, err := http.NewRequest(r.Method, urlStrings[rand.Intn(len(urlStrings))], r.Body)
	if err != nil {
		glog.Errorf("NewRequest %s: %v", urlStrings[0], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	proxyReq.Header.Set("Host", r.Host)
	proxyReq.Header.Set("X-Forwarded-For", r.RemoteAddr)

	for header, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(header, value)
		}
	}

	proxyResponse, postErr := client.Do(proxyReq)

	if postErr != nil {
		glog.Errorf("post to filer: %v", postErr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer util.CloseResponse(proxyResponse)

	for k, v := range proxyResponse.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(proxyResponse.StatusCode)

	buf := mem.Allocate(128 * 1024)
	defer mem.Free(buf)
	io.CopyBuffer(w, proxyResponse.Body, buf)

}
