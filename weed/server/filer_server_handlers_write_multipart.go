package weed_server

import (
	"net/http"
)

func (fs *FilerServer) multipartUploadAnalyzer(w http.ResponseWriter, r *http.Request, replication, collection string, dataCenter string) (path string, err error) {
	//Default handle way for http multipart
	if r.Method == "PUT" {
		path = r.URL.Path
	}
	return
}
