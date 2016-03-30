package weedserver

import (
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/stats"
)

/*

If volume server is started with a separated public port, the public port will
be more "secure".

Public port currently only supports reads.

Later writes on public port can have one of the 3
security settings:
1. not secured
2. secured by white list
3. secured by JWT(Json Web Token)

*/

func (vs *VolumeServer) privateStoreHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		stats.ReadRequest()
		vs.GetOrHeadHandler(w, r)
	case "HEAD":
		stats.ReadRequest()
		vs.GetOrHeadHandler(w, r)
	case "DELETE":
		stats.DeleteRequest()
		vs.guard.WhiteList(vs.DeleteHandler)(w, r)
	case "PUT":
		stats.WriteRequest()
		vs.guard.WhiteList(vs.PostHandler)(w, r)
	case "POST":
		stats.WriteRequest()
		vs.guard.WhiteList(vs.PostHandler)(w, r)
	}
}

func (vs *VolumeServer) publicReadOnlyHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		stats.ReadRequest()
		vs.GetOrHeadHandler(w, r)
	case "HEAD":
		stats.ReadRequest()
		vs.GetOrHeadHandler(w, r)
	}
}
