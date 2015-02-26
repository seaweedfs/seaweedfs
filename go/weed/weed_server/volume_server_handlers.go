package weed_server

import (
	"net/http"

	"github.com/chrislusf/weed-fs/go/stats"
)

/*

Public port supports reads. Writes on public port can have one of the 3
security settings:
1. not secured
2. secured by white list
3. secured by JWT(Json Web Token)

If volume server is started with a separated admin port, the admin port will
have less "security" for easier implementation.
Admin port always supports reads.  Writes on admin port can have one of
the 2 security settings:
1. not secured
2. secured by white list

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

func (vs *VolumeServer) publicStoreHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		stats.ReadRequest()
		vs.GetOrHeadHandler(w, r)
	case "HEAD":
		stats.ReadRequest()
		vs.GetOrHeadHandler(w, r)
	case "DELETE":
		stats.DeleteRequest()
		vs.guard.Secure(vs.DeleteHandler)(w, r)
	case "PUT":
		stats.WriteRequest()
		vs.guard.Secure(vs.PostHandler)(w, r)
	case "POST":
		stats.WriteRequest()
		vs.guard.Secure(vs.PostHandler)(w, r)
	}
}
