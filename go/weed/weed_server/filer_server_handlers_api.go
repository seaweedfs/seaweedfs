package weed_server

import (
	"net/http"
)

func (fs *FilerServer) filerApiHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		fs.GetOrHeadHandler(w, r, true)
	case "HEAD":
		fs.GetOrHeadHandler(w, r, false)
	case "DELETE":
		fs.DeleteHandler(w, r)
	case "PUT":
		fs.PostHandler(w, r)
	case "POST":
		fs.PostHandler(w, r)
	}
}
