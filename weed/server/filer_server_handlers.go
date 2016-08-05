package weed_server

import (
	"net/http"
)

func (fs *FilerServer) filerHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		fs.get_guard.WhiteList2(fs.GetOrHeadHandler)(w, r, true)
	case "HEAD":
		fs.head_guard.WhiteList2(fs.GetOrHeadHandler)(w, r, false)
	case "DELETE":
		fs.delete_guard.WhiteList(fs.DeleteHandler)(w, r)
	case "PUT":
		fs.put_guard.WhiteList(fs.PostHandler)(w, r)
	case "POST":
		fs.post_guard.WhiteList(fs.PostHandler)(w, r)
	}
}
