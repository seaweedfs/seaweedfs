package weed_server

import (
	"net/http"
	"strconv"

	"github.com/chrislusf/weed-fs/go/filer"
	"github.com/chrislusf/weed-fs/go/filer/embedded_filer"
	"github.com/chrislusf/weed-fs/go/glog"
)

type FilerServer struct {
	port               string
	master             string
	collection         string
	defaultReplication string
	redirectOnRead     bool
	filer              filer.Filer
}

func NewFilerServer(r *http.ServeMux, port int, master string, dir string, collection string,
	replication string, redirectOnRead bool,
) (fs *FilerServer, err error) {
	fs = &FilerServer{
		master:             master,
		collection:         collection,
		defaultReplication: replication,
		redirectOnRead:     redirectOnRead,
		port:               ":" + strconv.Itoa(port),
	}

	if fs.filer, err = embedded_filer.NewFilerEmbedded(master, dir); err != nil {
		glog.Fatal("Can not start filer in dir", dir, ": ", err.Error())
		return
	}

	r.HandleFunc("/admin/mv", fs.moveHandler)
	r.HandleFunc("/", fs.filerHandler)

	return fs, nil
}
