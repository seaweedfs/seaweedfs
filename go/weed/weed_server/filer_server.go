package weed_server

import (
	"net/http"
	"strconv"

	"github.com/chrislusf/weed-fs/go/filer"
	"github.com/chrislusf/weed-fs/go/filer/cassandra_store"
	"github.com/chrislusf/weed-fs/go/filer/embedded_filer"
	"github.com/chrislusf/weed-fs/go/filer/flat_namespace"
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

func NewEmbeddedFilerServer(r *http.ServeMux, port int, master string, dir string, collection string,
	replication string, redirectOnRead bool,
	cassandra_server string, cassandra_keyspace string,
) (fs *FilerServer, err error) {
	fs = &FilerServer{
		master:             master,
		collection:         collection,
		defaultReplication: replication,
		redirectOnRead:     redirectOnRead,
		port:               ":" + strconv.Itoa(port),
	}

	if cassandra_server == "" {
		if fs.filer, err = embedded_filer.NewFilerEmbedded(master, dir); err != nil {
			glog.Fatalf("Can not start filer in dir %s : %v", err)
			return
		}

		r.HandleFunc("/admin/mv", fs.moveHandler)
	} else {
		cassandra_store, err := cassandra_store.NewCassandraStore(cassandra_keyspace, cassandra_server)
		if err != nil {
			glog.Fatalf("Can not connect to cassandra server %s with keyspace %s: %v", cassandra_server, cassandra_keyspace, err)
		}
		fs.filer = flat_namespace.NewFlatNamesapceFiler(master, cassandra_store)
	}

	r.HandleFunc("/", fs.filerHandler)

	return fs, nil
}
