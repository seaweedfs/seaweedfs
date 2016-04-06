package weedserver

import (
	"net/http"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/cassandra_store"
	"github.com/chrislusf/seaweedfs/weed/filer/embedded_filer"
	"github.com/chrislusf/seaweedfs/weed/filer/flat_namespace"
	"github.com/chrislusf/seaweedfs/weed/filer/redis_store"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
)

type FilerServer struct {
	port               string
	master             string
	collection         string
	defaultReplication string
	redirectOnRead     bool
	disableDirListing  bool
	secret             security.Secret
	filer              filer.Filer
}

func NewFilerServer(r *http.ServeMux, port int, master string, dir string, collection string,
	replication string, redirectOnRead bool, disableDirListing bool,
	secret string,
	cassandra_server string, cassandra_keyspace string,
	redis_server string, redis_password string, redis_database int,
) (fs *FilerServer, err error) {
	fs = &FilerServer{
		master:             master,
		collection:         collection,
		defaultReplication: replication,
		redirectOnRead:     redirectOnRead,
		disableDirListing:  disableDirListing,
		port:               ":" + strconv.Itoa(port),
	}

	if cassandra_server != "" {
		cassandra_store, err := cassandra_store.NewCassandraStore(cassandra_keyspace, cassandra_server)
		if err != nil {
			glog.Fatalf("Can not connect to cassandra server %s with keyspace %s: %v", cassandra_server, cassandra_keyspace, err)
		}
		fs.filer = flat_namespace.NewFlatNamespaceFiler(master, cassandra_store)
	} else if redis_server != "" {
		redis_store := redis_store.NewRedisStore(redis_server, redis_password, redis_database)
		fs.filer = flat_namespace.NewFlatNamespaceFiler(master, redis_store)
	} else {
		if fs.filer, err = embedded_filer.NewFilerEmbedded(master, dir); err != nil {
			glog.Fatalf("Can not start filer in dir %s : %v", dir, err)
			return
		}

		r.HandleFunc("/admin/mv", fs.moveHandler)
	}

	r.HandleFunc("/", fs.filerHandler)

	return fs, nil
}

func (fs *FilerServer) jwt(fileId string) security.EncodedJwt {
	return security.GenJwt(fs.secret, fileId)
}
