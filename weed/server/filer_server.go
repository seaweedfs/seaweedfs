package weed_server

import (
	"net/http"
	"strconv"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/cassandra"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/leveldb"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/memdb"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/mysql"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/postgres"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/redis"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

type FilerServer struct {
	port               string
	masters            []string
	collection         string
	defaultReplication string
	redirectOnRead     bool
	disableDirListing  bool
	secret             security.Secret
	filer              *filer2.Filer
	maxMB              int
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, ip string, port int, masters []string, collection string,
	replication string, redirectOnRead bool, disableDirListing bool,
	maxMB int,
	secret string,
) (fs *FilerServer, err error) {
	fs = &FilerServer{
		masters:            masters,
		collection:         collection,
		defaultReplication: replication,
		redirectOnRead:     redirectOnRead,
		disableDirListing:  disableDirListing,
		maxMB:              maxMB,
		port:               ip + ":" + strconv.Itoa(port),
	}

	if len(masters) == 0 {
		glog.Fatal("master list is required!")
	}

	fs.filer = filer2.NewFiler(masters)

	go fs.filer.KeepConnectedToMaster()

	fs.filer.LoadConfiguration()

	defaultMux.HandleFunc("/favicon.ico", faviconHandler)
	defaultMux.HandleFunc("/", fs.filerHandler)
	if defaultMux != readonlyMux {
		readonlyMux.HandleFunc("/", fs.readonlyFilerHandler)
	}

	return fs, nil
}

func (fs *FilerServer) jwt(fileId string) security.EncodedJwt {
	return security.GenJwt(fs.secret, fileId)
}
