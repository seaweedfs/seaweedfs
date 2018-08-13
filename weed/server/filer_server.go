package weed_server

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/cassandra"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/leveldb"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/memdb"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/mysql"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/postgres"
	_ "github.com/chrislusf/seaweedfs/weed/filer2/redis"
	_ "github.com/chrislusf/seaweedfs/weed/msgqueue/kafka"
	_ "github.com/chrislusf/seaweedfs/weed/msgqueue/log"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"net/http"
	"github.com/chrislusf/seaweedfs/weed/msgqueue"
)

type FilerOption struct {
	Masters            []string
	Collection         string
	DefaultReplication string
	RedirectOnRead     bool
	DisableDirListing  bool
	MaxMB              int
	SecretKey          string
	DirListingLimit    int
	DataCenter         string
	EnableNotification bool
}

type FilerServer struct {
	option *FilerOption
	secret security.Secret
	filer  *filer2.Filer
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, option *FilerOption) (fs *FilerServer, err error) {
	fs = &FilerServer{
		option: option,
	}

	if len(option.Masters) == 0 {
		glog.Fatal("master list is required!")
	}

	fs.filer = filer2.NewFiler(option.Masters)

	go fs.filer.KeepConnectedToMaster()

	fs.filer.LoadConfiguration()

	if fs.option.EnableNotification {
		msgqueue.LoadConfiguration()
	}

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
